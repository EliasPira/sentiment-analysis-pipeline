# Importando as bibliotecas.
import gc
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, FloatType
from googletrans import Translator
from nltk.sentiment import SentimentIntensityAnalyzer
from datetime import datetime
from google.cloud import bigquery


# Configurando logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Inicializando o cliente Big Query
try:
    bq_client = bigquery.Client()
    logging.info("Cliente Big Query inicializado com sucesso.")
except Exception as e:
    logging.error(f"Falha ao inicializar o cliente Big Query. Erro: {e}")
    raise e


# Configurações de variáveis para o BigQuery
bucket_dataproc = 'project-b2d9e7e0-e964-49fe-935-dataproc'
projeto_bigquery = 'project-b2d9e7e0-e964-49fe-935'
dataset_bigquery = 'insight_data'

tabela_origem_yelp_review = 'tb_yelp_review'
tabela_origem_yelp_business = 'tb_yelp_business'

external_table_yelp_review = 'ext_tb_review'
external_table_yelp_business = 'ext_tb_business'

tabela_analizada_yelp = 'tb_yelp_review_analizados'
view_materializada = 'view_materializada_review_sentimentos'
tabela_final_review = 'tb_final_review_sentimentos'


# Inicializar a Spark Session
logging.info("Inicializando a Spark Session...")
spark = SparkSession.builder \
    .appName("Yelp Reviews Analysis with NLTK") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "3") \
    .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0') \
    .getOrCreate()
spark.conf.set("temporaryGcsBucket", bucket_dataproc)
spark.conf.set("viewsEnabled", "true")
spark.conf.set("materializationDataset", dataset_bigquery)
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)


# Instâncias para tradução e análise de sentimentos
logging.info("Inicializando os analisadores de sentimentos e tradução...")
sia = SentimentIntensityAnalyzer()
logging.info("Analisadores inicializados com sucesso.")


# Função para análise de sentimentos
def analyze_sentiment(text):
    sentiment = sia.polarity_scores(text)['compound']
    return sentiment


# Criar UDFs para tradução e análise de sentimento
logging.info("Criando UDFs para tradução e análise de sentimentos...")
sentiment_udf = udf(analyze_sentiment, FloatType())
logging.info("UDFs criadas com sucesso.")


# Query responsável por criar uma tabela nativa a partir da tabela externa no BigQuery
logging.info(f"Criando a tabela nativa '{tabela_origem_yelp_review}' no BigQuery a partir da tabela externa '{external_table_yelp_review}'")
create_table_yelp_review = f"""
CREATE OR REPLACE TABLE `{projeto_bigquery}.{dataset_bigquery}.{tabela_origem_yelp_review}` AS 
SELECT * FROM `{projeto_bigquery}.{dataset_bigquery}.{external_table_yelp_review}` 
"""

# Execução da query para criar a tabela nativa
job1 = bq_client.query(create_table_yelp_review)

# Esperar a execução da query
job1.result()
logging.info(f"Tabela '{tabela_origem_yelp_review}' criada com sucesso'")


# Query responsável por criar uma tabela nativa a partir da tabela externa no BigQuery
logging.info(f"Criando a tabela nativa '{tabela_origem_yelp_business}' no BigQuery a partir da tabela externa '{external_table_yelp_business}'")
create_table_yelp_business = f"""
CREATE OR REPLACE TABLE `{projeto_bigquery}.{dataset_bigquery}.{tabela_origem_yelp_business}` AS 
SELECT business_id, categories, name FROM `{projeto_bigquery}.{dataset_bigquery}.{external_table_yelp_business}`
"""

# Execução da query para criar a tabela nativa
job2 = bq_client.query(create_table_yelp_business)

# Esperar a execução da query
job2.result()
logging.info(f"Tabela '{tabela_origem_yelp_review}' criada com sucesso'")


# Leitura da tabela Yelp Reviews no BigQuery
logging.info(f"Lendo a tabela '{tabela_origem_yelp_review}' do BigQuery...")
df_yelp = spark.read.format('bigquery')\
.option('table', f'{projeto_bigquery}.{dataset_bigquery}.{tabela_origem_yelp_review}')\
.load()
logging.info("Tabela Yelp Reviews carregada com sucesso.")


# Aplicar análise de sentimentos
logging.info("Aplicando análise de sentimentos...")
df_yelp_analizado = df_yelp.withColumn('sentiment_score', sentiment_udf(col('text')))
df_yelp_analizado = df_yelp_analizado.drop("business_id","cool","date","funny","stars","text","useful","user_id")
logging.info("Análise de sentimentos aplicada com sucesso.")


# Marcar o início da execução
inicio = datetime.now()
logging.info(f"Início da carga: {inicio}")

# Salvar os resultados de volta no BigQuery
logging.info(f"Salvando os resultados da análise de sentimentos na tabela '{tabela_analizada_yelp}'...")
df_yelp_analizado.write.format('bigquery') \
.mode("overwrite") \
.option('table', tabela_analizada_yelp) \
.option('parentProject', projeto_bigquery) \
.option('dataset', dataset_bigquery) \
.option('temporaryGcsBucket', bucket_dataproc) \
.save()
logging.info(f"Resultados salvos com sucesso na tabela '{tabela_analizada_yelp}'.")

# Marcar o fim da execução
fim = datetime.now()
logging.info(f"Fim da carga: {fim}")

# Calcular a duração da execução
duracao = fim - inicio
logging.info(f"Duração total: {duracao}")


# Excluindo o DF Spark e liberando a memória do cluster
df_yelp.unpersist()
df_yelp_analizado.unpersist()
del df_yelp,df_yelp_analizado
gc.collect()
spark.stop()
logging.info('Memória do cluster liberada e Spark finalizado.')


# Criar a view materializada no BigQuery
logging.info("Criando a view materializada no BigQuery...")
sql_query = f"""
CREATE OR REPLACE MATERIALIZED VIEW `{projeto_bigquery}.{dataset_bigquery}.{view_materializada}` AS 
SELECT 
    c.name as NOME,
    c.categories AS CATEGORIAS,
    b.text AS REVIEW,
    b.date AS DATA_AVALIACAO,
    b.stars AS ESTRELAS,
    a.sentiment_score AS SENTIMENT_SCORE,
    CASE 
      WHEN a.sentiment_score = 0 THEN 'NEUTRO'
      WHEN a.sentiment_score > 0 THEN 'POSITIVO'
      WHEN a.sentiment_score < 0 THEN 'NEGATIVO'
    END AS SENTIMENTO
FROM `{projeto_bigquery}.{dataset_bigquery}.{tabela_analizada_yelp}` a
INNER JOIN `{projeto_bigquery}.{dataset_bigquery}.{tabela_origem_yelp_review}` b ON b.review_id = a.review_id
INNER JOIN `{projeto_bigquery}.{dataset_bigquery}.{tabela_origem_yelp_business}` c ON c.business_id = b.business_id
"""

# Executar a query para criar a view materializada
job = bq_client.query(sql_query)

# Esperar a execução da query
job.result()
logging.info(f"View materializada '{view_materializada}' criada com sucesso.")
