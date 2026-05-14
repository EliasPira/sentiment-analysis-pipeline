# Importando as bibliotecas.
import gc
import pytz
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import StringType, FloatType
from googletrans import Translator
from nltk.sentiment import SentimentIntensityAnalyzer
from datetime import datetime


# Configurando logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Configurações de variáveis para o BigQuery
bucket_dataproc = 'project-b2d9e7e0-e964-49fe-935-dataproc'
nome_tabela_analizada_bq = 'tb_emails_feedback_analizados'
projeto_bigquery = 'project-b2d9e7e0-e964-49fe-935'
dataset_bigquery = 'insight_data'
tabela_origem = 'tb_emails_feedback'


# Inicializar a Spark Session
logging.info("Inicializando a Spark Session com as configurações definidas...")
spark = SparkSession.builder \
    .appName("Email Feedback Analysis with NLTK") \
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
logging.info("Inicializando a instância do analisador de sentimentos...")
sia = SentimentIntensityAnalyzer()
logging.info("Analisador de sentimentos inicializado com sucesso.")


# Função para traduzir o texto
def translate_text(text):
    translator = Translator()
    try:
        lang = translator.detect(text).lang
        if lang != 'en':
            return translator.translate(text, src=lang, dest='en').text
        else:
            return text
    except Exception:
        return text  # Retorna o texto original em caso de falha na tradução


# Função para análise de sentimentos
def analyze_sentiment(text):
    sentiment = sia.polarity_scores(text)['compound']
    return sentiment


# Criar UDFs para tradução e análise de sentimento
logging.info("Criando UDFs para tradução e análise de sentimentos...")
translate_udf = udf(translate_text, StringType())
sentiment_udf = udf(analyze_sentiment, FloatType())
logging.info("UDFs criadas com sucesso.")


# Leitura da tabela no BigQuery e conversão em um DF Spark
full_table_name_emails = f'{projeto_bigquery}.{dataset_bigquery}.{tabela_origem}'
df_email = spark.read.format('bigquery').option('table', full_table_name_emails).load()
logging.info(f"Tabela {tabela_origem} lida com sucesso. Iniciando a análise de sentimentos...")


# Aplicar UDFs para traduzir e calcular pontuação de sentimento
logging.info("Aplicando tradução e cálculo de sentimento nos textos...")
df_analizado = df_email.withColumn('translated_body', translate_udf(col('corpo')))
df_analizado = df_analizado.withColumn('sentiment_score', sentiment_udf(col('translated_body')))
df_analizado = df_analizado.drop("translated_body")
logging.info("Tradução e cálculo de sentimento concluídos.")


# Classificar os sentimentos como 'Positive', 'Negative', ou 'Neutral'
logging.info("Classificando os sentimentos como Positive, Negative ou Neutral...")
df_analizado = df_analizado.withColumn('sentiment',
                   when(col('sentiment_score') > 0, 'Positive')
                   .when(col('sentiment_score') < 0, 'Negative')
                   .otherwise('Neutral'))
logging.info("Classificação dos sentimentos concluída.")


# Marcar o início da execução
fuso_horario_sp = pytz.timezone('America/Sao_Paulo')
inicio = datetime.now(fuso_horario_sp)
logging.info(f"Início da carga: {inicio}")


# Carregar o resultado da análise no BigQuery
logging.info("Carregando os resultados da análise de sentimentos no BigQuery...")
df_analizado.write.format('bigquery') \
.mode("overwrite") \
.option('table', nome_tabela_analizada_bq) \
.option('parentProject', projeto_bigquery) \
.option('dataset', dataset_bigquery) \
.option('temporaryGcsBucket', bucket_dataproc) \
.save()
logging.info('Análise de sentimentos dos emails carregado com sucesso no BigQuery')


# Marcar o fim da execução
fim = datetime.now(fuso_horario_sp)
logging.info(f"Fim da carga: {fim}")


# Calcular a duração da execução
duracao = fim - inicio
logging.info(f"Duração total: {duracao}")


# Excluindo o DF Spark e liberando a memória do cluster
df_email.unpersist()
df_analizado.unpersist()
del df_email,df_analizado
gc.collect()
spark.stop()
logging.info('Memória do cluster liberada e Spark finalizado.')





