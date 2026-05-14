# Importando as Bibliotecas
import logging
import json
from google.cloud import storage
from google.cloud import bigquery


# Configurando logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Inializando as variáveis
yelp_bucket = 'project-b2d9e7e0-e964-49fe-935-yelp-datalake'
projeto_bigquery = 'project-b2d9e7e0-e964-49fe-935'
dataset_bigquery = 'insight_data'


# Inicializando o cliente de armazenamento do Google Cloud
try:
    storage_client = storage.Client()
    logging.info("Cliente de armazenamento do Google Cloud inicializado com sucesso.")
except Exception as e:
    logging.error(f"Falha ao inicializar o cliente de armazenamento do Google Cloud. Erro: {e}")
    raise e


# Inicializando o cliente Big Query
try:
    bq_client = bigquery.Client()
    logging.info("Cliente Big Query inicializado com sucesso.")
except Exception as e:
    logging.error(f"Falha ao inicializar o cliente Big Query. Erro: {e}")
    raise e


# Listando todos os blobs no bucket       
try:
    logging.info("Listando os blobs do bucket Yelp...")
    yelp_blobs = list(storage_client.list_blobs(yelp_bucket)) # Armazenando os blobs em uma lista para reutilização    
except Exception as e:
    logging.error(f"Falha ao listar os blobs. Erro: {e}")
    raise e


# Listando e ordenando todas as pastas no bucket que seguem o formato [yyyy-mm-dd-hh-mm]
try:
    pastas = list(set([yelp_blob.name.split('/')[0] for yelp_blob in yelp_blobs if '/' in yelp_blob.name]))    
    pastas_ordenadas = sorted(pastas, reverse=True)
    ultima_pasta = pastas_ordenadas[0]
    logging.info(f"Última pasta encontrada: {ultima_pasta}")
    
    # Listando todos os arquivos JSON na última pasta
    arquivos_json = [yelp_blob.name for yelp_blob in yelp_blobs if yelp_blob.name.startswith(ultima_pasta) and yelp_blob.name.endswith('.json')]
    logging.info(f"Arquivos JSON encontrados na pasta {ultima_pasta}: {arquivos_json}")

except Exception as e:
    logging.error(f"Falha ao listar as pastas. Erro: {e}")
    raise e


# Função para remover a coluna "attributes" de um JSON
def remover_coluna_attributes(json_content):
    linhas_corrigidas = []
    for line in json_content.splitlines():
        try:
            data = json.loads(line)
            if 'attributes' in data:
                del data['attributes']  # Remove a coluna "attributes"
            linhas_corrigidas.append(json.dumps(data))  # Reescreve como JSON
        except Exception as e:
            logging.error(f"Erro ao processar linha JSON. Linha ignorada. Erro: {e}")
    return "\n".join(linhas_corrigidas)


# Processar o arquivo yelp_business antes de criar a tabela externa
try:
    for arquivo_json in arquivos_json:
        if "business" in arquivo_json:  # Verifica se é o arquivo yelp_business
            logging.info(f"Processando arquivo yelp_business: {arquivo_json}")

            # Baixa o conteúdo do arquivo JSON
            blob = storage_client.bucket(yelp_bucket).blob(arquivo_json)
            file_content = blob.download_as_text()

            # Remove a coluna "attributes"
            conteudo_corrigido = remover_coluna_attributes(file_content)

            # Sobrescreve o arquivo no bucket
            blob.upload_from_string(conteudo_corrigido, content_type='application/json')
            logging.info(f"Arquivo de negócios corrigido enviado para o bucket: {arquivo_json}")
except Exception as e:
    logging.error(f"Erro ao processar o arquivo yelp_businessb. Erro: {e}")
    raise e


# Criando external tables a partir dos arquivos .json no bucket
for arquivo_json in arquivos_json:
    arquivo = arquivo_json.split("/")[1].split(".")[0].split("_")[-1]   
    full_ext_table_name = f"`{projeto_bigquery}.{dataset_bigquery}.ext_tb_{arquivo}`"
        
    uri = f"gs://{yelp_bucket}/{arquivo_json}"
        
    create_external_table_query = f"""
    CREATE OR REPLACE EXTERNAL TABLE {full_ext_table_name}
    OPTIONS(
        format = 'NEWLINE_DELIMITED_JSON',
        uris = ['{uri}']
        );"""        
    
    # Executa a query de criar a tabela externa
    try:
        bq_client.query(create_external_table_query).result()  # Adicionado .result() para esperar a conclusão da query
        logging.info(f"Tabela externa criada com sucesso: {full_ext_table_name}")
    except Exception as e:
        logging.error(f"Erro ao criar tabela externa para {full_ext_table_name}. Erro: {e}")

logging.info("Processo concluído!")
