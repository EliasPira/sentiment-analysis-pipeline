# Importando as Bibliotecas
import pandas as pd
from pandas_gbq import to_gbq
from google.cloud import storage
from email import message_from_string
import logging

# Configurando logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Inicializando as variáveis
nome_do_bucket_eml = 'project-b2d9e7e0-e964-49fe-935-emails-datalake'
projeto_bigquery = 'project-b2d9e7e0-e964-49fe-935'
dataset_bigquery = 'insight_data'
tabela_bigquery = f'{dataset_bigquery}.tb_emails_feedback'

# Inicializando o cliente de armazenamento do Google Cloud
try:
    storage_client = storage.Client()
    logging.info("Cliente de armazenamento do Google Cloud inicializado com sucesso.")
except Exception as e:
    logging.error(f"Falha ao inicializar o cliente de armazenamento do Google Cloud. Erro: {e}")
    raise e

# Listando todos os blobs no bucket    
try:
    blobs = list(storage_client.list_blobs(nome_do_bucket_eml))
    logging.info(f"Listando os blobs do bucket {nome_do_bucket_eml}...")    
except Exception as e:
    logging.error(f"Falha ao listar os blobs. Erro: {e}")
    raise e

# Filtrar as pastas que seguem o formato de data e hora (yyyy-mm-dd-hh-mm)
try: 
    pastas = list(set([blob.name.split('/')[0] for blob in blobs if '/' in blob.name]))
    pastas_ordenadas = sorted(pastas, reverse=True)
    ultima_pasta = pastas_ordenadas[0]
    pasta_eml = f'{ultima_pasta}/emails_feedback_faker/'
    logging.info(f"Última pasta de data encontrada: {pasta_eml}")

    blobs_eml = list(storage_client.list_blobs(nome_do_bucket_eml, prefix=pasta_eml))
    arquivos_eml = [blob.name for blob in blobs_eml if blob.name.endswith('.eml')]
    logging.info(f"Arquivos .eml encontrados: {arquivos_eml}")
except Exception as e:
    logging.error(f"Falha ao listar as pastas e arquivos. Erro: {e}")
    raise e

# Função para extrair informações dos e-mails
def extrair_informacoes_eml(conteudo_eml):
    mensagem = message_from_string(conteudo_eml)
    remetente = mensagem.get('From')
    destinatario = mensagem.get('To')
    assunto = mensagem.get('Subject')
    data = mensagem.get('Date')
    corpo = mensagem.get_payload()
    return {
        'remetente': remetente,
        'destinatario': destinatario,
        'assunto': assunto,
        'data': data,
        'corpo': corpo
    }

# Iterando sobre cada arquivo de email e carregando seus dados no Big Query
dados_emails = []

for arquivo in arquivos_eml:
    try:
        logging.info(f"Lendo arquivo .eml: {arquivo}")
        blob = storage_client.bucket(nome_do_bucket_eml).blob(arquivo)
        conteudo_eml = blob.download_as_text()
        
        dados_extraidos = extrair_informacoes_eml(conteudo_eml)
        dados_emails.append(dados_extraidos)
    except Exception as e:
        logging.error(f"Erro ao processar o arquivo {arquivo}. Erro: {e}")
        continue


# Convertendo a lista para um DataFrame Pandas
try:
    logging.info("Convertendo os dados dos e-mails para DataFrame Pandas.")
    df_emails = pd.DataFrame(dados_emails)
except Exception as e:
    logging.error(f"Erro ao converter os dados dos e-mails para DataFrame. Erro: {e}")
    raise e


# Carregando os dados no BigQuery usando pandas-gbq
try:
    logging.info(f"Carregando os dados no BigQuery: {tabela_bigquery}...")

    # Usando pandas-gbq para carregar os dados
    to_gbq(df_emails, tabela_bigquery, project_id=projeto_bigquery, if_exists='replace')    
    logging.info("Dados carregados com sucesso no BigQuery.")
    
except Exception as e:
    logging.error(f"Erro ao carregar os dados no BigQuery. Erro: {e}")
    raise e

