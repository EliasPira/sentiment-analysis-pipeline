# Importando as bibliotecas.
import zipfile
import gcsfs
import pytz
import logging
from google.cloud import storage
from io import BytesIO
from datetime import datetime

# Configurando logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Inicializando as Variáveis
bucket_name_emails = 'project-b2d9e7e0-e964-49fe-935-emails-datalake'
arquivo_zip_no_bucket = 'emails_feedback.zip'


# Criando uma pasta com hora e data atual no formato [yyyy-mm-dd-hh-mm]
fuso_horario_sp = pytz.timezone('America/Sao_paulo')
data_atual = datetime.now(fuso_horario_sp).strftime('%Y-%m-%d-%H-%M')
pasta_data_atual = f'{data_atual}/'
logging.info(f'Pasta de destino para os arquivos extraídos: {pasta_data_atual}')


# Inicializando o cliente de armazenamento do Google Cloud
storage_client = storage.Client()
logging.info(f'Cliente de armazenamento do Goocle Cloud iniciado com sucesso!')


# Inicializando o sistema de arquivos do Google Cloud Storage (gcsfs)
fs = gcsfs.GCSFileSystem(project=storage_client.project)
logging.info(f'Sistema de arquivo do GCS iniciado com sucesso!')


# Acessando o Bucket no Cloud Storage
bucket_emails = storage_client.bucket(bucket_name_emails)
logging.info(f'Acesso ao bucket {bucket_name_emails} realizado com sucesso!')


# Adicionando o arquivo .zip na memória
zip_content = BytesIO()
blob_zip = bucket_emails.blob(arquivo_zip_no_bucket)
blob_zip.download_to_file(zip_content)


# Reinicializando o ponteiro para a posição inicial
zip_content.seek(0)
logging.info(f'Arquivo .zip {arquivo_zip_no_bucket} salvo na memória!')


# Realizando a extração dos arquivos ZIP para a pasta dentro do bucket
with zipfile.ZipFile(zip_content) as zip_ref:
    logging.info(f'Extraindo arquivos do .zip {arquivo_zip_no_bucket}')
    
    for item in zip_ref.infolist():
        if not item.is_dir():
            file_content = zip_ref.read(item)
            
            caminho_no_bucket = f'{pasta_data_atual}{item.filename}'
            
            blob = bucket_emails.blob(caminho_no_bucket)
            blob.upload_from_string(file_content)
            
            logging.info(f'Arquivo {item.filename} enviado para {caminho_no_bucket} no bucket.')





