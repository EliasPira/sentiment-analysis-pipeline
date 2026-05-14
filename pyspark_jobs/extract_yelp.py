# Importando as bibliotecas.
import tarfile
import gcsfs
import pytz
import logging
import json
from datetime import datetime
from google.cloud import storage

# Configurando logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Inicializando as Variáveis
bucket_name_yelp = 'project-b2d9e7e0-e964-49fe-935-yelp-datalake'
arquivo_tar_no_bucket = 'yelp_dataset.tar'

# Criando uma pasta com hora e data atuais no formato [yyyy-mm-dd-hh-mm]
fuso_horario_sp = pytz.timezone('America/Sao_Paulo')
data_atual = datetime.now(fuso_horario_sp).strftime('%Y-%m-%d-%H-%M')
pasta_data_atual = f'{data_atual}'  # Pasta no bucket para os arquivos extraídos
logging.info(f"Pasta de destino para os arquivos extraídos: {pasta_data_atual}")

# Inicializando o cliente de armazenamento do Google Cloud
try: 
    storage_client = storage.Client()
    logging.info(f"Cliente de armazenamento do Google Cloud inicializado com sucesso.")
except Exception as e:
    logging.error(f"Falha ao inicializar o cliente de armazenamento do Google Cloud. Erro: {e}")
    raise e
  
# Inicializando o sistema de arquivos do Google Cloud Storage (gcsfs)
try:
    fs = gcsfs.GCSFileSystem(project=storage_client.project)
    logging.info("Sistema de arquivos do GCS inicializado com sucesso.")
except Exception as e:
    logging.error(f"Falha ao inicializar o sistema de arquivos do GCS. Erro: {e}")
    raise e    

# Acessando o Bucket no Cloud Storage
try:
    bucket_yelp = storage_client.bucket(bucket_name_yelp)
    logging.info(f"Acesso ao bucket '{bucket_name_yelp}' realizado com sucesso.")
except Exception as e:
    logging.error(f"Erro ao acessar o bucket '{bucket_name_yelp}'. Erro: {e}")
    raise e

# Realizando a extração dos arquivos json do arquivo Yelp .tar
try:
    with fs.open(f'gs://{bucket_name_yelp}/{arquivo_tar_no_bucket}', 'rb') as tar_file:
        logging.info(f"Arquivo .TAR {arquivo_tar_no_bucket} aberto com sucesso no bucket {bucket_name_yelp}")
        
        with tarfile.open(fileobj=tar_file, mode='r:*') as tar:
            logging.info(f'Iniciando a extração dos arquivos de {arquivo_tar_no_bucket}...')
            
            for item in tar.getmembers():
                if item.isfile():
                    try:
                        file_content = tar.extractfile(item).read()
                        
                        caminho_no_bucket = f'{pasta_data_atual}/{item.name}'
                        
                        blob = bucket_yelp.blob(caminho_no_bucket)
                        
                        blob.upload_from_string(file_content)
                        logging.info(f'Arquivo {item.name} enviado para a pasta {pasta_data_atual}.')
                    
                    except Exception as e:
                        logging.error(f"Erro ao processar o arquivo {item.name}. Erro: {e}")
                        raise e
except Exception as e:
    logging.error(f"Erro durante o processo de extração/upload do arquivo {arquivo_tar_no_bucket}")
    raise e
