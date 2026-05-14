#!/bin/bash

##########################################################
### CLUSTER PARA A DESCOMPACTAÇÃO E CARGA NO BIG QUERY ###
##########################################################
gcloud dataproc clusters create cluster-extract-load-anhanguera \
--enable-component-gateway \
--bucket project-b2d9e7e0-e964-49fe-935-dataproc \
--region us-central1 \
--subnet default \
--zone us-central1-a \
--single-node \
--master-machine-type n2-standard-8 \
--master-boot-disk-type pd-balanced \
--master-boot-disk-size 200 \
--image-version 2.2-debian12 \
--optional-components JUPYTER \
--max-age 14400s \
--scopes 'https://www.googleapis.com/auth/cloud-platform' \
--initialization-actions 'gs://project-b2d9e7e0-e964-49fe-935-dataproc/bootstrap_pandas_gbq.sh' \
--project project-b2d9e7e0-e964-49fe-935

# CHECANDO O COMANDO ANTERIOR
if [ $? -eq 0 ]
then 
    echo "Cluster [cluster-extract-load-anhanguera] criado com sucesso!"
else
    echo "Falha na criação do cluster [cluster-extract-load-anhanguera]."
    error_ocurred=1
fi

#----------------------------------------------------------------------------#
echo "Executando o JOB: extract_email"
gcloud dataproc jobs submit pyspark gs://project-b2d9e7e0-e964-49fe-935-dataproc/notebooks/jupyter/extract_email.py \
--cluster=cluster-extract-load-anhanguera \
--region=us-central1 \
--id=extract_email_$(date +%s)

# CHECANDO O COMANDO ANTERIOR
if [ $? -eq 0 ]
then 
    echo "JOB extract_email executado com sucesso!"
else
    echo "Falha na execução do JOB extract_email."
    error_ocurred=1
fi
#----------------------------------------------------------------------------#

#----------------------------------------------------------------------------#
echo "Executando o JOB: extract_yelp"
gcloud dataproc jobs submit pyspark gs://project-b2d9e7e0-e964-49fe-935-dataproc/notebooks/jupyter/extract_yelp.py \
--cluster=cluster-extract-load-anhanguera \
--region=us-central1 \
--id=extract_yelp_$(date +%s)

# CHECANDO O COMANDO ANTERIOR
if [ $? -eq 0 ]
then 
    echo "JOB extract_yelp executado com sucesso!"
else
    echo "Falha na execução do JOB extract_yelp."
    error_ocurred=1
fi
#----------------------------------------------------------------------------#

#----------------------------------------------------------------------------#
echo "Executando o JOB: load_to_bq_emails"
gcloud dataproc jobs submit pyspark gs://project-b2d9e7e0-e964-49fe-935-dataproc/notebooks/jupyter/load_to_bq_emails.py \
--cluster=cluster-extract-load-anhanguera \
--region=us-central1 \
--id=load_to_bq_emails_$(date +%s)

# CHECANDO O COMANDO ANTERIOR
if [ $? -eq 0 ]
then 
    echo "JOB load_to_bq_emails executado com sucesso!"
else
    echo "Falha na execução do JOB load_to_bq_emails."
    error_ocurred=1
fi
#----------------------------------------------------------------------------#

#----------------------------------------------------------------------------#
echo "Executando o JOB: load_to_bq_yelp"
gcloud dataproc jobs submit pyspark gs://project-b2d9e7e0-e964-49fe-935-dataproc/notebooks/jupyter/load_to_bq_yelp.py \
--cluster=cluster-extract-load-anhanguera \
--region=us-central1 \
--id=load_to_bq_yelp_$(date +%s)

# CHECANDO O COMANDO ANTERIOR
if [ $? -eq 0 ]
then 
    echo "JOB load_to_bq_yelp executado com sucesso!"
else
    echo "Falha na execução do JOB load_to_bq_yelp."
    error_ocurred=1
fi
#----------------------------------------------------------------------------#

#----------------------------------------------------------------------------#
echo "Excluindo o cluster [cluster-extract-load-anhanguera]"
gcloud dataproc clusters delete cluster-extract-load-anhanguera --region us-central1 -q

if [ $? -eq 0 ]
then 
    echo "Cluster [cluster-extract-load-anhanguera] deletado com sucesso!"
else
    echo "Falha na exclusão do cluster [cluster-extract-load-anhanguera]."
    error_ocurred=1
fi
#----------------------------------------------------------------------------#




#############################################
### CLUSTER PARA A ANÁLISE DE SENTIMENTOS ###
#############################################
gcloud dataproc clusters create cluster-sentiment-analysis-anhanguera \
--enable-component-gateway \
--bucket project-b2d9e7e0-e964-49fe-935-dataproc \
--region us-central1 \
--subnet default \
--zone us-central1-a \
--master-machine-type n2-standard-2 \
--master-boot-disk-type pd-balanced \
--master-boot-disk-size 50 \
--num-workers 3 \
--worker-machine-type n2-standard-2 \
--worker-boot-disk-type pd-balanced \
--worker-boot-disk-size 50 \
--image-version 2.2-debian12 \
--optional-components JUPYTER \
--max-age 14400s \
--scopes 'https://www.googleapis.com/auth/cloud-platform' \
--initialization-actions 'gs://project-b2d9e7e0-e964-49fe-935-dataproc/bootstrap_nltk.sh' \
--project project-b2d9e7e0-e964-49fe-935

# CHECANDO O COMANDO ANTERIOR
if [ $? -eq 0 ]
then 
    echo "Cluster [cluster-sentiment-analysis-anhanguera] criado com sucesso!"
else
    echo "Falha na criação do cluster [cluster-sentiment-analysis-anhanguera]."
    error_ocurred=1
fi

#----------------------------------------------------------------------------#
echo "Executando o JOB: sentiment_analysis_emails"
gcloud dataproc jobs submit pyspark gs://project-b2d9e7e0-e964-49fe-935-dataproc/notebooks/jupyter/sentiment_analysis_emails.py \
--cluster=cluster-sentiment-analysis-anhanguera \
--region=us-central1 \
--id=sentiment_analysis_emails_$(date +%s)

# CHECANDO O COMANDO ANTERIOR
if [ $? -eq 0 ]
then 
    echo "JOB sentiment_analysis_emails executado com sucesso!"
else
     echo "Falha na execução do JOB sentiment_analysis_emails."
    error_ocurred=1
fi
#----------------------------------------------------------------------------#

#----------------------------------------------------------------------------#
echo "Executando o JOB: sentiment_analysis_yelp"
gcloud dataproc jobs submit pyspark gs://project-b2d9e7e0-e964-49fe-935-dataproc/notebooks/jupyter/sentiment_analysis_yelp.py \
--cluster=cluster-sentiment-analysis-anhanguera \
--region=us-central1 \
--id=sentiment_analysis_yelp_$(date +%s)

# CHECANDO O COMANDO ANTERIOR
if [ $? -eq 0 ]
then 
    echo "JOB sentiment_analysis_emails executado com sucesso!"
else
    echo "Falha na execução do JOB sentiment_analysis_emails."
    error_ocurred=1
fi
#----------------------------------------------------------------------------#

#----------------------------------------------------------------------------#
echo "Excluindo o cluster [cluster-sentiment-analysis-anhanguera]"
gcloud dataproc clusters delete cluster-sentiment-analysis-anhanguera --region us-central1 -q

if [ $? -eq 0 ]
then 
    echo "Cluster [cluster-sentiment-analysis-anhanguera] deletado com sucesso!"
else
    echo "Falha na exclusão do cluster [cluster-sentiment-analysis-anhanguera]."
    error_ocurred=1
fi
#----------------------------------------------------------------------------#

#----------------------------------------------------------------------------#
# Verificar se ocorreu algum erro durante a execução
if [ -n "error_ocurred" ]; then 
    exit 1 # Retorna um código de erro se algum comando falhou
else 
    exit 0 # Retorna um código de sucesso de todos os comandos rodaram com sucesso

fi
#----------------------------------------------------------------------------#