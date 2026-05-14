#######################################
### Importação das libs necessárias ###
#######################################
import time
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    ClusterGenerator,
)
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

############################
### Configurações Gerais ###
############################
DAG_ID = "composer_cluster_sentiment_analysis"
PROJECT_ID = "project-b2d9e7e0-e964-49fe-935"
BUCKET_NAME = "project-b2d9e7e0-e964-49fe-935-dataproc"
REGION = "us-central1"
ZONE = "us-central1-a"


##########################
### Nomes dos clusters ###
##########################
CLUSTERS = [
    "cluster-extract-load-anhanguera",
    "cluster-sentiment-analysis-anhanguera"
]


#####################################
### Caminho dos scripts no bucket ###
#####################################
SCRIPT_BUCKET_PATH = f"gs://{BUCKET_NAME}/notebooks/jupyter"

SCRIPTS = [
    "extract_email.py",
    "extract_yelp.py",
    "load_to_bq_emails.py",
    "load_to_bq_yelp.py",
    "sentiment_analysis_emails.py",
    "sentiment_analysis_yelp.py"
]


##############################
### Caminho dos bootstraps ###
##############################
BOOTSTRAP_PANDAS_GBQ = f"gs://{BUCKET_NAME}/bootstrap_pandas_gbq.sh"
BOOTSTRAP_NLTK = f"gs://{BUCKET_NAME}/bootstrap_nltk.sh"


##################################
### Configurações do cluster 1 ###
##################################
CLUSTER_GENERATOR_CONFIG_1 = ClusterGenerator(
    project_id=PROJECT_ID,
    zone=ZONE,
    master_machine_type="n2-standard-8",
    master_disk_size=200,
    num_workers=0,
    storage_bucket=BUCKET_NAME,
    init_actions_uris=[BOOTSTRAP_PANDAS_GBQ],
).make()


##################################
### Configurações do cluster 2 ###
##################################
CLUSTER_GENERATOR_CONFIG_2 = ClusterGenerator(
    project_id=PROJECT_ID,
    zone=ZONE,
    master_machine_type="n2-standard-2",
    master_disk_size=50,
    worker_machine_type="n2-standard-2",
    worker_disk_size=50,
    num_workers=3,
    storage_bucket=BUCKET_NAME,
    init_actions_uris=[BOOTSTRAP_NLTK],
).make()


######################################
### Configurações dos jobs PySpark ###
######################################
PYSPARK_JOB_1 = {
    "reference": {
        "project_id": PROJECT_ID,
        "job_id": f"{SCRIPTS[0].replace('.py','')}_{int(time.time())}"},
    "placement": {"cluster_name": CLUSTERS[0]},
    "pyspark_job": {"main_python_file_uri": f"{SCRIPT_BUCKET_PATH}/{SCRIPTS[0]}"},
}

PYSPARK_JOB_2 = {
    "reference": {
        "project_id": PROJECT_ID,
        "job_id": f"{SCRIPTS[1].replace('.py','')}_{int(time.time())}"},
    "placement": {"cluster_name": CLUSTERS[0]},
    "pyspark_job": {"main_python_file_uri": f"{SCRIPT_BUCKET_PATH}/{SCRIPTS[1]}"},
}

PYSPARK_JOB_3 = {
    "reference": {
        "project_id": PROJECT_ID,
        "job_id": f"{SCRIPTS[2].replace('.py','')}_{int(time.time())}"},
    "placement": {"cluster_name": CLUSTERS[0]},
    "pyspark_job": {"main_python_file_uri": f"{SCRIPT_BUCKET_PATH}/{SCRIPTS[2]}"},
}

PYSPARK_JOB_4 = {
    "reference": {
        "project_id": PROJECT_ID,
        "job_id": f"{SCRIPTS[3].replace('.py','')}_{int(time.time())}"},

    "placement": {"cluster_name": CLUSTERS[0]},
    "pyspark_job": {"main_python_file_uri": f"{SCRIPT_BUCKET_PATH}/{SCRIPTS[3]}"},
}

PYSPARK_JOB_5 = {
    "reference": {
        "project_id": PROJECT_ID,
        "job_id": f"{SCRIPTS[4].replace('.py','')}_{int(time.time())}"},
    "placement": {"cluster_name": CLUSTERS[1]},
    "pyspark_job": {"main_python_file_uri": f"{SCRIPT_BUCKET_PATH}/{SCRIPTS[4]}"},
}

PYSPARK_JOB_6 = {
    "reference": {
        "project_id": PROJECT_ID,
        "job_id": f"{SCRIPTS[5].replace('.py','')}_{int(time.time())}"},
    "placement": {"cluster_name": CLUSTERS[1]},
    "pyspark_job": {"main_python_file_uri": f"{SCRIPT_BUCKET_PATH}/{SCRIPTS[5]}"},
}


########################
### Definição do DAG ###
########################
with models.DAG(
    DAG_ID,
    description="Pipeline de análise de sentimentos usando Dataproc e PySpark.",
    schedule='12 20 * * *',  # 20:12 UTC = 23:12 Brasília
    start_date=days_ago(1),
    catchup=False,
    tags=["dataproc", "anhanguera"],
) as dag:


    #################
    ### Cluster 1 ###
    #################
    create_cluster_extract_load = DataprocCreateClusterOperator(
        task_id="create_cluster_extract_load",
        cluster_name=CLUSTERS[0],
        region=REGION,
        cluster_config=CLUSTER_GENERATOR_CONFIG_1,
        project_id=PROJECT_ID,
    )


    extract_emails = DataprocSubmitJobOperator(
        task_id="extract_emails",
        job=PYSPARK_JOB_1,
        region=REGION,
        project_id=PROJECT_ID,
    )


    extract_yelp = DataprocSubmitJobOperator(
        task_id="extract_yelp",
        job=PYSPARK_JOB_2,
        region=REGION,
        project_id=PROJECT_ID,
    )


    load_to_bq_emails = DataprocSubmitJobOperator(
        task_id="load_to_bq_emails",
        job=PYSPARK_JOB_3,
        region=REGION,
        project_id=PROJECT_ID,
    )


    load_to_bq_yelp = DataprocSubmitJobOperator(
        task_id="load_to_bq_yelp",
        job=PYSPARK_JOB_4,
        region=REGION,
        project_id=PROJECT_ID,
    )


    delete_cluster_extract_load = DataprocDeleteClusterOperator(
        task_id="delete_cluster_extract_load",
        project_id=PROJECT_ID,
        cluster_name=CLUSTERS[0],
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )


    #################
    ### Cluster 2 ###
    #################
    create_cluster_sentiment_analysis = DataprocCreateClusterOperator(
        task_id="create_cluster_sentiment_analysis",
        cluster_name=CLUSTERS[1],
        region=REGION,
        cluster_config=CLUSTER_GENERATOR_CONFIG_2,
        project_id=PROJECT_ID,
    )


    sentiment_analysis_emails = DataprocSubmitJobOperator(
        task_id="sentiment_analysis_emails",
        job=PYSPARK_JOB_5,
        region=REGION,
        project_id=PROJECT_ID,
    )


    sentiment_analysis_yelp = DataprocSubmitJobOperator(
        task_id="sentiment_analysis_yelp",
        job=PYSPARK_JOB_6,
        region=REGION,
        project_id=PROJECT_ID,
    )


    delete_cluster_sentiment_analysis = DataprocDeleteClusterOperator(
        task_id="delete_cluster_sentiment_analysis",
        project_id=PROJECT_ID,
        cluster_name=CLUSTERS[1],
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )


#####################################################
### Definição de dependências e ordem de execução ###
#####################################################
create_cluster_extract_load >> extract_emails >> extract_yelp >> [load_to_bq_emails, load_to_bq_yelp] >> delete_cluster_extract_load
delete_cluster_extract_load >> create_cluster_sentiment_analysis >> sentiment_analysis_emails >> sentiment_analysis_yelp >> delete_cluster_sentiment_analysis