
# 📘 Sentiment Analysis Pipeline — GCP (Composer + Dataproc + PySpark + BigQuery)

Este repositório contém um pipeline completo de **extração, processamento, carga e análise de sentimentos** utilizando serviços do **Google Cloud Platform (GCP)**.  
A solução foi projetada para ser **escalável, modular e totalmente automatizada**, usando Cloud Composer (Airflow) para orquestração e Dataproc para processamento distribuído com PySpark.

---

## 🏗 Arquitetura da Solução

```
                ┌──────────────────────────┐
                │      Cloud Composer       │
                │        (Airflow)          │
                └─────────────┬────────────┘
                              │
                              ▼
                ┌──────────────────────────┐
                │   Dataproc Cluster 1      │
                │  (Extract + Load Jobs)    │
                └─────────────┬────────────┘
                              │
                              ▼
                ┌──────────────────────────┐
                │        BigQuery           │
                │ (Tabelas de Emails/Yelp)  │
                └─────────────┬────────────┘
                              │
                              ▼
                ┌──────────────────────────┐
                │   Dataproc Cluster 2      │
                │ (Sentiment Analysis Jobs) │
                └─────────────┬────────────┘
                              │
                              ▼
                ┌──────────────────────────┐
                │        BigQuery           │
                │ (Resultados Finais)       │
                └──────────────────────────┘
```

---

## 📂 Estrutura do Repositório

```
sentiment-analysis-pipeline/
│
├── dags/
│   └── composer_cluster_sentiment_analysis.py
│
├── pyspark_jobs/
│   ├── extract_email.py
│   ├── extract_yelp.py
│   ├── load_to_bq_emails.py
│   ├── load_to_bq_yelp.py
│   ├── sentiment_analysis_emails.py
│   └── sentiment_analysis_yelp.py
│
├── init_actions/
│   ├── bootstrap_pandas_gbq.sh
│   └── bootstrap_nltk.sh
│
└── README.md
```

---

## ⚙️ Componentes do Pipeline

### **1. Cloud Composer (Airflow)**
Orquestra todo o fluxo:

- Criação de clusters Dataproc efêmeros  
- Execução dos jobs PySpark  
- Carregamento no BigQuery  
- Análise de sentimentos  
- Deleção dos clusters  

DAG principal:

```
composer_cluster_sentiment_analysis.py
```

---

### **2. Dataproc Cluster 1 — Extract & Load**

Executa os jobs:

| Job | Descrição |
|-----|-----------|
| `extract_email.py` | Extrai dados de emails |
| `extract_yelp.py` | Extrai dados do Yelp |
| `load_to_bq_emails.py` | Carrega emails no BigQuery |
| `load_to_bq_yelp.py` | Carrega Yelp no BigQuery |

---

### **3. Dataproc Cluster 2 — Sentiment Analysis**

Executa:

| Job | Descrição |
|-----|-----------|
| `sentiment_analysis_emails.py` | Análise de sentimentos em emails |
| `sentiment_analysis_yelp.py` | Análise de sentimentos em reviews do Yelp |

---

### **4. BigQuery**

Armazena:

- Dados brutos  
- Dados transformados  
- Resultados da análise de sentimentos  

---

### **5. Cloud Storage (GCS)**

Armazena:

- Scripts PySpark  
- Init Actions  
- Artefatos temporários  

---

## 🚀 Como Executar o Pipeline

### **1. Subir arquivos para o GCS**

DAG:

```bash
gsutil cp dags/*.py gs://<composer-bucket>/dags/
```

Scripts PySpark:

```bash
gsutil cp pyspark_jobs/*.py gs://<dataproc-bucket>/notebooks/jupyter/
```

Init Actions:

```bash
gsutil cp init_actions/*.sh gs://<dataproc-bucket>/
```

---

### **2. Ativar a DAG no Composer**

No Airflow UI:

- Vá em **DAGs**
- Ative `composer_cluster_sentiment_analysis`
- Clique em **Trigger DAG**

---

### **3. Monitorar Execução**

- Graph View  
- Tree View  
- Logs das tasks  

---

## 🔧 Requisitos

- GCP habilitado com:
  - Cloud Composer
  - Dataproc
  - BigQuery
  - Cloud Storage
- Service Accounts com permissões:
  - `roles/composer.worker`
  - `roles/dataproc.editor`
  - `roles/compute.instanceAdmin.v1`
  - `roles/iam.serviceAccountUser`
  - `roles/storage.objectAdmin`
  - `roles/storage.objectViewer`

---

## 🧪 Testes Locais

Testar scripts PySpark:

```bash
python3 script.py
```

Ou:

```bash
pyspark script.py
```

---

## 📈 Resultados Esperados

- Tabelas populadas no BigQuery  
- Dados enriquecidos com análise de sentimentos  
- Pipeline executando diariamente via Composer  
- Clusters Dataproc criados e destruídos automaticamente  

---

## 🤝 Contribuições

Pull requests são bem-vindos.  
Para mudanças maiores, abra uma issue para discussão.

---

## 📄 Licença

Distribuído sob a licença MIT.
```
