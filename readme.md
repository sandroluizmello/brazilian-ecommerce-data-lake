# 🚀 Data Engineering Pipeline - Olist

Projeto de Engenharia de Dados com foco na construção de um pipeline moderno utilizando arquitetura de Data Lake (Bronze, Silver e Gold), simulando um ambiente real de produção.

---

## 🎯 Objetivo

Desenvolver um pipeline completo de dados, desde a ingestão até a disponibilização para análise, utilizando ferramentas amplamente adotadas no mercado e boas práticas de engenharia de dados.

---

## 🧱 Arquitetura

Kaggle → Data Lake (MinIO) → PySpark → Bronze → Silver → Gold → Airflow

---

## 🛠️ Tecnologias Utilizadas

* Python
* PySpark
* Apache Airflow
* Docker
* MinIO (simulação de S3)
* Kaggle API
* Git / GitHub

---

## ⚙️ Ambiente

O ambiente foi provisionado utilizando Docker, contendo:

* **Airflow** → Orquestração de pipelines
* **MinIO** → Data Lake (simulando S3)

### 🔗 Acessos

* Airflow: http://localhost:8080

  * user: airflow
  * senha: airflow

* MinIO: http://localhost:9001

  * user: admin
  * senha: password

---

## 📂 Estrutura do Projeto

```
data-engineering-pipeline-olist/
│
├── dags/                  # DAGs do Airflow
├── src/                   # Código do pipeline
│   ├── ingestion/         # Ingestão de dados
│   ├── processing/        # Transformações (Silver/Gold)
│   └── utils/             # Funções auxiliares
│
├── datalake/              # Data Lake local (MinIO)
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── data/                  # Dados brutos (Kaggle - não versionado)
├── docker/                # Configuração Docker (Airflow + MinIO)
├── logs/                  # Logs do Airflow (ignorado no Git)
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 🧱 Camadas do Data Lake

### 🟤 Bronze

* Dados brutos
* Sem tratamento
* Origem: Kaggle

### ⚪ Silver

* Dados limpos e padronizados
* Tratamento de tipos, nulos e inconsistências

### 🟡 Gold

* Dados agregados
* Métricas de negócio
* Pronto para análise

---

## ✅ Status Atual do Projeto

Até o momento, já foi implementado:

* [x] Criação do ambiente virtual (.venv)
* [x] Configuração do projeto no GitHub
* [x] Criação da estrutura do projeto
* [x] Setup do Docker
* [x] Subida do Airflow
* [x] Subida do MinIO (Data Lake)
* [x] Configuração de volumes (dags, logs, plugins)
* [x] Configuração do `.gitignore`

---

## 🔄 Próximas etapas

### 🔹 Fase 1 — Ingestão (Bronze)

* [ ] Download dos dados do Kaggle
* [ ] Ingestão para camada Bronze com PySpark
* [ ] Armazenamento em formato otimizado (Parquet)

### 🔹 Fase 2 — Processamento (Silver)

* [ ] Limpeza de dados
* [ ] Padronização de schemas
* [ ] Tratamento de nulos e inconsistências

### 🔹 Fase 3 — Camada Analítica (Gold)

* [ ] Criação de métricas de negócio
* [ ] Agregações (faturamento, pedidos, clientes, etc.)

### 🔹 Fase 4 — Orquestração

* [ ] Criação de DAGs no Airflow
* [ ] Automatização do pipeline completo

---

## 📊 Dataset

Dataset público utilizado:

👉 https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

---

## 🚀 Como executar o projeto

### 1. Subir ambiente Docker

```bash
cd docker
docker compose up airflow-init
docker compose up
```

---

### 2. Ativar ambiente Python

```bash
.venv\Scripts\activate
```

---

### 3. Instalar dependências

```bash
pip install -r requirements.txt
```

---

## 📍 Ponto Atual do Projeto (Importante)

O ambiente já está completamente configurado e funcional (Airflow + MinIO).

👉 Próximo passo ao continuar o desenvolvimento:

**Implementar ingestão de dados com PySpark na camada Bronze**

---

## 👤 Autor

**Sandro Luiz**

---

## 📌 Observação

Este projeto tem como objetivo aprendizado prático e simulação de cenários reais de engenharia de dados, seguindo boas práticas utilizadas no mercado.
