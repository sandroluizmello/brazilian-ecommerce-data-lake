# 🚀 Data Engineering Pipeline - Olist

Projeto de Engenharia de Dados com foco na construção de um pipeline moderno utilizando arquitetura de Data Lake (Bronze, Silver e Gold), simulando um ambiente real de produção.

---

## 🎯 Objetivo

Desenvolver um pipeline completo de dados, desde a ingestão até a disponibilização para análise, utilizando ferramentas amplamente adotadas no mercado.

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

---

## ⚙️ Ambiente

O ambiente foi provisionado utilizando Docker, contendo:

* **Airflow** → Orquestração de pipelines
* **MinIO** → Data Lake (simulando S3)

### Acessos:

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
│   ├── ingestion/
│   ├── processing/
│   └── utils/
│
├── datalake/              # Data Lake local (MinIO)
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── docker/                # Configuração Docker (Airflow + MinIO)
├── notebooks/             # Exploração de dados (opcional)
├── tests/                 # Testes (opcional)
│
├── requirements.txt
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

## ✅ Etapas já implementadas

* [x] Criação do ambiente virtual (.venv)
* [x] Configuração do projeto no GitHub
* [x] Setup do Docker
* [x] Subida do Airflow
* [x] Subida do MinIO (Data Lake)
* [x] Estrutura inicial do projeto

---

## 🔄 Próximas etapas

* [ ] Download dos dados do Kaggle
* [ ] Ingestão para camada Bronze
* [ ] Processamento com PySpark
* [ ] Criação das camadas Silver e Gold
* [ ] Orquestração com Airflow
* [ ] Criação de métricas de negócio

---

## 📊 Dataset

Será utilizado o dataset público de e-commerce brasileiro (Olist):

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

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

## 👤 Autor

**Sandro Luiz**

---

## 📌 Observação

Este projeto tem como objetivo aprendizado prático e simulação de cenários reais de engenharia de dados, utilizando boas práticas de mercado.
