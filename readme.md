# 🚀 Data Engineering Pipeline - Olist

Projeto de Engenharia de Dados com foco na construção de um pipeline moderno utilizando arquitetura de Data Lake (Bronze, Silver e Gold), simulando um ambiente real de produção.

---

## 🎯 Objetivo

Desenvolver um pipeline completo de dados, desde a ingestão até a disponibilização para análise, utilizando ferramentas amplamente adotadas no mercado e boas práticas de engenharia de dados.

---

## 🧱 Arquitetura

```
Kaggle → Data Lake (MinIO) → PySpark → Bronze → Silver → Gold → Airflow
```

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

* Dados brutos ingeridos do Kaggle via PySpark
* Formato Parquet (otimizado para leitura)
* Origem: landing-zone/olist/ → bronze/

### ⚪ Silver

* Dados limpos e padronizados
* Tratamento de tipos, nulos e inconsistências

### 🟡 Gold

* Dados agregados
* Métricas de negócio
* Pronto para análise

---

## 🔐 Integração com a API do Kaggle

Para permitir o download automatizado dos dados, foi realizada a integração com a API do Kaggle utilizando o novo modelo de autenticação via **API Token**.

### 🔑 1. Configurar variável no `.env`

Criar ou editar o arquivo `.env` na raiz do projeto:

```
KAGGLE_API_TOKEN=SEU_TOKEN_AQUI
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=password
```

---

### 🛠️ 2. Configuração no Windows

A biblioteca do Kaggle exige que o token esteja disponível em um arquivo local.

#### Criar diretório `.kaggle`

```powershell
mkdir -p ~/.kaggle
```

#### Criar arquivo `access_token`

```powershell
"SEU_TOKEN_AQUI" | Out-File -FilePath ~/.kaggle/access_token -Encoding ascii
```

---

### 🔍 3. Validar configuração

```powershell
cat ~/.kaggle/access_token
```

Saída esperada:

```
SEU_TOKEN_AQUI
```

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

### 4. Baixar dados do Kaggle para o MinIO (Landing Zone)

```bash
python src/ingestion/download_kaggle.py
```

Os arquivos CSV serão carregados para o bucket `landing-zone/olist/` no MinIO.

---

### 5. Ingerir dados na camada Bronze

```bash
python src/ingestion/bronze_ingestion.py
```

O script descobre automaticamente todos os CSVs disponíveis na landing zone, converte para Parquet com PySpark e salva no bucket `bronze/` do MinIO.

---

## ✅ Status Atual do Projeto

* [x] Criação do ambiente virtual (.venv)
* [x] Configuração do projeto no GitHub
* [x] Criação da estrutura do projeto
* [x] Setup do Docker
* [x] Subida do Airflow
* [x] Subida do MinIO (Data Lake)
* [x] Configuração de volumes (dags, logs, plugins)
* [x] Configuração do `.gitignore`
* [x] Integração com a API do Kaggle
* [x] Script de download automático dos dados (`download_kaggle.py`)
* [x] Ingestão para camada Bronze com PySpark (`bronze_ingestion.py`)
* [x] Armazenamento em formato otimizado (Parquet)

---

## 🔄 Próximas etapas

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

## 👤 Autor

**Sandro Luiz**

---

## 📌 Observação

Este projeto tem como objetivo aprendizado prático e simulação de cenários reais de engenharia de dados, seguindo boas práticas utilizadas no mercado.