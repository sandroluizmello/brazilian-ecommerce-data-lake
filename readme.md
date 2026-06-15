# 🚀 Data Engineering Pipeline - Olist

[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python 3.13](https://img.shields.io/badge/python-3.13+-blue)](https://www.python.org/downloads/)
[![PySpark 3.5](https://img.shields.io/badge/PySpark-3.5-orange)](https://spark.apache.org/)
[![Airflow 3.2.1](https://img.shields.io/badge/Airflow-3.2.1-017cee.svg)](https://airflow.apache.org/)
[![Docker Ready](https://img.shields.io/badge/Docker-Ready-2496ED.svg)](https://www.docker.com/)
[![Status](https://img.shields.io/badge/Status-100%25%20Complete-brightgreen)]()
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Projeto de **Engenharia de Dados** com foco na construção de um pipeline moderno utilizando arquitetura de Data Lake (Bronze, Silver e Gold), simulando um ambiente real de produção.

---

## 📑 Sumário

- [🎯 Objetivo](#-objetivo)
- [🧱 Arquitetura](#-arquitetura)
- [🛠️ Tecnologias](#-tecnologias-utilizadas)
- [⚙️ Ambiente](#-ambiente)
- [📂 Estrutura](#-estrutura-do-projeto)
- [🧱 Camadas](#-camadas-do-data-lake)
- [🔐 Configuração Kaggle](#-configuração-kaggle)
- [🚀 Como Executar](#-como-executar-o-projeto)
- [✅ Status](#-status-atual-do-projeto)
- [🏛️ Arquitetura de Dados](#-arquitetura-de-dados)
- [⚙️ Orquestração](#-arquitetura-de-orquestração-fase-5---concluída-)

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

### 🟤 Bronze (`bronze_ingestion.py`)

* Leitura dinâmica de todos os CSVs da Landing Zone via PySpark
* Persistência no bucket `bronze/` em formato **Parquet** (otimizado para leitura columnar)
* **Tratamento de Integridade** ⚠️: Implementação de suporte a múltiplas linhas (`multiLine=True`) e caracteres de escape (`escape='"'`) para blindar a ingestão contra comentários de clientes que possuem quebras de linha (`\n`) no texto original, evitando a corrupção estrutural do schema
  * 🔗 **Impacto no Silver**: Esse tratamento preventivo na Bronze elimina inconsistências que causariam falhas durante a validação e remoção de nulos no Silver
* Origem: landing-zone/olist/ → bronze/

### ⚪ Silver (`silver_processing.py`)

Centralização e automação da higienização de **8 tabelas do ecossistema**, aplicando transformações de qualidade de dados (*Data Quality*):

#### `olist_orders_dataset`
* Padronização de strings (trim, lower)
* Remoção de nulos na chave primária (`order_id`, `customer_id`)
* Cast rigoroso de campos de data/timestamp
* Remoção de duplicados

#### `olist_order_items_dataset`
* Ajuste dos tipos de dados para métricas financeiras (`DoubleType`)
* Padronização de contagem de itens (`IntegerType`)
* Validação de chaves primárias e estrangeiras
* Remoção de duplicados por ordem e item

#### `olist_order_payments_dataset`
* Padronização dos métodos de pagamento (caixa baixa)
* Validação de valores monetários (`DoubleType`)
* Cast de quantidade de parcelas (`IntegerType`)
* Remoção de duplicados por sequência de pagamento

#### `olist_products_dataset`
* Correção de nomenclatura de colunas (ex: `lenght` → `length`)
* Cast de dimensões físicas e peso para `DoubleType`
* Categorias padronizadas em caixa baixa (`lower`)
* Validação de chave primária (`product_id`)

#### `olist_customers_dataset`
* Correção de mapeamento geográfico (cidades e estados em caixa baixa)
* Preservação do tipo texto nos prefixos de CEP (evita perda de dígitos iniciais zero)
* Validação de chaves primárias (`customer_id`, `customer_unique_id`)
* Remoção de duplicados

#### `olist_sellers_dataset`
* Limpeza geográfica e padronização textual de estados e municípios
* Validação de chave primária (`seller_id`)
* Caixa baixa em localidades

#### `olist_order_reviews_dataset`
* Tratamento contra ruídos residuais
* Validação do score de satisfação (1 a 5)
* Tolerância a campos de texto nulos legítimos
* Remoção de duplicados por review

#### `olist_geolocation_dataset` ⭐ *Otimização Premium*
* Remoção de acentuação textual (`translate`) para agrupar chaves idênticas (ex: *são paulo* e *sao paulo*)
* **Agregação por coordenadas médias (`avg`) por prefixo de CEP**: reduz o volume massivo de redundâncias transacionais para uma tabela dimensão de alta performance
* Resultado: tabela compactada em Parquet com redução significativa de volume

### 🟡 Gold (`gold_processing.py`)

Camada analítica dimensional pronta para consumo em ferramentas de BI.

#### 🏛️ Modelo Dimensional Consolidado

**Fato Central:**
* `fato_vendas`: Tabela de fatos contendo todas as transações de venda com métricas quantitativas

**Dimensões Diretas:**
* `dim_produtos`: Atributos dos produtos (categoria, dimensões físicas, peso)
* `dim_clientes`: Perfil de clientes (localização, CEP)
* `dim_vendedores`: Informações dos vendedores (localização, CEP)
* `dim_pagamentos`: Métodos e valores de pagamento por pedido
* `dim_avaliacoes`: Reviews e feedbacks de clientes

**Dimensões com Extensão Geográfica (Role-Playing):**
* `dim_geolocalizacao_clientes`: Coordenadas e localização por CEP de clientes
* `dim_geolocalizacao_vendedores`: Coordenadas e localização por CEP de vendedores

#### 📊 Métricas na Fato Vendas

* `sk_pedido`: Chave da transação
* `nk_cliente`, `nk_produto`, `nk_vendedor`: Chaves das dimensões
* `quantidade_itens`: Unidades vendidas
* `valor_produto`, `valor_frete`, `valor_total_item`: Métricas monetárias
* `data_compra`, `data_aprovacao`, `data_postagem`, `data_entrega`: Dimensão temporal
* `status_pedido`: Contexto transacional

---

## 🔗 Dependências Entre Camadas

### Bronze → Silver

O tratamento de integridade implementado na **camada Bronze** é fundamental para o sucesso do processamento na **camada Silver**:

| Problema no Kaggle | Solução na Bronze | Benefício no Silver |
|---|---|---|
| Comentários de clientes com quebras de linha (`\n`) | `multiLine=True` e `escape='"'` | Schema consistente, validações de nulos funcionam corretamente |
| CSVs malformados por caracteres especiais | Parsing robusto com escape | Remoção de duplicados sem erros estruturais |
| Dados corrompidos durante leitura | Parquet com schema inferido corretamente | Cast de tipos e transformações precisas |

**Sem o tratamento preventivo na Bronze**, a camada Silver teria falhado ao tentar aplicar validações de chaves primárias e transformações de tipos.

---

## 🔐 Configuração Kaggle

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

### 6. Processar e limpar dados na camada Silver

```bash
python src/processing/silver_processing.py
```

O script realiza limpeza e padronização de **8 tabelas** do ecossistema Olist:
- Validação de tipos de dados
- Remoção de nulos em chaves primárias
- Padronização de strings (trim, lower)
- Correção de nomenclatura de colunas
- Agregações inteligentes (ex: geolocalização por CEP)
- Remoção de duplicados

Todos os dados processados são salvos no bucket `silver/` do MinIO em formato Parquet otimizado.

---

### 7. Processar e agregar dados na camada Gold

```bash
python src/processing/gold_processing.py
```

O script constrói o modelo dimensional com **1 tabela de fatos e 7 dimensões**:
- Fato central de vendas com métricas quantitativas
- Dimensões descritivas de produtos, clientes, vendedores
- Extensões geográficas com role-playing para análises espaciais
- Validação de integridade referencial

Todos os dados agregados são salvos no bucket `gold/` do MinIO em formato Parquet.

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
* [x] Limpeza e padronização da camada Silver (`silver_processing.py`)
* [x] Processamento de 8 tabelas com validações de Data Quality
* [x] Modelo dimensional na camada Gold (`gold_processing.py`)
* [x] Construção de 1 tabela de fatos + 7 dimensões
* [x] Orquestração com Apache Airflow (`pipeline_olist.py`)
* [x] DAG completa: Download → Bronze → Silver → Gold
* [x] Suporte para execução local e em containers (detecção automática)

---

## 🏛️ Arquitetura de Dados

O projeto segue a abordagem de construção incremental de um Data Lake moderno, validando cada camada individualmente antes da automatização completa do fluxo:

```
Kaggle
   ↓
Landing Zone (MinIO)
   ↓
Bronze (Ingestão)
   ↓
Silver (Qualidade)
   ↓
Gold (Dimensional)
```

### Camadas

#### Landing Zone
Área de recebimento dos dados brutos provenientes do Kaggle.

#### 🟤 Bronze
Camada responsável pela ingestão e persistência dos dados em formato Parquet, preservando ao máximo as informações originais e aplicando tratamentos estruturais necessários para garantir a integridade dos dados.

#### ⚪ Silver
Camada de qualidade de dados, responsável por padronizações, validações, remoção de duplicidades, tratamento de nulos, correções de schema e preparação para consumo analítico.

#### 🟡 Gold
Camada analítica dimensional destinada à criação de indicadores, métricas de negócio e tabelas agregadas para consumo por ferramentas de BI e Analytics.

---

## 🔄 Estratégia de Desenvolvimento

A construção do projeto foi realizada de forma **incremental**, seguindo boas práticas de Engenharia de Dados.

Cada camada foi validada individualmente para garantir:

* ✅ Confiabilidade das extrações
* ✅ Integridade dos dados
* ✅ Qualidade das transformações
* ✅ Facilidade de troubleshooting
* ✅ Reprodutibilidade dos processos

---

## ⚙️ Arquitetura de Orquestração (Fase 5 - CONCLUÍDA) ✅

O pipeline foi completamente automatizado utilizando **Apache Airflow**:

```
Airflow DAG: pipeline_olist_data_lake
   ↓
Task 1: download_kaggle_to_landing
   ↓
Task 2: bronze_ingestion
   ↓
Task 3: silver_processing
   ↓
Task 4: gold_processing
```

### 🔧 Configuração da DAG

**Arquivo**: `dags/pipeline_olist.py`

- **DAG ID**: `pipeline_olist_data_lake`
- **Schedule**: `@weekly` (executado toda segunda-feira)
- **Executor**: CeleryExecutor com Redis
- **Database**: PostgreSQL 16
- **Retry Policy**: 1 tentativa + 5 minutos de espera
- **Owner**: Sandro Luiz
- **Tags**: olist, pyspark, minio

### 🐳 Imagem Docker Customizada

**Arquivo**: `docker/Dockerfile`

Estende a imagem oficial do Airflow (`apache/airflow:3.2.1`) adicionando:
- OpenJDK (necessário para PySpark rodar a JVM)
- Exportação automática de `JAVA_HOME`
- Suporte completo a execução de jobs PySpark

### 🔄 Responsabilidades do Airflow

* 📅 **Agendamento**: Execução automática semanal
* 🔍 **Monitoramento**: Acompanhamento em tempo real via UI (localhost:8080)
* 🔗 **Dependências**: Controle de fluxo sequencial (Bronze → Silver → Gold)
* ⚠️ **Tratamento de Falhas**: Retry automático com delay de 5 minutos
* 📊 **Observabilidade**: Logs centralizados por task
* 🌐 **Compatibilidade**: Detecção automática de execução em container vs local

### 🎯 Como Executar via Airflow

1. **Subir os containers**:
```bash
cd docker
docker compose build      # Builda a imagem com Java
docker compose up airflow-init
docker compose up -d
```

2. **Acessar a UI do Airflow**:
   - URL: http://localhost:8080
   - User: airflow
   - Password: airflow

3. **Ativar e disparar a DAG**:
   - Localize `pipeline_olist_data_lake` na lista de DAGs
   - Clique no botão ON/OFF para ativar
   - Clique no botão ▶️ (Trigger) para executar manualmente
   - Ou aguarde o agendamento semanal automático

4. **Monitorar execução**:
   - Clique na DAG para ver o gráfico de tasks
   - Cada task mostra seu status (em execução, sucesso, falha)
   - Clique na task para ver logs detalhados

### 🔐 Variáveis de Ambiente Críticas

O arquivo `.env` deve conter:
```
KAGGLE_API_TOKEN=seu_token_aqui
MINIO_ENDPOINT=http://minio:9000           # Para execução no container
MINIO_ENDPOINT_LOCAL=http://localhost:9000 # Para execução local no Windows
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=password
```

**Nota**: Os scripts detectam automaticamente se estão rodando em container ou localmente via verificação de `/.dockerenv`, escolhendo o endpoint correto.

### ⚡ Problemas Resolvidos na Fase 5

| Problema | Solução |
|---|---|
| Java não instalado no container | Dockerfile estende imagem Airflow com OpenJDK |
| `MINIO_ENDPOINT=http://minio:9000` quebrava em execução local | Detecção automática com fallback para localhost |
| Buffer dir (`C:/hadoop/temp`) não existia em containers Linux | Script detecta SO e cria `/tmp/spark-s3a-buffer` no Linux |
| Imports do Kaggle falhavam por falta de variáveis | Load .env ANTES dos imports no download_kaggle.py |
| Caminho .env diferente entre container e local | Fallback inteligente: `/opt/airflow/.env` → `.env` local |

---

## 🏆 Resumo das Fases Completadas

| Fase | Descrição | Status |
|---|---|---|
| **Fase 1** | Download Kaggle + Upload MinIO | ✅ CONCLUÍDA |
| **Fase 2** | Ingestão Bronze (CSV → Parquet) | ✅ CONCLUÍDA |
| **Fase 3** | Limpeza Silver (8 tabelas, Data Quality) | ✅ CONCLUÍDA |
| **Fase 4** | Modelo Dimensional Gold (Star Schema) | ✅ CONCLUÍDA |
| **Fase 5** | Orquestração Airflow (DAG completa) | ✅ CONCLUÍDA |

**Progresso Total**: 100% (5/5 fases) ✅ **PROJETO FINALIZADO**

---

## 📌 Escopo do Projeto

Este é um projeto de **Engenharia de Dados** que cobre:

✅ Ingestão de dados  
✅ Processamento distribuído  
✅ Validação e qualidade  
✅ Modelagem dimensional  
✅ Orquestração automática  
✅ Infraestrutura reproduzível  

**Nota**: Consumo analítico (Power BI, dashboards) é escopo de **Ciência de Dados/BI**, portanto fora do escopo deste projeto.

---

## 📊 Dataset

Dataset público utilizado:

👉 https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

---

## 👤 Autor

**Sandro Luiz**

Pós-Graduando em Engenharia de Dados - PUC Minas

---

## 📌 Observação

Este projeto tem como objetivo aprendizado prático e simulação de cenários reais de engenharia de dados, seguindo boas práticas utilizadas no mercado.