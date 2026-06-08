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

### 🟡 Gold

* Dados agregados
* Métricas de negócio
* Pronto para análise

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

---

## 🔄 Próximas etapas

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