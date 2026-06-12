import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, to_timestamp, to_date, translate
from pyspark.sql.types import IntegerType, DoubleType

# ==============================================================================
# 1. CONFIGURAÇÃO DO AMBIENTE (COMPARTILHADA)
# ==============================================================================

def create_spark_session():
    """Inicializa a sessão do PySpark com suporte a S3 (MinIO) detectando o ambiente."""
    print("🚀 Inicializando a sessão do PySpark com suporte a S3 (MinIO)...")
    
    # Detecção dinâmica se está rodando dentro do Docker ou local no Windows
    rodando_em_container = os.path.exists("/.dockerenv")
    if rodando_em_container:
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    else:
        minio_endpoint = os.getenv("MINIO_ENDPOINT_LOCAL", "http://localhost:9000")

    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "password")
    
    # Versão atualizada e estável do pacote Hadoop
    hadoop_aws_package = "org.apache.hadoop:hadoop-aws:3.4.2"
    
    # Configuração de diretório de buffer seguro para o Windows vs Linux (Docker)
    import platform
    default_buffer_dir = "C:/tmp/spark-s3a-buffer" if platform.system() == "Windows" else "/tmp/spark-s3a-buffer"
    os.makedirs(default_buffer_dir, exist_ok=True)
    
    return SparkSession.builder \
        .appName("Olist_Silver_Processing_MinIO") \
        .master("local[*]") \
        .config("spark.jars.packages", hadoop_aws_package) \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.buffer.dir", default_buffer_dir) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory") \
        .config("spark.hadoop.fs.s3a.committer.name", "directory") \
        .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace") \
        .getOrCreate()

# ==============================================================================
# 2. FUNÇÕES ESPECÍFICAS DE TRATAMENTO DE CADA TABELA
# ==============================================================================

def process_orders(spark):
    """Lê a tabela de pedidos do bucket Bronze, limpa, padroniza e remove dados inválidos."""
    print("⏳ Lendo olist_orders_dataset do bucket bronze (MinIO)...")
    
    bronze_path = "s3a://bronze/olist_orders_dataset"
    df_orders = spark.read.parquet(bronze_path)
    
    # 1. Seleção, padronização de tipos e limpeza de espaços (Trim)
    df_silver = df_orders.select(
        trim(col("order_id")).alias("order_id"),
        trim(col("customer_id")).alias("customer_id"),
        lower(trim(col("order_status"))).alias("order_status"),
        to_timestamp(col("order_purchase_timestamp")).alias("order_purchase_timestamp"),
        to_timestamp(col("order_approved_at")).alias("order_approved_at"),
        to_timestamp(col("order_delivered_carrier_date")).alias("order_delivered_carrier_date"),
        to_timestamp(col("order_delivered_customer_date")).alias("order_delivered_customer_date"),
        to_date(col("order_estimated_delivery_date")).alias("order_estimated_delivery_date")
    )
    
    # 2. LIMPEZA DE DADOS: Garantir que chaves primárias essenciais não sejam nulas ou vazias
    print("🧹 Removendo registros com chaves obrigatórias em branco (order_id / customer_id)...")
    df_silver = df_silver \
        .dropna(subset=["order_id", "customer_id"]) \
        .filter((col("order_id") != "") & (col("customer_id") != ""))
        
    # 3. (Opcional) Remover duplicados se houver risco de reprocessamento na Bronze
    df_silver = df_silver.dropDuplicates(["order_id"])
    
    return df_silver

def process_order_items(spark):
    """Processa a tabela olist_order_items_dataset."""
    print("\n⏳ [ITENS] Lendo do bucket bronze...")
    df_items = spark.read.parquet("s3a://bronze/olist_order_items_dataset")
    
    df_silver = df_items.select(
        trim(col("order_id")).alias("order_id"),
        col("order_item_id").cast(IntegerType()).alias("order_item_id"),
        trim(col("product_id")).alias("product_id"),
        trim(col("seller_id")).alias("seller_id"),
        to_timestamp(col("shipping_limit_date")).alias("shipping_limit_date"),
        col("price").cast(DoubleType()).alias("price"),
        col("freight_value").cast(DoubleType()).alias("freight_value")
    )
    
    print("🧹 [ITENS] Aplicando regras de limpeza...")
    df_silver = df_silver \
        .dropna(subset=["order_id", "product_id", "seller_id"]) \
        .filter((col("order_id") != "") & (col("product_id") != "") & (col("seller_id") != "")) \
        .dropDuplicates(["order_id", "order_item_id"])
        
    return df_silver

def process_order_payments(spark):
    """Processa a tabela olist_order_payments_dataset."""
    print("\n⏳ [PAGAMENTOS] Lendo do bucket bronze...")
    df_payments = spark.read.parquet("s3a://bronze/olist_order_payments_dataset")
    
    df_silver = df_payments.select(
        trim(col("order_id")).alias("order_id"),
        col("payment_sequential").cast(IntegerType()).alias("payment_sequential"),
        lower(trim(col("payment_type"))).alias("payment_type"),
        col("payment_installments").cast(IntegerType()).alias("payment_installments"),
        col("payment_value").cast(DoubleType()).alias("payment_value")
    )
    
    print("🧹 [PAGAMENTOS] Aplicando regras de limpeza...")
    df_silver = df_silver \
        .dropna(subset=["order_id"]) \
        .filter(col("order_id") != "") \
        .dropDuplicates(["order_id", "payment_sequential"])
        
    return df_silver

def process_products(spark):
    """Processa a tabela olist_products_dataset."""
    print("\n⏳ [PRODUTOS] Lendo do bucket bronze...")
    df_products = spark.read.parquet("s3a://bronze/olist_products_dataset")
    
    df_silver = df_products.select(
        trim(col("product_id")).alias("product_id"),
        lower(trim(col("product_category_name"))).alias("product_category_name"),
        col("product_name_lenght").cast(IntegerType()).alias("product_name_length"), # Corrigindo o 'lenght' do original
        col("product_description_lenght").cast(IntegerType()).alias("product_description_length"),
        col("product_photos_qty").cast(IntegerType()).alias("product_photos_qty"),
        col("product_weight_g").cast(DoubleType()).alias("product_weight_g"),
        col("product_length_cm").cast(DoubleType()).alias("product_length_cm"),
        col("product_height_cm").cast(DoubleType()).alias("product_height_cm"),
        col("product_width_cm").cast(DoubleType()).alias("product_width_cm")
    )
    
    print("🧹 [PRODUTOS] Aplicando regras de limpeza...")
    df_silver = df_silver \
        .dropna(subset=["product_id"]) \
        .filter(col("product_id") != "") \
        .dropDuplicates(["product_id"])
        
    return df_silver

def process_customers(spark):
    """Processa a tabela olist_customers_dataset."""
    print("\n⏳ [CLIENTES] Lendo do bucket bronze...")
    df_customers = spark.read.parquet("s3a://bronze/olist_customers_dataset")
    
    df_silver = df_customers.select(
        trim(col("customer_id")).alias("customer_id"),
        trim(col("customer_unique_id")).alias("customer_unique_id"),
        trim(col("customer_zip_code_prefix")).alias("customer_zip_code_prefix"),
        lower(trim(col("customer_city"))).alias("customer_city"),
        lower(trim(col("customer_state"))).alias("customer_state")
    )
    
    print("🧹 [CLIENTES] Aplicando regras de limpeza...")
    df_silver = df_silver \
        .dropna(subset=["customer_id", "customer_unique_id"]) \
        .filter((col("customer_id") != "") & (col("customer_unique_id") != "")) \
        .dropDuplicates(["customer_id"])
        
    return df_silver


def process_sellers(spark):
    """Processa a tabela olist_sellers_dataset."""
    print("\n⏳ [VENDEDORES] Lendo do bucket bronze...")
    df_sellers = spark.read.parquet("s3a://bronze/olist_sellers_dataset")
    
    df_silver = df_sellers.select(
        trim(col("seller_id")).alias("seller_id"),
        trim(col("seller_zip_code_prefix")).alias("seller_zip_code_prefix"),
        lower(trim(col("seller_city"))).alias("seller_city"),
        lower(trim(col("seller_state"))).alias("seller_state")
    )
    
    print("🧹 [VENDEDORES] Aplicando regras de limpeza...")
    df_silver = df_silver \
        .dropna(subset=["seller_id"]) \
        .filter(col("seller_id") != "") \
        .dropDuplicates(["seller_id"])
        
    return df_silver

def process_order_reviews(spark):
    """Processa a tabela olist_order_reviews_dataset."""
    print("\n⏳ [ANÁLISES] Lendo do bucket bronze...")
    df_reviews = spark.read.parquet("s3a://bronze/olist_order_reviews_dataset")
    
    df_silver = df_reviews.select(
        trim(col("review_id")).alias("review_id"),
        trim(col("order_id")).alias("order_id"),
        col("review_score").cast(IntegerType()).alias("review_score"),
        trim(col("review_comment_title")).alias("review_comment_title"),
        trim(col("review_comment_message")).alias("review_comment_message"),
        to_timestamp(col("review_creation_date")).alias("review_creation_date"),
        to_timestamp(col("review_answer_timestamp")).alias("review_answer_timestamp")
    )
    
    print("🧹 [ANÁLISES] Aplicando regras de limpeza...")
    df_silver = df_silver \
        .dropna(subset=["review_id", "order_id"]) \
        .filter((col("review_id") != "") & (col("order_id") != "")) \
        .dropDuplicates(["review_id", "order_id"])
        
    return df_silver

def process_geolocation(spark):
    """Processa a tabela olist_geolocation_dataset."""
    print("\n⏳ [GEOLOCALIZAÇÃO] Lendo do bucket bronze...")
    df_geolocation = spark.read.parquet("s3a://bronze/olist_geolocation_dataset")

    # Criamos um mapeamento de caracteres com acento para caracteres sem acento
    acentos = "áàâãéèêíìîóòôõúùûç"
    sem_acentos = "aaaaeeeiiioooouuuc"
    
    df_silver = df_geolocation.select(
        trim(col("geolocation_zip_code_prefix")).alias("geolocation_zip_code_prefix"),
        col("geolocation_lat").cast(DoubleType()).alias("geolocation_lat"),
        col("geolocation_lng").cast(DoubleType()).alias("geolocation_lng"),
        translate(lower(trim(col("geolocation_city"))), acentos, sem_acentos).alias("geolocation_city"),
        lower(trim(col("geolocation_state"))).alias("geolocation_state")
    )
    
    print("🧹 [GEOLOCALIZAÇÃO] Aplicando regras de limpeza...")
    # Removendo nulos nas chaves principais
    df_silver = df_silver \
        .dropna(subset=["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"])
    
    # Agrupamos por CEP/Cidade/Estado para pegar a latitude e longitude média de cada região.
    # Isso reduz drasticamente o tamanho da tabela, deixando-a leve e performática para a Gold!
    df_silver = df_silver.groupBy(
        "geolocation_zip_code_prefix", 
        "geolocation_city", 
        "geolocation_state"
    ).agg(
        {"geolocation_lat": "avg", "geolocation_lng": "avg"}
    ).withColumnRenamed("avg(geolocation_lat)", "geolocation_lat") \
     .withColumnRenamed("avg(geolocation_lng)", "geolocation_lng")
        
    return df_silver

# ==============================================================================
# 3. ORQUESTRADOR DO PIPELINE (MAESTRO)
# ==============================================================================

def main():
    spark = create_spark_session()
    
    # Lista mapeando a 'função de tratamento' ao 'nome do diretório de destino'
    pipeline_tabelas = [
        (process_orders, "olist_orders_dataset"),
        (process_order_items, "olist_order_items_dataset"),
        (process_order_payments, "olist_order_payments_dataset"),
        (process_products, "olist_products_dataset"),
        (process_customers, "olist_customers_dataset"),
        (process_sellers, "olist_sellers_dataset"),
        (process_order_reviews, "olist_order_reviews_dataset"),
        (process_geolocation, "olist_geolocation_dataset")
    ]
    
    for funcao_processamento, nome_tabela in pipeline_tabelas:
        try:
            # Executa a limpeza específica da tabela
            df_silver = funcao_processamento(spark)
            
            # Caminho de destino unificado
            silver_output_path = f"s3a://silver/{nome_tabela}"
            print(f"💾 Salvando dados limpos na Silver: {silver_output_path}")
            
            df_silver.write \
                .mode("overwrite") \
                .parquet(silver_output_path)
                
            print(f"✅ Tabela {nome_tabela} concluída com sucesso!")
            
        except Exception as e:
            print(f"❌ Erro ao processar a tabela {nome_tabela}: {e}")
            
    print("\n🛑 Finalizando o pipeline e encerrando a sessão do Spark.")
    spark.stop()

if __name__ == "__main__":
    main()