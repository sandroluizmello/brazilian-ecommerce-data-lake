import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr

def carregar_configuracoes():
    """Carrega as variáveis de ambiente do arquivo .env."""
    env_path = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(dotenv_path=env_path)

def create_spark():
    """Inicializa a SparkSession configurada para o MinIO com correção de buffer local."""
    hadoop_aws_package = "org.apache.hadoop:hadoop-aws:3.4.2"
    
    # Criando explicitamente um diretório temporário local seguro e sem espaços
    # Se estiver no Windows, criará algo como C:\tmp\spark-s3a-buffer
    buffer_dir = os.path.join(os.environ.get("SystemDrive", "C:"), os.sep, "tmp", "spark-s3a-buffer")
    os.makedirs(buffer_dir, exist_ok=True)
    
    return SparkSession.builder \
        .appName("Gold Processing - Star Schema") \
        .config("spark.jars.packages", hadoop_aws_package) \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://localhost:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "admin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "password")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.buffer.dir", buffer_dir) \
        .getOrCreate()

def build_fato_vendas(spark):
    print("\n⏳ [GOLD - FATO_VENDAS] Lendo tabelas da camada Silver...")
    # Carrega as tabelas necessárias da Silver
    df_orders = spark.read.parquet("s3a://silver/olist_orders_dataset")
    df_items = spark.read.parquet("s3a://silver/olist_order_items_dataset")
    
    print("🔗 [GOLD - FATO_VENDAS] Cruzando dados e gerando chaves dimensionais...")
    
    # Unificando Itens e Pedidos através do order_id
    fato_vendas = df_items.join(df_orders, on="order_id", how="inner")
    
    # Seleção e renomeação de colunas com foco em negócio (Métricas + Chaves)
    fato_final = fato_vendas.select(
        # Chaves (Dimensões)
        col("order_id").alias("sk_pedido"),
        col("customer_id").alias("nk_cliente"),
        col("product_id").alias("nk_produto"),
        col("seller_id").alias("nk_vendedor"),
        
        # Métricas Quantitativas (Fatos)
        col("order_item_id").alias("quantidade_itens"),
        col("price").alias("valor_produto"),
        col("freight_value").alias("valor_frete"),
        (col("price") + col("freight_value")).alias("valor_total_item"),
        
        # Datas / Dimensão Tempo
        col("order_purchase_timestamp").alias("data_compra"),
        col("order_approved_at").alias("data_aprovacao"),
        col("order_delivered_carrier_date").alias("data_postagem"),
        col("order_delivered_customer_date").alias("data_entrega"),
        col("order_estimated_delivery_date").alias("data_estimada_entrega"),
        
        # Contexto rápido do pedido
        col("order_status").alias("status_pedido")
    )
    
    return fato_final

def build_dim_produtos(spark):
    print("📦 [GOLD] Gerando DIM_PRODUTOS...")
    df_products = spark.read.parquet("s3a://silver/olist_products_dataset")
    
    # Seleção de atributos descritivos do produto
    return df_products.select(
        col("product_id").alias("nk_produto"),
        col("product_category_name").alias("categoria_produto"),
        col("product_name_length").alias("comprimento_nome_produto"),
        col("product_description_length").alias("comprimento_descricao_produto"),
        col("product_photos_qty").alias("qtd_fotos_produto"),
        col("product_weight_g").alias("peso_gramas"),
        col("product_length_cm").alias("comprimento_cm"),
        col("product_height_cm").alias("altura_cm"),
        col("product_width_cm").alias("largura_cm")
    )

def build_dim_clientes(spark):
    print("👥 [GOLD] Gerando DIM_CLIENTES...")
    df_customers = spark.read.parquet("s3a://silver/olist_customers_dataset")
    
    return df_customers.select(
        col("customer_id").alias("nk_cliente"),
        col("customer_unique_id").alias("id_unico_cliente"),
        col("customer_zip_code_prefix").alias("cep_prefixo_cliente"),
        col("customer_city").alias("cidade_cliente"),
        col("customer_state").alias("estado_cliente")
    )

def build_dim_vendedores(spark):
    print("🏪 [GOLD] Gerando DIM_VENDEDORES...")
    df_sellers = spark.read.parquet("s3a://silver/olist_sellers_dataset")
    
    return df_sellers.select(
        col("seller_id").alias("nk_vendedor"),
        col("seller_zip_code_prefix").alias("cep_prefixo_vendedor"),
        col("seller_city").alias("cidade_vendedor"),
        col("seller_state").alias("estado_vendedor")
    )

def build_dim_pagamentos(spark):
    print("💳 [GOLD] Gerando DIM_PAGAMENTOS...")
    df_payments = spark.read.parquet("s3a://silver/olist_order_payments_dataset")
    
    return df_payments.select(
        col("order_id").alias("sk_pedido"),  # Amarração com a Fato
        col("payment_sequential").alias("sequencial_pagamento"),
        col("payment_type").alias("tipo_pagamento"),
        col("payment_installments").alias("qtd_parcelas"),
        col("payment_value").alias("valor_pagamento")
    )

def build_dim_geolocalizacao_clientes(spark):
    print("🌍 [GOLD] Gerando DIM_GEOLOCALIZACAO_CLIENTES...")
    df_geo = spark.read.parquet("s3a://silver/olist_geolocation_dataset")
    
    return df_geo.select(
        col("geolocation_zip_code_prefix").alias("nk_cep_prefixo_cliente"),
        col("geolocation_lat").alias("latitude_cliente"),
        col("geolocation_lng").alias("longitude_cliente"),
        col("geolocation_city").alias("cidade_cliente"),
        col("geolocation_state").alias("estado_cliente")
    )

def build_dim_geolocalizacao_vendedores(spark):
    print("🌍 [GOLD] Gerando DIM_GEOLOCALIZACAO_VENDEDORES...")
    df_geo = spark.read.parquet("s3a://silver/olist_geolocation_dataset")
    
    return df_geo.select(
        col("geolocation_zip_code_prefix").alias("nk_cep_prefixo_vendedor"),
        col("geolocation_lat").alias("latitude_vendedor"),
        col("geolocation_lng").alias("longitude_vendedor"),
        col("geolocation_city").alias("cidade_vendedor"),
        col("geolocation_state").alias("estado_vendedor")
    )

def build_dim_avaliacoes(spark):
    print("⭐ [GOLD] Gerando DIM_AVALIACOES (Reviews)...")
    df_reviews = spark.read.parquet("s3a://silver/olist_order_reviews_dataset")
    
    return df_reviews.select(
        col("review_id").alias("sk_avaliacao"),
        col("order_id").alias("sk_pedido"),  # Amarração com a Fato
        col("review_score").alias("nota_avaliacao"),
        col("review_comment_title").alias("titulo_comentario"),
        col("review_comment_message").alias("mensagem_comentario"),
        col("review_creation_date").alias("data_criacao_avaliacao"),
        col("review_answer_timestamp").alias("data_resposta_vendedor")
    )

def main():
    carregar_configuracoes()
    
    print("🚀 Inicializando Spark Session para a camada GOLD...")
    spark = create_spark()
    
    # Dicionário mapeando o nome da tabela analítica com sua respectiva função geradora
    pipelines_gold = {
        "fato_vendas": build_fato_vendas,
        "dim_produtos": build_dim_produtos,
        "dim_clientes": build_dim_clientes,
        "dim_vendedores": build_dim_vendedores,
        "dim_pagamentos": build_dim_pagamentos,
        "dim_geolocalizacao_clientes": build_dim_geolocalizacao_clientes,
        "dim_geolocalizacao_vendedores": build_dim_geolocalizacao_vendedores,
        "dim_avaliacoes": build_dim_avaliacoes
    }
    
    try:
        for tabela, funcao_builder in pipelines_gold.items():
            print(f"\n⏳ processando tabela analítica: {tabela.upper()}")
            df_resultado = funcao_builder(spark)
            
            output_path = f"s3a://gold/{tabela}"
            print(f"💾 Salvando no MinIO: {output_path}")
            df_resultado.write.mode("overwrite").parquet(output_path)
            print(f"✅ Tabela {tabela.upper()} integrada com sucesso!")
            
        print("\n🏆 [GOLD] Camada Analítica Dimensional gerada por completo!")
        
    except Exception as e:
        print(f"❌ Erro na execução em lote da camada Gold: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()