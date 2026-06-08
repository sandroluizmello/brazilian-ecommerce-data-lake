import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import boto3

def carregar_configuracoes():
    """Carrega as variáveis de ambiente do arquivo .env."""
    env_path = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(dotenv_path=env_path)

def obter_arquivos_da_landing():
    """Conecta ao MinIO via boto3 e lista dinamicamente todos os CSVs na pasta olist/."""
    s3_client = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "password"),
    )
    
    bucket_name = "landing-zone"
    prefix = "olist/"
    
    print(f"🔍 Listando arquivos dinamicamente no bucket '{bucket_name}' sob o prefixo '{prefix}'...")
    
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if "Contents" not in response:
            print(f"⚠️ Nenhum arquivo encontrado no bucket {bucket_name}.")
            return []
            
        csv_files = []
        for obj in response["Contents"]:
            key = obj["Key"]
            # Filtra para garantir que estamos a recolher apenas arquivos .csv
            if key.endswith(".csv"):
                filename = key.split("/")[-1]
                if filename:
                    csv_files.append(filename)
                    
        return csv_files
        
    except Exception as e:
        print(f"❌ Erro ao listar arquivos no MinIO: {e}")
        return []

def create_spark():
    """Inicializa a SparkSession configurada para se conectar ao MinIO de forma estável."""
    hadoop_aws_package = "org.apache.hadoop:hadoop-aws:3.4.2"

    return SparkSession.builder \
        .appName("Bronze Ingestion - Dynamic MinIO") \
        .config("spark.jars.packages", hadoop_aws_package) \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://localhost:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "admin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "password")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.buffer.dir", "C:/hadoop/temp") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .getOrCreate()

def ingest_csv_to_parquet(spark, file_name):
    """Lê o CSV do bucket landing-zone e salva em Parquet no bucket bronze

    tratando quebras de linha dentro dos campos de texto (ex: reviews).
    """
    input_path = f"s3a://landing-zone/olist/{file_name}"
    folder_name = file_name.replace(".csv", "")
    output_path = f"s3a://bronze/{folder_name}"

    print(f"⏳ Ingerindo {file_name}...")

    try:
        # Adicionadas as opções multiline e escape para blindar a leitura de comentários
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiLine", "true") \
            .option("escape", '"') \
            .csv(input_path)
            
        df.write.mode("overwrite").parquet(output_path)
        print(f"✅ Salvo com sucesso em: {output_path}")
    except Exception as e:
        print(f"❌ Erro ao processar o arquivo {file_name}: {e}")

def main():
    carregar_configuracoes()
    
    # 1. Descobre os arquivos de forma dinâmica primeiro
    files = obter_arquivos_da_landing()
    
    if not files:
        print("🛑 Nenhum arquivo para processar. Finalizando pipeline.")
        return

    print(f"📦 Encontrados {len(files)} arquivos para processamento na camada Bronze.")
    
    # 2. Inicializa o Spark apenas se houver arquivos a processar
    print("🚀 Inicializando Spark Session...")
    spark = create_spark()
    
    for file in files:
        ingest_csv_to_parquet(spark, file)

    print("✨ Pipeline da camada Bronze finalizado com sucesso no MinIO!")
    spark.stop()

if __name__ == "__main__":
    main()