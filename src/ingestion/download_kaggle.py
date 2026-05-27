import os
import shutil
from pathlib import Path
from dotenv import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi
import boto3
from botocore.exceptions import ClientError

DATASET = "olistbr/brazilian-ecommerce"
TMP_DIR = Path("data/tmp_staging")
BUCKET_NAME = "landing-zone"

def carregar_configuracoes():
    """Carrega as variáveis de ambiente do arquivo .env."""
    env_path = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(dotenv_path=env_path)
    
    # Injeta token do Kaggle no ambiente
    token = os.getenv('KAGGLE_API_TOKEN')
    if token:
        os.environ['KAGGLE_API_TOKEN'] = token

def obter_cliente_minio():
    """Inicializa e retorna o cliente boto3 configurado para o MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "password"),
    )

def garantir_bucket_no_minio(s3_client, bucket_name):
    """Verifica se o bucket existe no MinIO; se não existirá, cria-o."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"🪣 Bucket '{bucket_name}' já existe no MinIO.")
    except ClientError:
        print(f"🪣 Bucket '{bucket_name}' não encontrado. Criando...")
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' criado com sucesso.")

def upload_para_minio(s3_client, local_dir, bucket_name):
    """Varre o diretório temporário e envia todos os arquivos CSV para o MinIO."""
    print("⏳ Iniciando o upload dos arquivos para o Data Lake (MinIO)...")
    for arquivo in local_dir.glob("*.csv"):
        print(f"   ⬆️ Enviando {arquivo.name}...")
        s3_client.upload_file(
            Filename=str(arquivo),
            Bucket=bucket_name,
            Key=f"olist/{arquivo.name}"  # Organiza dentro de uma pasta virtual no MinIO
        )
    print("✅ Todos os arquivos foram salvos com sucesso no MinIO!")

def main():
    carregar_configuracoes()
    s3_client = obter_cliente_minio()
    
    # 1. Garante a infraestrutura no MinIO
    garantir_bucket_no_minio(s3_client, BUCKET_NAME)

    # 2. Cria diretório temporário local
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    try:
        # 3. Download do Kaggle
        print(f"📥 Baixando dataset {DATASET} do Kaggle...")
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(DATASET, path=str(TMP_DIR), unzip=True)
        print("✅ Download e descompactação concluídos localmente.")

        # 4. Upload para o MinIO
        upload_para_minio(s3_client, TMP_DIR, BUCKET_NAME)

    except Exception as e:
        print(f"❌ Erro durante o processo: {e}")
    
    finally:
        # 5. Limpeza do Staging Local (Garante que o disco não fique poluído)
        if TMP_DIR.exists():
            print("🧹 Limpando arquivos temporários locais...")
            shutil.rmtree(TMP_DIR)
            print("✨ Concluído! Nenhum arquivo residual ficou no seu disco rígido.")

if __name__ == "__main__":
    main()