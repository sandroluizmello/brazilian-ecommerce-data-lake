import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


# ==============================================================================
# FUNÇÕES WRAPPER
# Cada wrapper adia o import do módulo original para o momento da execução,
# garantindo que o path '/opt/airflow' já esteja disponível no sys.path
# e que as variáveis de ambiente estejam corretamente carregadas.
# ==============================================================================

def executar_ingestao_kaggle():
    """Wrapper para o download dos dados do Kaggle e upload para a Landing Zone (MinIO)."""
    if '/opt/airflow' not in sys.path:
        sys.path.append('/opt/airflow')

    from src.ingestion.download_kaggle import main as run_download_kaggle
    run_download_kaggle()


def executar_ingestao_bronze():
    """Wrapper para a ingestão da Landing Zone -> Bronze (CSV para Parquet)."""
    if '/opt/airflow' not in sys.path:
        sys.path.append('/opt/airflow')

    from src.ingestion.bronze_ingestion import main as run_bronze_ingestion
    run_bronze_ingestion()


def executar_processamento_silver():
    """Wrapper para o processamento e limpeza da camada Silver (8 tabelas)."""
    if '/opt/airflow' not in sys.path:
        sys.path.append('/opt/airflow')

    from src.processing.silver_processing import main as run_silver_processing
    run_silver_processing()


def executar_processamento_gold():
    """Wrapper para a modelagem dimensional da camada Gold (Star Schema)."""
    if '/opt/airflow' not in sys.path:
        sys.path.append('/opt/airflow')

    from src.processing.gold_processing import main as run_gold_processing
    run_gold_processing()


# ==============================================================================
# CONFIGURAÇÃO DA DAG
# ==============================================================================

default_args = {
    'owner': 'Sandro Luiz',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='pipeline_olist_data_lake',
    default_args=default_args,
    description='Pipeline completo de Ingestão e Processamento do Olist (Bronze, Silver e Gold)',
    start_date=datetime(2026, 1, 1),
    schedule='@weekly',
    catchup=False,
    tags=['olist', 'pyspark', 'minio'],
) as dag:

    task_download_kaggle = PythonOperator(
        task_id='download_kaggle_to_landing',
        python_callable=executar_ingestao_kaggle,
    )

    task_bronze_ingestion = PythonOperator(
        task_id='bronze_ingestion',
        python_callable=executar_ingestao_bronze,
    )

    task_silver_processing = PythonOperator(
        task_id='silver_processing',
        python_callable=executar_processamento_silver,
    )

    task_gold_processing = PythonOperator(
        task_id='gold_processing',
        python_callable=executar_processamento_gold,
    )

    # Definição da ordem de execução do pipeline:
    # Landing Zone -> Bronze -> Silver -> Gold
    task_download_kaggle >> task_bronze_ingestion >> task_silver_processing >> task_gold_processing