import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# 1. Uma única função wrapper para adiar a leitura do seu script original
def executar_ingestao_kaggle():
    # Garante o mapeamento do caminho apenas na hora da execução
    if '/opt/airflow' not in sys.path:
        sys.path.append('/opt/airflow')
    
    # A importação do seu arquivo original acontece estritamente aqui dentro!
    from src.ingestion.download_kaggle import main as run_download_kaggle
    
    # Dispara a execução do seu método main original
    run_download_kaggle()


default_args = {
    'owner': 'Sandro Luiz',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='pipeline_olist_data_lake',
    default_args=default_args,
    description='Pipeline de Ingestão e Processamento do Olist',
    start_date=datetime(2026, 1, 1),
    schedule='@weekly',
    catchup=False,
    tags=['olist', 'pyspark', 'minio'],
) as dag:

    # 2. A task agora chama a nossa função interna controlada
    task_download_kaggle = PythonOperator(
        task_id='download_kaggle_to_landing',
        python_callable=executar_ingestao_kaggle,
    )