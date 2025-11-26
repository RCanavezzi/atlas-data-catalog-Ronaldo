from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os

# Configurações
SPARK_JOB_PATH = "/opt/airflow/spark_jobs/northwind_to_iceberg.py"

def check_spark_job_exists(**context):
    """Verifica se o job Spark existe"""
    if os.path.exists(SPARK_JOB_PATH):
        print(f"✅ Job Spark encontrado: {SPARK_JOB_PATH}")
        return True
    else:
        print(f"❌ Job Spark não encontrado: {SPARK_JOB_PATH}")
        raise FileNotFoundError(f"Job Spark não encontrado: {SPARK_JOB_PATH}")



def validate_etl_results(**context):
    """Valida resultados do ETL via Atlas API"""
    print("🔍 Validando resultados do ETL...")
    
    import requests
    from requests.auth import HTTPBasicAuth
    
    atlas_config = {
        "url": "http://atlas:21000",
        "username": "admin",
        "password": "admin"
    }
    
    try:
        auth = HTTPBasicAuth(atlas_config["username"], atlas_config["password"])
        
        # Buscar tabelas raw no Atlas
        response = requests.get(
            f"{atlas_config['url']}/api/atlas/v2/search/basic",
            params={"query": "raw_", "typeName": "hive_table"},
            auth=auth
        )
        
        if response.status_code == 200:
            entities = response.json().get('entities', [])
            raw_tables = [e for e in entities if e.get('displayText', '').startswith('raw_')]
            
            print(f"📊 Tabelas raw encontradas no Atlas: {len(raw_tables)}")
            for table in raw_tables:
                print(f"  - {table.get('displayText')}: {table.get('status')}")
            
            print("✅ Validação concluída com sucesso!")
            return f"Found {len(raw_tables)} raw tables in Atlas"
        else:
            print(f"⚠️ Erro ao consultar Atlas: {response.status_code}")
            return "Validation completed with warnings"
            
    except Exception as e:
        print(f"❌ Erro na validação: {str(e)}")
        raise

# Definição da DAG
default_args = {
    'owner': 'dataops-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'etl_northwind_to_iceberg',
    default_args=default_args,
    description='ETL Northwind PostgreSQL para Iceberg Raw Layer com catalogação Atlas',
    schedule='@weekly',  # Execução semanal
    catchup=False,
    tags=['etl', 'spark', 'iceberg', 'atlas', 'northwind']
)

# Task 1: Verificar se job Spark existe
check_job_task = PythonOperator(
    task_id='check_spark_job',
    python_callable=check_spark_job_exists,
    dag=dag
)

# Task 2: Submeter job Spark no container pyspark-aula
submit_job_task = BashOperator(
    task_id='submit_spark_job',
    bash_command="""
    docker exec pyspark_aula_container spark-submit \
        --master local[*] \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.iceberg.type=hadoop \
        --conf spark.sql.catalog.iceberg.warehouse=/home/jovyan/iceberg-warehouse \
        --jars /usr/local/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar,/usr/local/spark/jars/postgresql-42.7.1.jar \
        /home/jovyan/work/spark_jobs/northwind_to_iceberg.py
    """,
    dag=dag
)

# Task 4: Validar resultados
validate_task = PythonOperator(
    task_id='validate_results',
    python_callable=validate_etl_results,
    dag=dag
)

# Definir dependências
check_job_task >> submit_job_task >> validate_task