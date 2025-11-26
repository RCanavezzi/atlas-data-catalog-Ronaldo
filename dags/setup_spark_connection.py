from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def create_spark_connection(**context):
    """Cria conexão Spark para container pyspark-aula"""
    from airflow.models import Connection
    from airflow import settings
    
    # Configuração da conexão Spark para container
    spark_conn = Connection(
        conn_id='spark_container',
        conn_type='spark',
        host='pyspark_aula_container',
        port=7077,
        extra={
            "spark-home": "/usr/local/spark",
            "spark-binary": "docker exec pyspark_aula_container spark-submit",
            "namespace": "default"
        }
    )
    
    # Adicionar conexão ao banco do Airflow
    session = settings.Session()
    
    # Verificar se conexão já existe
    existing_conn = session.query(Connection).filter(Connection.conn_id == 'spark_container').first()
    
    if existing_conn:
        print("✅ Conexão Spark container já existe")
        session.close()
        return
    
    # Criar nova conexão
    session.add(spark_conn)
    session.commit()
    session.close()
    
    print("✅ Conexão Spark container criada com sucesso!")

# DAG para setup da conexão
default_args = {
    'owner': 'dataops-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    'setup_spark_connection',
    default_args=default_args,
    description='Setup da conexão Spark para container pyspark-aula',
    schedule=None,  # Execução manual
    catchup=False,
    tags=['setup', 'spark', 'connection']
)

setup_task = PythonOperator(
    task_id='create_spark_connection',
    python_callable=create_spark_connection,
    dag=dag
)

setup_task