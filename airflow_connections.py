#!/usr/bin/env python3
"""
Script para configurar conexões do Airflow
"""

import os
import sys

# Configurar variáveis de ambiente do Airflow
os.environ.setdefault('AIRFLOW__CORE__DAGS_FOLDER', '/opt/airflow/dags')
os.environ.setdefault('AIRFLOW__CORE__LOAD_EXAMPLES', 'False')

try:
    from airflow.models import Connection
    from airflow import settings
    
    def create_spark_connection():
        """Cria conexão Spark no Airflow para container pyspark-aula"""
        
        # Configuração da conexão Spark para container
        spark_conn = Connection(
            conn_id='spark_container',
            conn_type='spark',
            host='pyspark_aula_container',
            port=7077,
            extra={
                "spark-home": "/opt/spark",
                "spark-binary": "spark-submit",
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
    
    if __name__ == "__main__":
        create_spark_connection()
        
except ImportError as e:
    print(f"⚠️ Erro ao importar Airflow: {e}")
    print("Conexão será criada via DAG setup_spark_connection")
except Exception as e:
    print(f"⚠️ Erro ao criar conexão: {e}")
    print("Conexão será criada via DAG setup_spark_connection")