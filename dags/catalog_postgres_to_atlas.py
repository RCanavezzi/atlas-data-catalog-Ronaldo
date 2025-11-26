from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests
import psycopg2
import pandas as pd
from requests.auth import HTTPBasicAuth
import json

# Configurações
ATLAS_CONFIG = {
    "url": "http://atlas:21000",
    "username": "admin",
    "password": "admin"
}

POSTGRES_CONFIG = {
    "host": "postgres_erp",
    "port": 5432,
    "database": "northwind",
    "user": "postgres",
    "password": "postgres"
}

def extract_postgres_metadata(**context):
    """Task 1: Extrai metadados do PostgreSQL (apenas tabelas Northwind)"""
    print("🔄 Extraindo metadados do PostgreSQL Northwind...")
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    
    # Lista de tabelas do Northwind (excluindo tabelas do Airflow)
    northwind_tables = [
        'categories', 'customers', 'employees', 'order_details', 'orders',
        'products', 'shippers', 'suppliers', 'territories', 'region',
        'employee_territories', 'customer_demographics', 'customer_customer_demo'
    ]
    
    # Buscar apenas tabelas do Northwind
    placeholders = ','.join(['%s'] * len(northwind_tables))
    tables_df = pd.read_sql(f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name IN ({placeholders})
        ORDER BY table_name
    """, conn, params=northwind_tables)
    
    metadata = {}
    for _, row in tables_df.iterrows():
        table_name = row['table_name']
        
        # Buscar colunas
        columns_df = pd.read_sql("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = %s AND table_schema = 'public'
            ORDER BY ordinal_position
        """, conn, params=[table_name])
        
        metadata[table_name] = columns_df.to_dict('records')
    
    conn.close()
    print(f"📋 {len(metadata)} tabelas Northwind encontradas")
    print(f"Tabelas: {list(metadata.keys())}")
    
    # Armazenar metadados no XCom
    return metadata

def create_database_in_atlas(**context):
    """Task 2: Cria database no Atlas"""
    print("🗄️ Criando database no Atlas...")
    auth = HTTPBasicAuth(ATLAS_CONFIG["username"], ATLAS_CONFIG["password"])
    
    db_payload = {
        "entities": [{
            "typeName": "hive_db",
            "attributes": {
                "name": "northwind_postgres",
                "qualifiedName": "northwind_postgres@cluster1",
                "clusterName": "cluster1"
            },
            "guid": -1
        }]
    }
    
    response = requests.post(
        f"{ATLAS_CONFIG['url']}/api/atlas/v2/entity/bulk",
        json=db_payload, 
        auth=auth
    )
    
    if response.status_code in [200, 201]:
        result = response.json()
        if 'mutatedEntities' in result and 'CREATE' in result['mutatedEntities']:
            db_guid = result['mutatedEntities']['CREATE'][0]['guid']
            print(f"✅ Database criado com GUID: {db_guid}")
            return db_guid
    
    # Buscar database existente
    search_response = requests.get(
        f"{ATLAS_CONFIG['url']}/api/atlas/v2/search/basic",
        params={"query": "northwind_postgres"},
        auth=auth
    )
    
    if search_response.status_code == 200:
        entities = search_response.json().get('entities', [])
        db_entities = [e for e in entities if e.get('typeName') == 'hive_db']
        if db_entities:
            db_guid = db_entities[0]['guid']
            print(f"✅ Database existente encontrado com GUID: {db_guid}")
            return db_guid
    
    raise Exception("Falha ao criar/encontrar database no Atlas")

def catalog_tables_structure(**context):
    """Task 3: Cataloga estrutura das tabelas (sem colunas)"""
    print("📋 Catalogando estrutura das tabelas...")
    
    # Recuperar dados das tasks anteriores
    metadata = context['task_instance'].xcom_pull(task_ids='extract_metadata')
    db_guid = context['task_instance'].xcom_pull(task_ids='create_database')
    
    auth = HTTPBasicAuth(ATLAS_CONFIG["username"], ATLAS_CONFIG["password"])
    table_guids = {}
    
    for table_name in metadata.keys():
        # Remove tabela existente
        search_response = requests.get(
            f"{ATLAS_CONFIG['url']}/api/atlas/v2/search/basic",
            params={"query": table_name},
            auth=auth
        )
        
        if search_response.status_code == 200:
            entities = search_response.json().get('entities', [])
            for entity in entities:
                if entity.get('typeName') == 'hive_table' and entity.get('displayText') == table_name:
                    requests.delete(
                        f"{ATLAS_CONFIG['url']}/api/atlas/v2/entity/guid/{entity['guid']}",
                        auth=auth
                    )
        
        # Criar tabela
        table_entity = {
            "typeName": "hive_table",
            "attributes": {
                "name": table_name,
                "qualifiedName": f"northwind_postgres.{table_name}@cluster1",
                "db": {"guid": db_guid},
                "owner": "postgres"
            },
            "guid": -1
        }
        
        payload = {"entities": [table_entity]}
        response = requests.post(
            f"{ATLAS_CONFIG['url']}/api/atlas/v2/entity/bulk",
            json=payload,
            auth=auth
        )
        
        if response.status_code in [200, 201]:
            result = response.json()
            if 'mutatedEntities' in result and 'CREATE' in result['mutatedEntities']:
                table_guid = result['mutatedEntities']['CREATE'][0]['guid']
                table_guids[table_name] = table_guid
                print(f"  ✅ Tabela {table_name} criada")
            else:
                print(f"  ❌ Erro ao criar tabela {table_name}")
        else:
            print(f"  ❌ Erro HTTP ao criar tabela {table_name}: {response.status_code}")
    
    print(f"📋 {len(table_guids)} tabelas catalogadas")
    return table_guids

def catalog_columns(**context):
    """Task 4: Cataloga colunas de todas as tabelas"""
    print("📝 Catalogando colunas das tabelas...")
    
    # Recuperar dados das tasks anteriores
    metadata = context['task_instance'].xcom_pull(task_ids='extract_metadata')
    table_guids = context['task_instance'].xcom_pull(task_ids='catalog_tables')
    
    auth = HTTPBasicAuth(ATLAS_CONFIG["username"], ATLAS_CONFIG["password"])
    success_count = 0
    
    for table_name, columns in metadata.items():
        if table_name not in table_guids:
            print(f"  ⚠️ GUID da tabela {table_name} não encontrado")
            continue
            
        table_guid = table_guids[table_name]
        column_entities = []
        
        # Criar colunas
        for i, col in enumerate(columns, 1):
            column_entity = {
                "typeName": "hive_column",
                "attributes": {
                    "name": col['column_name'],
                    "qualifiedName": f"northwind_postgres.{table_name}.{col['column_name']}@cluster1",
                    "table": {"guid": table_guid},
                    "type": col['data_type'],
                    "position": i
                },
                "guid": -(i+1)
            }
            column_entities.append(column_entity)
        
        # Enviar colunas para Atlas
        if column_entities:
            payload = {"entities": column_entities}
            response = requests.post(
                f"{ATLAS_CONFIG['url']}/api/atlas/v2/entity/bulk",
                json=payload,
                auth=auth
            )
            
            if response.status_code in [200, 201]:
                success_count += 1
                print(f"  ✅ {table_name}: {len(column_entities)} colunas catalogadas")
            else:
                print(f"  ❌ Erro ao catalogar colunas de {table_name}: {response.status_code}")
    
    print(f"🎉 Colunas catalogadas para {success_count}/{len(metadata)} tabelas!")
    return success_count

# Definição da DAG
default_args = {
    'owner': 'dataops-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'catalog_postgres_to_atlas',
    default_args=default_args,
    description='Catalogação automática do PostgreSQL no Apache Atlas',
    schedule='@daily',
    catchup=False,
    tags=['atlas', 'postgres', 'catalogacao']
)

# Tasks da DAG
extract_metadata_task = PythonOperator(
    task_id='extract_metadata',
    python_callable=extract_postgres_metadata,
    dag=dag
)

create_database_task = PythonOperator(
    task_id='create_database',
    python_callable=create_database_in_atlas,
    dag=dag
)

catalog_tables_task = PythonOperator(
    task_id='catalog_tables',
    python_callable=catalog_tables_structure,
    dag=dag
)

catalog_columns_task = PythonOperator(
    task_id='catalog_columns',
    python_callable=catalog_columns,
    dag=dag
)

# Definir dependências
extract_metadata_task >> create_database_task >> catalog_tables_task >> catalog_columns_task