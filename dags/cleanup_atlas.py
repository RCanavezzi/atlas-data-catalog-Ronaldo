from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from requests.auth import HTTPBasicAuth
import time

# Configurações
ATLAS_CONFIG = {
    "url": "http://atlas:21000",
    "username": "admin",
    "password": "admin"
}

def get_all_entities(**context):
    """Task 1: Busca todas as entidades no Atlas"""
    print("🔍 Buscando todas as entidades no Atlas...")
    auth = HTTPBasicAuth(ATLAS_CONFIG["username"], ATLAS_CONFIG["password"])
    
    # Buscar todas as entidades
    response = requests.get(
        f"{ATLAS_CONFIG['url']}/api/atlas/v2/search/basic",
        params={"query": "*", "limit": 1000},
        auth=auth
    )
    
    if response.status_code != 200:
        raise Exception(f"Erro ao buscar entidades: {response.status_code}")
    
    entities = response.json().get('entities', [])
    
    # Filtrar apenas entidades ativas
    active_entities = [e for e in entities if e.get('status') == 'ACTIVE']
    
    print(f"📊 {len(active_entities)} entidades ativas encontradas")
    
    # Agrupar por tipo
    by_type = {}
    for entity in active_entities:
        entity_type = entity.get('typeName', 'unknown')
        if entity_type not in by_type:
            by_type[entity_type] = []
        by_type[entity_type].append(entity['guid'])
    
    for entity_type, guids in by_type.items():
        print(f"  - {entity_type}: {len(guids)} entidades")
    
    return [e['guid'] for e in active_entities]

def delete_columns(**context):
    """Task 2: Deleta todas as colunas"""
    print("🗑️ Deletando colunas...")
    auth = HTTPBasicAuth(ATLAS_CONFIG["username"], ATLAS_CONFIG["password"])
    
    # Buscar apenas colunas
    response = requests.get(
        f"{ATLAS_CONFIG['url']}/api/atlas/v2/search/basic",
        params={"query": "*", "typeName": "hive_column", "limit": 1000},
        auth=auth
    )
    
    if response.status_code == 200:
        entities = response.json().get('entities', [])
        columns = [e for e in entities if e.get('status') == 'ACTIVE']
        
        deleted_count = 0
        for column in columns:
            delete_response = requests.delete(
                f"{ATLAS_CONFIG['url']}/api/atlas/v2/entity/guid/{column['guid']}",
                auth=auth
            )
            if delete_response.status_code in [200, 204]:
                deleted_count += 1
            time.sleep(0.1)  # Evitar sobrecarga
        
        print(f"✅ {deleted_count} colunas deletadas")
        return deleted_count
    
    return 0

def delete_tables(**context):
    """Task 3: Deleta todas as tabelas"""
    print("🗑️ Deletando tabelas...")
    auth = HTTPBasicAuth(ATLAS_CONFIG["username"], ATLAS_CONFIG["password"])
    
    # Buscar apenas tabelas
    response = requests.get(
        f"{ATLAS_CONFIG['url']}/api/atlas/v2/search/basic",
        params={"query": "*", "typeName": "hive_table", "limit": 1000},
        auth=auth
    )
    
    if response.status_code == 200:
        entities = response.json().get('entities', [])
        tables = [e for e in entities if e.get('status') == 'ACTIVE']
        
        deleted_count = 0
        for table in tables:
            delete_response = requests.delete(
                f"{ATLAS_CONFIG['url']}/api/atlas/v2/entity/guid/{table['guid']}",
                auth=auth
            )
            if delete_response.status_code in [200, 204]:
                deleted_count += 1
            time.sleep(0.1)  # Evitar sobrecarga
        
        print(f"✅ {deleted_count} tabelas deletadas")
        return deleted_count
    
    return 0

def delete_databases(**context):
    """Task 4: Deleta todos os databases"""
    print("🗑️ Deletando databases...")
    auth = HTTPBasicAuth(ATLAS_CONFIG["username"], ATLAS_CONFIG["password"])
    
    # Buscar apenas databases
    response = requests.get(
        f"{ATLAS_CONFIG['url']}/api/atlas/v2/search/basic",
        params={"query": "*", "typeName": "hive_db", "limit": 1000},
        auth=auth
    )
    
    if response.status_code == 200:
        entities = response.json().get('entities', [])
        databases = [e for e in entities if e.get('status') == 'ACTIVE']
        
        deleted_count = 0
        for database in databases:
            delete_response = requests.delete(
                f"{ATLAS_CONFIG['url']}/api/atlas/v2/entity/guid/{database['guid']}",
                auth=auth
            )
            if delete_response.status_code in [200, 204]:
                deleted_count += 1
            time.sleep(0.1)  # Evitar sobrecarga
        
        print(f"✅ {deleted_count} databases deletados")
        return deleted_count
    
    return 0

def cleanup_remaining(**context):
    """Task 5: Limpa entidades restantes"""
    print("🧹 Limpando entidades restantes...")
    auth = HTTPBasicAuth(ATLAS_CONFIG["username"], ATLAS_CONFIG["password"])
    
    # Buscar entidades restantes
    response = requests.get(
        f"{ATLAS_CONFIG['url']}/api/atlas/v2/search/basic",
        params={"query": "*", "limit": 1000},
        auth=auth
    )
    
    if response.status_code == 200:
        entities = response.json().get('entities', [])
        remaining = [e for e in entities if e.get('status') == 'ACTIVE']
        
        deleted_count = 0
        for entity in remaining:
            delete_response = requests.delete(
                f"{ATLAS_CONFIG['url']}/api/atlas/v2/entity/guid/{entity['guid']}",
                auth=auth
            )
            if delete_response.status_code in [200, 204]:
                deleted_count += 1
            time.sleep(0.1)  # Evitar sobrecarga
        
        print(f"✅ {deleted_count} entidades restantes deletadas")
        
        # Verificação final
        final_response = requests.get(
            f"{ATLAS_CONFIG['url']}/api/atlas/v2/search/basic",
            params={"query": "*", "limit": 10},
            auth=auth
        )
        
        if final_response.status_code == 200:
            final_entities = final_response.json().get('entities', [])
            active_final = [e for e in final_entities if e.get('status') == 'ACTIVE']
            print(f"🎯 Atlas limpo! {len(active_final)} entidades ativas restantes")
        
        return deleted_count
    
    return 0

# Definição da DAG
default_args = {
    'owner': 'dataops-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'cleanup_atlas',
    default_args=default_args,
    description='Limpeza completa de todas as entidades do Apache Atlas',
    schedule=None,  # Execução manual apenas
    catchup=False,
    tags=['atlas', 'cleanup', 'maintenance']
)

# Tasks da DAG
get_entities_task = PythonOperator(
    task_id='get_all_entities',
    python_callable=get_all_entities,
    dag=dag
)

delete_columns_task = PythonOperator(
    task_id='delete_columns',
    python_callable=delete_columns,
    dag=dag
)

delete_tables_task = PythonOperator(
    task_id='delete_tables',
    python_callable=delete_tables,
    dag=dag
)

delete_databases_task = PythonOperator(
    task_id='delete_databases',
    python_callable=delete_databases,
    dag=dag
)

cleanup_remaining_task = PythonOperator(
    task_id='cleanup_remaining',
    python_callable=cleanup_remaining,
    dag=dag
)

# Definir dependências (ordem hierárquica: colunas → tabelas → databases → restantes)
get_entities_task >> delete_columns_task >> delete_tables_task >> delete_databases_task >> cleanup_remaining_task