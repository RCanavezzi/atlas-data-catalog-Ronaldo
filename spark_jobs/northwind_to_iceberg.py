#!/usr/bin/env python3
"""
Spark Job: Northwind PostgreSQL to Iceberg Raw Layer
Extrai dados do PostgreSQL Northwind para camada raw no Iceberg
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import requests
from requests.auth import HTTPBasicAuth

# Configurações
POSTGRES_CONFIG = {
    "url": "jdbc:postgresql://postgres_erp:5432/northwind",
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

ATLAS_CONFIG = {
    "url": "http://atlas:21000",
    "username": "admin",
    "password": "admin"
}

NORTHWIND_TABLES = [
    'categories', 'customers', 'employees', 'order_details', 'orders',
    'products', 'shippers', 'suppliers', 'territories', 'region',
    'employee_territories', 'customer_demographics', 'customer_customer_demo'
]

def create_spark_session():
    """Cria sessão Spark com Iceberg"""
    return SparkSession.builder \
        .appName("NorthwindToIceberg") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse", "/home/jovyan/iceberg-warehouse") \
        .getOrCreate()

def extract_table_to_iceberg(spark, table_name):
    """Extrai uma tabela do PostgreSQL para Iceberg"""
    print(f"📊 Extraindo tabela: {table_name}")
    
    # Ler dados do PostgreSQL
    df = spark.read \
        .format("jdbc") \
        .options(**POSTGRES_CONFIG) \
        .option("dbtable", table_name) \
        .load()
    
    # Adicionar metadados de controle
    df_with_metadata = df \
        .withColumn("_extracted_at", current_timestamp()) \
        .withColumn("_source_system", lit("northwind_postgres")) \
        .withColumn("_layer", lit("raw"))
    
    # Criar tabela Iceberg se não existir
    iceberg_table = f"iceberg.raw_{table_name}"
    
    try:
        # Verificar se tabela existe
        spark.sql(f"DESCRIBE TABLE {iceberg_table}")
        print(f"  📋 Tabela {iceberg_table} já existe, fazendo append")
        df_with_metadata.writeTo(iceberg_table).append()
    except:
        print(f"  🆕 Criando nova tabela {iceberg_table}")
        df_with_metadata.writeTo(iceberg_table).create()
    
    record_count = df_with_metadata.count()
    print(f"  ✅ {record_count} registros extraídos para {iceberg_table}")
    
    return record_count, iceberg_table

def catalog_iceberg_table_in_atlas(table_name, iceberg_table, record_count):
    """Cataloga tabela Iceberg no Atlas"""
    print(f"📝 Catalogando {iceberg_table} no Atlas...")
    
    auth = HTTPBasicAuth(ATLAS_CONFIG["username"], ATLAS_CONFIG["password"])
    
    # Criar entidade da tabela Iceberg
    table_entity = {
        "typeName": "hive_table",
        "attributes": {
            "name": f"raw_{table_name}",
            "qualifiedName": f"iceberg.raw_{table_name}@datalake",
            "owner": "spark_etl",
            "description": f"Raw layer table from Northwind {table_name}",
            "parameters": {
                "format": "iceberg",
                "layer": "raw",
                "source_table": table_name,
                "record_count": str(record_count)
            }
        }
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
            print(f"  ✅ Tabela catalogada com GUID: {table_guid}")
            return table_guid
    
    print(f"  ⚠️ Erro ao catalogar tabela: {response.status_code}")
    return None

def create_etl_process_lineage(source_table, target_table_guid):
    """Cria processo ETL e linhagem no Atlas"""
    print(f"🔗 Criando linhagem ETL: {source_table} -> raw_{source_table}")
    
    auth = HTTPBasicAuth(ATLAS_CONFIG["username"], ATLAS_CONFIG["password"])
    
    # Buscar tabela fonte no Atlas
    search_response = requests.get(
        f"{ATLAS_CONFIG['url']}/api/atlas/v2/search/basic",
        params={"query": source_table, "typeName": "hive_table"},
        auth=auth
    )
    
    source_guid = None
    if search_response.status_code == 200:
        entities = search_response.json().get('entities', [])
        for entity in entities:
            if entity.get('displayText') == source_table:
                source_guid = entity['guid']
                break
    
    if not source_guid or not target_table_guid:
        print(f"  ⚠️ GUIDs não encontrados: source={source_guid}, target={target_table_guid}")
        return None
    
    # Criar processo ETL
    process_entity = {
        "typeName": "Process",
        "attributes": {
            "name": f"northwind_to_raw_{source_table}",
            "qualifiedName": f"etl.northwind_to_raw_{source_table}@spark",
            "description": f"ETL process: PostgreSQL {source_table} to Iceberg raw layer",
            "inputs": [{"guid": source_guid}],
            "outputs": [{"guid": target_table_guid}]
        }
    }
    
    payload = {"entities": [process_entity]}
    response = requests.post(
        f"{ATLAS_CONFIG['url']}/api/atlas/v2/entity/bulk",
        json=payload,
        auth=auth
    )
    
    if response.status_code in [200, 201]:
        result = response.json()
        if 'mutatedEntities' in result and 'CREATE' in result['mutatedEntities']:
            process_guid = result['mutatedEntities']['CREATE'][0]['guid']
            print(f"  ✅ Processo ETL criado com GUID: {process_guid}")
            return process_guid
    
    print(f"  ⚠️ Erro ao criar processo ETL: {response.status_code}")
    return None

def add_quality_tag(table_guid):
    """Adiciona tag de qualidade à tabela no Atlas"""
    print(f"🏷️ Adicionando tag de qualidade...")
    
    auth = HTTPBasicAuth(ATLAS_CONFIG["username"], ATLAS_CONFIG["password"])
    
    # Adicionar classificação de qualidade
    classification = {
        "typeName": "PII",  # Usar classificação existente ou criar nova
        "attributes": {},
        "entityGuid": table_guid
    }
    
    response = requests.post(
        f"{ATLAS_CONFIG['url']}/api/atlas/v2/entity/guid/{table_guid}/classifications",
        json=[classification],
        auth=auth
    )
    
    if response.status_code in [200, 204]:
        print(f"  ✅ Tag de qualidade adicionada")
        return True
    else:
        print(f"  ⚠️ Erro ao adicionar tag: {response.status_code}")
        return False

def main():
    """Função principal do job Spark"""
    print("🚀 Iniciando ETL Northwind -> Iceberg Raw Layer")
    
    spark = create_spark_session()
    
    try:
        total_records = 0
        processed_tables = []
        
        for table_name in NORTHWIND_TABLES:
            try:
                # Extrair dados para Iceberg
                record_count, iceberg_table = extract_table_to_iceberg(spark, table_name)
                total_records += record_count
                
                # Catalogar no Atlas
                table_guid = catalog_iceberg_table_in_atlas(table_name, iceberg_table, record_count)
                
                if table_guid:
                    # Criar linhagem
                    process_guid = create_etl_process_lineage(table_name, table_guid)
                    
                    # Adicionar tag de qualidade
                    add_quality_tag(table_guid)
                    
                    processed_tables.append({
                        'table': table_name,
                        'records': record_count,
                        'table_guid': table_guid,
                        'process_guid': process_guid
                    })
                
            except Exception as e:
                print(f"❌ Erro ao processar {table_name}: {str(e)}")
                continue
        
        print(f"\n🎉 ETL Concluído!")
        print(f"📊 Total de registros processados: {total_records}")
        print(f"📋 Tabelas processadas: {len(processed_tables)}")
        
        for table_info in processed_tables:
            print(f"  - {table_info['table']}: {table_info['records']} registros")
        
        return 0
        
    except Exception as e:
        print(f"❌ Erro geral no ETL: {str(e)}")
        return 1
    
    finally:
        spark.stop()

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)