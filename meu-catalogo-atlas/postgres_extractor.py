import psycopg2
import pandas as pd
import requests
from config import POSTGRES_CONFIG, ATLAS_URL

def get_postgres_metadata():
    """Extrai metadados estruturais do PostgreSQL"""
    # Estabelece conexão com o PostgreSQL usando as configurações definidas
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    
    # Query SQL para buscar todas as tabelas do schema 'public'
    tables_df = pd.read_sql("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'  -- Apenas tabelas do schema público
        ORDER BY table_name            -- Ordena alfabeticamente
    """, conn)
    
    # Dicionário para armazenar metadados de todas as tabelas
    metadata = {}
    
    # Itera sobre cada tabela encontrada
    for _, row in tables_df.iterrows():
        table_name = row['table_name']  # Nome da tabela atual
        
        # Query SQL para buscar colunas da tabela específica
        columns_df = pd.read_sql("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = %s AND table_schema = 'public'
            ORDER BY ordinal_position  -- Ordena pela posição original da coluna
        """, conn, params=[table_name]) 
        
        #Chave primaria
        pk_df = pd.read_sql("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY' 
            AND tc.table_name = %s AND tc.table_schema = 'public'
        """, conn, params=[table_name])

        #Relacionamentos (foreign keys)
        fk_df = pd.read_sql("""
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM 
                information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                  AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' 
              AND tc.table_name = %s AND tc.table_schema = 'public';
        """, conn, params=[table_name])
        
        # Converte DataFrame para lista de dicionários e armazena
        metadata[table_name] = {
            'columns': columns_df.to_dict(orient='records'),
            'primary_keys': pk_df['column_name'].tolist(),
            'foreign_keys': fk_df.to_dict(orient='records')
        }
    
    # Fecha a conexão com o banco
    conn.close()
    return metadata

# Executa a extração de metadados
postgres_metadata = get_postgres_metadata()
print(f"📋 {len(postgres_metadata)} tabelas encontradas")
print(f"Exemplo: {list(postgres_metadata.keys())[:3]}")

def create_database_in_atlas():
    """Cria ou localiza database no Atlas"""
    # Payload JSON para criar entidade do tipo 'hive_db' no Atlas
    db_payload = {
        "entities": [{
            "typeName": "hive_db",                    # Tipo de entidade (database)
            "attributes": {
                "name": "northwind_postgres",          # Nome do database
                "qualifiedName": "northwind_postgres@cluster1",  # Nome único global
                "clusterName": "cluster1"              # Nome do cluster
            },
            "guid": -1                                 # GUID temporário (será gerado pelo Atlas)
        }]
    }
    
    # Tenta criar o database via API REST do Atlas
    response = requests.post(f"{ATLAS_URL}/api/atlas/v2/entity/bulk", 
                            json=db_payload, auth=auth)
    
    # Se criação foi bem-sucedida, retorna o GUID gerado
    if response.status_code in [200, 201]:
        result = response.json()
        if 'mutatedEntities' in result and 'CREATE' in result['mutatedEntities']:
            return result['mutatedEntities']['CREATE'][0]['guid']
    
    # Se não conseguiu criar, busca database existente
    search_response = requests.get(f"{ATLAS_URL}/api/atlas/v2/search/basic", 
                                 params={"query": "northwind_postgres"}, auth=auth)
    
    # Processa resultado da busca
    if search_response.status_code == 200:
        entities = search_response.json().get('entities', [])
        # Filtra apenas entidades do tipo 'hive_db'
        db_entities = [e for e in entities if e.get('typeName') == 'hive_db']
        if db_entities:
            return db_entities[0]['guid']  # Retorna GUID do primeiro database encontrado
    
    return None  # Retorna None se não conseguiu criar nem encontrar

# Executa criação/busca do database
db_guid = create_database_in_atlas()
print(f"✅ Database GUID: {db_guid}")