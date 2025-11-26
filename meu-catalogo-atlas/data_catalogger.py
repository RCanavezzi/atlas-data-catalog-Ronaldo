import requests
from config import ATLAS_URL

def catalog_data(table_name, columns, db_guid):
    """Recria tabela com colunas integradas no Atlas"""
    
    # Primeiro, remove tabela existente se houver
    search_response = requests.get(f"{ATLAS_URL}/api/atlas/v2/search/basic", 
                                  params={"query": table_name}, auth=auth)
    
    # Se encontrou entidades, deleta as tabelas existentes
    if search_response.status_code == 200:
        entities = search_response.json().get('entities', [])
        for entity in entities:
            # Verifica se é uma tabela com o nome exato
            if entity.get('typeName') == 'hive_table' and entity.get('displayText') == table_name:
                # Deleta a entidade usando seu GUID
                requests.delete(f"{ATLAS_URL}/api/atlas/v2/entity/guid/{entity['guid']}", auth=auth)
                print(f"  🗑️ Deletada tabela existente: {table_name}")
    
    # Lista para armazenar todas as entidades (tabela + colunas)
    entities = []
    
    # Cria entidade da tabela
    table_entity = {
        "typeName": "hive_table",                    # Tipo de entidade (tabela)
        "attributes": {
            "name": table_name,                       # Nome da tabela
            "qualifiedName": f"northwind_postgres.{table_name}@cluster1",  # Nome único
            "db": {"guid": db_guid},                  # Referência ao database pai
            "owner": "postgres"                       # Proprietário da tabela
        },
        "guid": -1                                    # GUID temporário negativo
    }
    entities.append(table_entity)
    
    # Cria entidades das colunas
    for i, col in enumerate(columns, 1):              # Enumera começando do 1
        column_entity = {
            "typeName": "hive_column",                # Tipo de entidade (coluna)
            "attributes": {
                "name": col['column_name'],            # Nome da coluna
                "qualifiedName": f"northwind_postgres.{table_name}.{col['column_name']}@cluster1",
                "table": {"guid": -1},                 # Referência à tabela (GUID -1)
                "type": col['data_type'],              # Tipo de dados da coluna
                "position": i                          # Posição da coluna na tabela
            },
            "guid": -(i+1)                             # GUID temporário negativo único
        }
        entities.append(column_entity)
    
    # Envia todas as entidades (tabela + colunas) em uma única requisição
    payload = {"entities": entities}
    response = requests.post(f"{ATLAS_URL}/api/atlas/v2/entity/bulk", 
                           json=payload, auth=auth)
    
    # Verifica se a criação foi bem-sucedida
    if response.status_code in [200, 201]:
        result = response.json()
        created = result.get('mutatedEntities', {}).get('CREATE', [])
        print(f"  ✅ {table_name}: {len(created)} entidades criadas")
        return True
    else:
        print(f"  ❌ Erro {table_name}: {response.status_code}")
        return False

# Executa catalogação de todas as tabelas
if db_guid:
    print(f"🔄 Recriando {len(postgres_metadata)} tabelas com colunas integradas...")
    success_count = 0
    
    # Processa cada tabela individualmente
    for table_name, columns in postgres_metadata.items():
        print(f"\n📋 Processando: {table_name}")
        success = catalog_data(table_name, columns, db_guid)
        if success:
            success_count += 1
    
    print(f"\n✅ {success_count}/{len(postgres_metadata)} tabelas recriadas com sucesso!")
    print("   Verifique no Atlas: todas as colunas devem aparecer no schema das tabelas")
else:
    print("❌ Database não disponível")

# Busca todas as entidades relacionadas ao projeto northwind_postgres
search_response = requests.get(f"{ATLAS_URL}/api/atlas/v2/search/basic", 
                              params={"query": "*", "limit": 200}, auth=auth)

# Processa e exibe resultados da catalogação
if search_response.status_code == 200:
    entities = search_response.json().get('entities', [])
    
    # Filtra entidades por tipo e status ativo
    databases = [e for e in entities if e.get('typeName') == 'hive_db' and e.get('status') == 'ACTIVE']
    tables = [e for e in entities if e.get('typeName') == 'hive_table' and e.get('status') == 'ACTIVE']
    columns = [e for e in entities if e.get('typeName') == 'hive_column' and e.get('status') == 'ACTIVE']
    
    # Exibe estatísticas finais
    print("📊 Resultado Final do Catálogo:")
    print(f"  🗄️ Databases: {len(databases)}")
    print(f"  📋 Tabelas: {len(tables)}")
    print(f"  📝 Colunas: {len(columns)}")
    
    # Mostra algumas tabelas como exemplo
    if tables:
        print("\n📋 Tabelas catalogadas (primeiras 5):")
        for i, table in enumerate(tables[:5], 1):
            print(f"  {i}. {table.get('displayText')}")
    
    # Informações de acesso ao Atlas
    print(f"\n🎉 Acesse o Atlas: http://localhost:21000")
    print(f"   Usuário: admin | Senha: admin")
    print(f"\n💡 Dica: Navegue até 'Search' e busque por 'northwind_postgres' para ver o catálogo completo")
else:
    print(f"❌ Erro na verificação: {search_response.status_code}")