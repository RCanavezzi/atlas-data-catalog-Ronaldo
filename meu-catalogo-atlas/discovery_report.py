import json
import pandas as pd
from atlas_client import AtlasClient

#Relatoio das entiades catalogadas no Apache Atlas

results = AtlasClient.search_entities("*")
entities = results.get('entities', [])
print(f"🔍 Entidades encontradas: {len(entities)}")
print("📊 Relatório de Entidades Catalogadas no Apache Atlas:")
for entity in entities:
    guid = entity.get('guid')
    type_name = entity.get('typeName')
    name = entity.get('attributes', {}).get('name', 'N/A')
    description = entity.get('attributes', {}).get('description', 'N/A')
    print(f"  - GUID: {guid}")
    print(f"    Tipo: {type_name}")
    print(f"    Nome: {name}")
    print(f"    Descrição: {description}\n")

#Exportar relatorio em json e csv

# Preparar dados para exportação
export_data = []
for entity in entities:
    guid = entity.get('guid')
    type_name = entity.get('typeName')
    name = entity.get('attributes', {}).get('name', 'N/A')
    description = entity.get('attributes', {}).get('description', 'N/A')
    export_data.append({
        "GUID": guid,
        "Tipo": type_name,
        "Nome": name,
        "Descrição": description
    })

# Exportar para JSON
with open("atlas_entities_report.json", "w") as json_file:
    json.dump(export_data, json_file, indent=4)
print("📁 Relatório exportado para atlas_entities_report.json")

# Exportar para CSV
df = pd.DataFrame(export_data)
df.to_csv("atlas_entities_report.csv", index=False)
print("📁 Relatório exportado para atlas_entities_report.csv")