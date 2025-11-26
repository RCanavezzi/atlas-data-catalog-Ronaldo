# main.py
from atlas_client import AtlasClient
from config import ATLAS_CONFIG, POSTGRES_CONFIG
from postgres_extractor import PostgreSQLExtractor
from data_catalogger import DataCatalogger
from discovery_report import DiscoveryReport

def main():
    # Inicializar componentes
    atlas = AtlasClient(**ATLAS_CONFIG)
    extractor = PostgreSQLExtractor(**POSTGRES_CONFIG)
    catalogger = DataCatalogger(atlas, extractor)
    
    # Catalogar dados
    print("Iniciando catalogação...")
    results = catalogger.catalog_all_tables()
    print(f"{results['tables_created']} tabelas catalogadas")
    
    # Gerar relatório
    report = DiscoveryReport(atlas)
    report.generate_report("discovery_report")
    print("Relatório gerado!")

if __name__ == "__main__":
    main()