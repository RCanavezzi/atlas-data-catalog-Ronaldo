#!/usr/bin/env python3
"""
Cliente Python para Apache Atlas
Demonstra conexão e operações básicas com a API REST
"""

import requests
import json
from requests.auth import HTTPBasicAuth

class AtlasClient:
    def __init__(self, url="http://localhost:21000", username="admin", password="admin"):
        self.url = url
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({'Content-Type': 'application/json'})
    
    def get_version(self):
        """Obter versão do Atlas"""
        response = self.session.get(f"{self.url}/api/atlas/admin/version")
        return response.json()
    
    def get_types(self):
        """Listar todos os tipos disponíveis"""
        response = self.session.get(f"{self.url}/api/atlas/v2/types/typedefs")
        return response.json()
    
    def search_entities(self, query="*", limit=10):
        """Buscar entidades por termo"""
        params = {"query": query, "limit": limit}
        response = self.session.get(f"{self.url}/api/atlas/v2/search/basic", params=params)
        return response.json()
    
    def create_entity(self, entity_data):
        """Criar nova entidade"""
        response = self.session.post(
            f"{self.url}/api/atlas/v2/entity",
            json={"entity": entity_data}
        )
        return response.json()
    
    def get_entity(self, guid):
        """Obter entidade por GUID"""
        response = self.session.get(f"{self.url}/api/atlas/v2/entity/guid/{guid}")
        return response.json()
    
    def get_lineage(self, guid, direction="BOTH", depth=3):
        """Obter linhagem de uma entidade"""
        params = {"direction": direction, "depth": depth}
        response = self.session.get(f"{self.url}/api/atlas/v2/lineage/{guid}", params=params)
        return response.json()