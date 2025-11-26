# Apache Atlas DataOps Lab

> **Laboratório completo para aprendizado de catalogação de dados com Apache Atlas, PostgreSQL e Python**

## Sobre o Projeto

Este repositório fornece um ambiente completo de aprendizado para **Data Governance** e **DataOps** usando Apache Atlas como catálogo de dados. O projeto demonstra desde conceitos básicos até implementações avançadas de descoberta automática de metadados, linhagem de dados e integração com bancos relacionais.

### Objetivos de Aprendizado

- **Catalogação de Dados**: Criar e gerenciar catálogos de metadados
- **API Integration**: Integrar sistemas via REST APIs do Apache Atlas
- **Data Lineage**: Mapear origem e transformações de dados
- **Metadata Management**: Extrair e organizar metadados estruturais
- **DataOps Practices**: Automatizar descoberta e catalogação

## Arquitetura do Sistema

### Stack Tecnológica

| Componente | Tecnologia | Versão | Porta | Função |
|------------|------------|--------|-------|--------|
| **Catálogo** | Apache Atlas | 2.3.0 | 21000 | Governança e metadados |
| **Database** | PostgreSQL | 14.19 | 2001 | Dados de exemplo (Northwind) |
| **Orquestração** | Apache Airflow | 3.0.0 | 5000 | Workflows e ETL |
| **Analytics** | PySpark + Jupyter | Latest | 8888 | Análise e notebooks |
| **Data Lake** | Apache Iceberg | 1.4.3 | - | Armazenamento com versionamento |
| **Storage** | HBase (embedded) | - | - | Persistência Atlas |
| **Search** | Apache Solr (embedded) | - | - | Indexação e busca |
| **Messaging** | Apache Kafka (embedded) | - | - | Eventos e notificações |

## 📁 Estrutura do Repositório

```
atlas-dataops-lab/
├── docker-compose.yml          # Orquestração dos serviços
├── Dockerfile                  # Atlas customizado
├── Dockerfile_Spark           # PySpark + Jupyter + Iceberg
├── Dockerfile_AirFlow         # Apache Airflow
├── wait-for-atlas.sh          # Script de inicialização
├── users-credentials.properties # Autenticação Atlas
├── requirements.txt           # Dependências globais
├── airflow_connections.py     # Configuração de conexões
├── spark_remote_submit.py     # Wrapper Spark remoto
├── .env                       # Variáveis de ambiente
├── LICENSE                    # Licença do projeto
├── README.md                  # Este arquivo
├── .gitignore                # Arquivos ignorados
│
├── dags/                      # DAGs do Airflow
│   ├── __init__.py
│   ├── catalog_postgres_to_atlas.py # DAG de catalogação PostgreSQL
│   ├── cleanup_atlas.py       # DAG de limpeza do Atlas
│   ├── etl_northwind_to_iceberg.py # DAG ETL Spark + Iceberg
│   └── setup_spark_connection.py # DAG setup conexão Spark
│
├── spark_jobs/                # Jobs Spark
│   └── northwind_to_iceberg.py # ETL Northwind -> Iceberg
│
├── logs/                      # Logs do Airflow
│   ├── dag_processor/         # Logs de processamento
│   └── .gitkeep              # Mantém diretório no Git
│
├── plugins/                   # Plugins customizados do Airflow
│   └── __init__.py
│
├── data/                      # Datasets para análise
├── db/
│   └── northwind.sql          # Schema e dados PostgreSQL
│
├── Exercicios/
│   └── EXERCICIO_ATLAS.md     # Exercício prático completo
│
├── lab/
│   ├── atlas_client.py        # Cliente Python para Atlas API
│   ├── config.py              # Configurações do laboratório
│   ├── data_discovery.py      # Descoberta de dados
│   ├── lineage_demo.py        # Demonstração de linhagem
│   ├── postgres_integration.py # Integração PostgreSQL
│   ├── LAB_ATLAS_PYTHON.md    # Guia do laboratório Python
│   ├── requirements.txt       # Dependências Python
│   └── run_lab.sh            # Script de execução
│
├── notebooks/
│   ├── Lab_Catalogo_Postgres_no_Atlas.ipynb
│   ├── Iceberg_Demo.ipynb     # Demo Apache Iceberg
│   └── data/                  # Dados para notebooks
│
└── respostas/
    ├── config_exercicio.py    # Configurações do exercício
    ├── requirements_exercicio.txt # Dependências do exercício
    └── SOLUCAO_EXERCICIO.py   # Solução completa
```

## Início Rápido

### 1. Pré-requisitos

- **Docker** >= 20.10
- **Docker Compose** >= 2.0
- **Python** >= 3.8 (opcional, para desenvolvimento local)
- **8GB RAM** disponível (recomendado)

### 2. Inicialização

```bash
# Clonar o repositório
git clone https://github.com/AleTavares/atlas-dataops-lab.git
cd atlas-dataops-lab

# Iniciar todos os serviços
docker-compose up --build -d

# Aguardar inicialização (5-10 minutos)
./wait-for-atlas.sh

# Verificar status dos serviços
docker-compose ps
```

### 3. Acesso aos Serviços

| Serviço | URL | Credenciais |
|---------|-----|-------------|
| **Apache Atlas** | http://localhost:21000 | admin / admin |
| **Apache Airflow** | http://localhost:5000 | admin / admin |
| **Jupyter Notebook** | http://localhost:8888 | Token: tavares1234 |
| **PostgreSQL** | localhost:2001 | postgres / postgres |

## Laboratórios Disponíveis

### Lab 1: Cliente Atlas Básico
```bash
cd lab
pip install -r requirements.txt
python atlas_client.py
```
**Aprenda**: Conexão com Atlas, busca de entidades, API REST

### Lab 2: Jupyter Notebook Interativo
```bash
# Acessar: http://localhost:8888 (token: tavares1234)
# Abrir: Lab_Catalogo_Postgres_no_Atlas.ipynb
```
**Aprenda**: Extração de metadados, catalogação automática, visualização

### Lab 3: Airflow - Catalogação Automática
```bash
# Acessar: http://localhost:5000 (admin/admin)
# Executar DAG: catalog_postgres_to_atlas
```
**Aprenda**: Orquestração de workflows de catalogação

### Lab 3.1: Airflow - Limpeza do Atlas
```bash
# Acessar: http://localhost:5000 (admin/admin)
# Executar DAG: cleanup_atlas (execução manual)
```
**Aprenda**: Manutenção e limpeza de metadados

### Lab 4: Spark + Iceberg - ETL Completo
```bash
# Acessar: http://localhost:5000 (admin/admin)
# Executar DAG: etl_northwind_to_iceberg
```
**Aprenda**: ETL com Spark, Iceberg, linhagem e tags de qualidade

### Lab 5: Iceberg Demo Interativo
```bash
# Acessar: http://localhost:8888 (token: tavares1234)
# Abrir: Iceberg_Demo.ipynb
```
**Aprenda**: Apache Iceberg, time travel, versionamento

### Lab 6: Exercício Prático Completo
```bash
# Seguir instruções em EXERCICIO_ATLAS.md
```
**Aprenda**: Implementação completa de catalogador de dados

## Configurações Detalhadas

### Apache Atlas
- **Modo**: Standalone com componentes embedded
- **Storage**: BerkeleyDB para grafos, HBase para metadados
- **Search**: Apache Solr embedded
- **Messaging**: Kafka embedded para eventos
- **Autenticação**: File-based (users-credentials.properties)
- **Memória**: 1GB heap, 512MB inicial
- **Persistência**: Volume Docker `atlas_data`

### PostgreSQL Northwind
- **Database**: northwind (carregado automaticamente)
- **Tabelas**: 14 tabelas relacionais completas
  - `customers`, `products`, `orders`, `order_details`
  - `employees`, `categories`, `suppliers`, `shippers`
  - `territories`, `region`, `employee_territories`
  - `customer_demographics`, `customer_customer_demo`
- **Dados**: ~3000 registros com relacionamentos
- **Persistência**: Volume Docker `postgres_data`

### PySpark + Jupyter
- **Base Image**: jupyter/pyspark-notebook:latest
- **Packages**: requests, psycopg2-binary, pandas, matplotlib, seaborn
- **Volumes**: notebooks/ e data/ mapeados
- **Spark UI**: http://localhost:4040 (quando jobs estão rodando)

## 📋 DAGs Disponíveis

### 1. **catalog_postgres_to_atlas**
- **Descrição**: Catalogação automática do PostgreSQL Northwind no Atlas
- **Schedule**: Diário (`@daily`)
- **Tasks**: 
  - `extract_metadata` - Extrai metadados do PostgreSQL
  - `create_database` - Cria database no Atlas
  - `catalog_tables` - Cataloga estrutura das tabelas
  - `catalog_columns` - Cataloga colunas das tabelas
- **Execução**: Automática ou manual

### 2. **cleanup_atlas**
- **Descrição**: Limpeza completa de todas as entidades do Atlas
- **Schedule**: Manual apenas
- **Tasks**:
  - `get_all_entities` - Lista todas as entidades
  - `delete_columns` - Remove todas as colunas
  - `delete_tables` - Remove todas as tabelas
  - `delete_databases` - Remove todos os databases
  - `cleanup_remaining` - Limpa entidades restantes
- **⚠️ ATENÇÃO**: Remove TODAS as entidades do Atlas

### 3. **etl_northwind_to_iceberg**
- **Descrição**: ETL completo Northwind PostgreSQL para Iceberg Raw Layer
- **Schedule**: Semanal (`@weekly`)
- **Tasks**:
  - `check_spark_job` - Verifica existência do job Spark
  - `submit_spark_job` - Executa job Spark no container pyspark-aula
  - `validate_results` - Valida tabelas criadas via Atlas API
- **Funcionalidades**: Extração, catalogação, linhagem, tags de qualidade

### 4. **setup_spark_connection**
- **Descrição**: Configuração da conexão Spark no Airflow
- **Schedule**: Manual apenas
- **Tasks**:
  - `create_spark_connection` - Cria conexão spark_container
- **Uso**: Executar uma vez para configurar ambiente

## Comandos Úteis

### Gerenciamento de Serviços
```bash
# Ver logs específicos
docker-compose logs -f atlas
docker-compose logs -f postgres_erp
docker-compose logs -f airflow-standalone

# Reiniciar serviço específico
docker-compose restart atlas

# Parar todos os serviços
docker-compose down

# Limpar volumes (CUIDADO: perde dados)
docker-compose down -v

# Rebuild completo
docker-compose up --build --force-recreate
```

### Airflow - Gerenciamento de DAGs
```bash
# Listar DAGs
docker exec -it airflow-standalone airflow dags list

# Executar DAG manualmente
docker exec -it airflow-standalone airflow dags trigger catalog_postgres_to_atlas

# Ver status de execução
docker exec -it airflow-standalone airflow dags state catalog_postgres_to_atlas

# Pausar/Despausar DAG
docker exec -it airflow-standalone airflow dags pause catalog_postgres_to_atlas
docker exec -it airflow-standalone airflow dags unpause catalog_postgres_to_atlas
```

### Diagnóstico
```bash
# Testar conectividade Atlas
curl -u admin:admin http://localhost:21000/api/atlas/admin/version

# Testar PostgreSQL
docker exec -it postgres-erp psql -U postgres -d northwind -c "SELECT count(*) FROM customers;"

# Verificar recursos
docker stats
```

## Casos de Uso Educacionais

### 1. **Data Discovery**
- Descoberta automática de esquemas de banco
- Catalogação de tabelas e colunas
- Busca e navegação no catálogo

### 2. **Metadata Management**
- Extração de metadados estruturais
- Criação de entidades no Atlas
- Relacionamentos entre entidades

### 3. **Data Lineage**
- Mapeamento de origem dos dados
- Rastreamento de transformações
- Visualização de fluxos de dados

### 4. **API Integration**
- Uso de REST APIs do Atlas
- Autenticação e autorização
- Operações CRUD em metadados

### 5. **DataOps Automation**
- Scripts de catalogação automática
- Integração com pipelines CI/CD
- Monitoramento de qualidade de dados

## Próximos Passos - Roadmap

### Evolução para Plataforma DataOps Completa

Os próximos desenvolvimentos deste repositório incluirão a implementação de uma **plataforma DataOps completa** com orquestração avançada e linhagem automática de dados:

#### **Apache Airflow - Orquestração de ETLs**
- **Scheduler Avançado**: Orquestração de pipelines de dados complexos
- **DAGs Automatizados**: Workflows para descoberta e catalogação contínua
- **Monitoramento**: Interface web para acompanhamento de execuções
- **Integração Atlas**: DAGs específicos para sincronização de metadados

#### **Apache Spark - Engine de Transformação**
- **Processamento Distribuído**: Transformações em larga escala
- **Conectores Nativos**: Integração direta com PostgreSQL e Atlas
- **Spark Streaming**: Processamento de dados em tempo real
- **Delta Lake**: Versionamento e qualidade de dados

#### **Linhagem Automática de Dados**
- **Rastreamento Completo**: Origem → Transformação → Destino
- **Spark Lineage**: Captura automática via Spark Listener
- **Atlas Integration**: Registro automático de processos ETL
- **Visualização Gráfica**: Mapeamento visual de fluxos de dados

### **Arquitetura Futura**

### **Funcionalidades Implementadas e Planejadas**

| Componente | Funcionalidade | Status |
|------------|----------------|--------|
| **Airflow** | DAGs de catalogação automática | ✅ **Implementado** |
| **Airflow** | DAG de limpeza do Atlas | ✅ **Implementado** |
| **Airflow** | DAG ETL Spark + Iceberg | ✅ **Implementado** |
| **Atlas** | Catalogação via API REST | ✅ **Implementado** |
| **Atlas** | Linhagem automática de dados | ✅ **Implementado** |
| **Atlas** | Tags de qualidade automatizadas | ✅ **Implementado** |
| **PostgreSQL** | Extração de metadados Northwind | ✅ **Implementado** |
| **Spark** | Jobs ETL com Iceberg | ✅ **Implementado** |
| **Iceberg** | Armazenamento com versionamento | ✅ **Implementado** |
| **Monitoring** | Dashboard de qualidade de dados | 📋 Planejado |
| **Governance** | Políticas avançadas | 📋 Planejado |

### **Benefícios da Evolução**

- **Automação Completa**: Descoberta e catalogação sem intervenção manual
- **Linhagem End-to-End**: Rastreamento completo do ciclo de vida dos dados
- **Escalabilidade**: Processamento distribuído para grandes volumes
- **Governança Avançada**: Políticas e qualidade automatizadas
- **Observabilidade**: Monitoramento completo de pipelines

### **Como Contribuir**

Interessado em contribuir com essas funcionalidades? Áreas de desenvolvimento:

- **Airflow DAGs**: Desenvolvimento de workflows de catalogação
- **Spark Jobs**: Implementação de ETLs com captura de linhagem
- **Atlas Hooks**: Conectores customizados para diferentes fontes
- **Monitoring**: Dashboards e alertas de qualidade
- **Documentation**: Tutoriais e guias avançados

## Contribuição

Este é um projeto educacional. Contribuições são bem-vindas:

1. **Fork** o repositório
2. **Crie** uma branch para sua feature
3. **Commit** suas mudanças
4. **Push** para a branch
5. **Abra** um Pull Request

### Áreas de Melhoria Atuais
- Novos conectores de dados
- Exemplos de classificação automática
- Integração com ferramentas de BI
- Testes automatizados
- Documentação adicional

## Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para detalhes.

## Agradecimentos

- **Apache Atlas Community** - Pela excelente ferramenta de governança
- **Northwind Database** - Pelo dataset educacional clássico
- **Docker Community** - Pela containerização simplificada
- **Jupyter Project** - Pelo ambiente interativo de análise

**📚 Para começar, acesse os laboratórios em ordem:**
1. [Lab Python Básico](lab/LAB_ATLAS_PYTHON.md)
2. [Exercício Prático](Exercicios/EXERCICIO_ATLAS.md)
3. [Notebook Interativo](notebooks/Lab_Catalogo_Postgres_no_Atlas.ipynb)