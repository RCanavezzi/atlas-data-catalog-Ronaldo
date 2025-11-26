#!/usr/bin/env python3
"""
Script wrapper para executar spark-submit no container remoto
"""

import subprocess
import sys
import os

def main():
    """Executa spark-submit no container pyspark-aula"""
    
    # Argumentos passados para o script
    args = sys.argv[1:]
    
    # Comando docker exec
    docker_cmd = [
        'docker', 'exec', 'pyspark_aula_container',
        'spark-submit'
    ] + args
    
    print(f"🚀 Executando: {' '.join(docker_cmd)}")
    
    # Executar comando
    try:
        result = subprocess.run(docker_cmd, check=True)
        sys.exit(result.returncode)
    except subprocess.CalledProcessError as e:
        print(f"❌ Erro na execução: {e}")
        sys.exit(e.returncode)
    except Exception as e:
        print(f"❌ Erro geral: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()