import requests
import time
import mysql.connector
from datetime import datetime
import os
import urllib3
import glob
import sys

sys.path.append('/PROD/PGMS/itau-sj7-modules-datalake-conf')

from configuracao import obter_configuracao_ambiente, ler_arquivo_configuracao # Adicionado ler_arquivo_configuracao
from ger_relatorio import gerar_relatorio
from ger_token import gerar_token

if len(sys.argv) != 2:
    print("Exemplo: python main.py PRD") 
    sys.exit(1) 
  
amb = sys.argv[1].upper()

try:
    ambiente = amb
    config = obter_configuracao_ambiente(ambiente)
    print(f"{datetime.now().strftime('%d-%m-%y %H:%M:%S')} Configurações para o ambiente {ambiente}")
except Exception as e: # Captura exceção genérica
    print(f"Erro ao obter configuração do ambiente: {e}")
    sys.exit(1) 

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

JOBNAME = "00000" 
ARQUIVO = f"/0000/{JOBNAME}_LISTA"
REPORT_NAME = "Alerts_Datalake"
directory = "/PROD/0000"
TABLE = "TB_ALARM"

VAR1, VAR2 = ler_arquivo_configuracao(config["file_pass"]) 
caminho_arquivo = '/PROD/PGMS/.prf_ira'
VAR3, VAR4 = ler_arquivo_configuracao(caminho_arquivo) # Assume que existe a mesma função

token_user = gerar_token(config["ENDPOINTCTM"], VAR3, VAR4)

gerar_relatorio(
    endpoint=config["ENDPOINTCTM"],
    nome_relatorio=REPORT_NAME,
    token=token_user,
    arquivo_saida=ARQUIVO
)

def limpar_aspas_do_arquivo(arquivo_entrada, arquivo_saida):
    """Remove todas as aspas duplas de um arquivo."""
    try:
        # Abre o arquivo de entrada para leitura ('r') e o de saída para escrita ('w')
        with open(arquivo_entrada, 'r') as entrada, open(arquivo_saida, 'w') as saida:
            for linha in entrada:
                # Substitui todas as ocorrências de '"' por nada (remove as aspas)
                linha_limpa = linha.replace('"', '')
                saida.write(linha_limpa)
        # Log de sucesso
        print(f"{datetime.now().strftime('%d-%m-%y %H:%M:%S')} Arquivo limpo salvo em: {arquivo_saida}")
    except Exception as e:
        # Log de erro
        print(f"Erro ao limpar aspas do arquivo: {e}")
        raise # Re-levanta a exceção para parar o script se a limpeza falhar

# Exemplo de uso da função de limpeza
ARQUIVO_LIMPO = f"{ARQUIVO}_CLEAN" # Define o nome do arquivo limpo
try:
    limpar_aspas_do_arquivo(ARQUIVO, ARQUIVO_LIMPO)
except Exception:
    sys.exit(1) 

def carregar_arquivo_no_banco(arquivo, VAR1_user, VAR2_pass, db_config):
    print(f"{datetime.now().strftime('%d-%m-%y %H:%M:%S')} Iniciando carga do arquivo {arquivo}")
    connection = None 
    cursor = None 
    try:
        connection = mysql.connector.connect(
            host=db_config["server"], # Obtém o host do objeto config
            user=VAR1,                # Usa a variável VAR1 como usuário
            password=VAR2,            # Usa a variável VAR2 como senha
            database=db_config["db_name"], # Obtém o nome do banco de dados do objeto config
            allow_local_infile=True,
            ssl_disable=not config["SSL"}
        )
        cursor = connection.cursor()
        cursor.execute(f"""
            LOAD DATA LOCAL INFILE '{arquivo}'
            REPLACE
            INTO TABLE {TABLE}
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES 
            (@campo0, @campo1, @campo2, @campo3, @campo4, @campo5, @campo6, @campo7, @campo8, @campo9, @campo10, @campo11, @campo12, @campo13, @campo14, @campo15, @campo16, @campo17, @campo18)
            SET
                SERIAL = NULLIF(@campo0, ''), -- Ajuste os nomes das colunas da sua tabela TB_ALARM
                ALERT_KEY = NULLIF(@campo1, ''),
                NODE = NULLIF(@campo2, ''),
                NODE_ALIAS = NULLIF(@campo3, ''),
                AGENT = NULLIF(@campo4, ''),
                ALERT_GROUP = NULLIF(@campo5, ''),
                X7 = NULLIF(@campo6, ''),
                APPLICATION = NULLIF(@campo7, ''),
                MANAGER = NULLIF(@campo8, ''),
                SERVER = NULLIF(@campo9, ''),
                SEVERITY = NULLIF(@campo10, ''),
                SUMMARY = NULLIF(@campo11, ''),
                FIRST_OCCURRENCE = STR_TO_DATE(NULLIF(@campo12, ''), '%d-%m-%y %H:%i:%s'), -- Ajuste o formato da data/hora
                LAST_OCCURRENCE = STR_TO_DATE(NULLIF(@campo13, ''), '%d-%m-%y %H:%i:%s'),
                INTERNAL_LAST = STR_TO_DATE(NULLIF(@campo14, ''), '%d-%m-%y %H:%i:%s'),
                POLL = NULLIF(@campo15, ''),
                TALLY = NULLIF(@campo16, ''),
                EXPIRE_TIME = NULLIF(@campo17, ''),
                OWNER_UID = NULLIF(@campo18, '')
                # Adicione ou remova colunas conforme necessário para sua tabela TB_ALARM
                # Certifique-se que o número de @campos corresponde às colunas no seu CSV
        """
        connection.commit()     
    except mysql.connector.Error as err:
        print(f"Erro MySQL ao carregar arquivo no banco: {err}")
        if connection:
            connection.rollback() # Desfaz a transação em caso de erro
        raise # Re-levanta a exceção para sinalizar falha na carga
    except Exception as e:
        print(f"Erro inesperado ao carregar arquivo no banco: {e}")
        if connection:
            connection.rollback()
        raise # Re-levanta a exceção
    finally:
        if cursor:
            cursor.close()
            print(f"{datetime.now().strftime('%d-%m-%y %H:%M:%S')} Cursor fechado.")
        if connection and connection.is_connected():
            connection.close()
            print(f"{datetime.now().strftime('%d-%m-%y %H:%M:%S')} Conexão com o banco fechada.")
    # Retorna True se a carga foi bem sucedida, False caso contrário
    return load_success

carregar_arquivo_no_banco(f"{ARQUIVO}_CLEAN", VAR1, VAR)
