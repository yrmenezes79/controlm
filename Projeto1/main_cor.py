import requests
import time
import mysql.connector
from datetime import datetime
import os
import urllib3
import glob
import sys

# Adiciona o caminho para os módulos customizados
sys.path.append('/PROD/PGMS/itau-sj7-modules-datalake-conf')

try:
    # Importa as funções customizadas
    from configuracao import obter_configuracao_ambiente, ler_arquivo_configuracao
    from ger_relatorio import gerar_relatorio
    from ger_token import gerar_token
except ImportError as e:
    print(f"Erro ao importar módulos customizados: {e}")
    print("Verifique se o caminho em sys.path.append está correto e se os módulos existem.")
    sys.exit(1)


if len(sys.argv) != 2:
    # A indentação aqui estava correta, mas é bom manter o padrão de 4 espaços
    print("Exemplo: python main.py PRD")
    sys.exit(1)

amb = sys.argv[1].upper()

try:
    # A indentação aqui também estava correta
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

try:
    # Carrega as credenciais do banco
    VAR1, VAR2 = ler_arquivo_configuracao(config["file_pass"])
    
    # Carrega as credenciais para o token
    caminho_arquivo = '/PROD/PGMS/.prf_ira'
    VAR3, VAR4 = ler_arquivo_configuracao(caminho_arquivo) # Assume que existe a mesma função

    # Gera o token
    token_user = gerar_token(config["ENDPOINTCTM"], VAR3, VAR4)

    # Gera o relatório
    gerar_relatorio(
        endpoint=config["ENDPOINTCTM"],
        nome_relatorio=REPORT_NAME,
        token=token_user,
        arquivo_saida=ARQUIVO
    )

except KeyError as e:
    print(f"Erro: Chave de configuração não encontrada: {e}")
    sys.exit(1)
except FileNotFoundError as e:
    print(f"Erro: Arquivo de configuração não encontrado: {e}")
    sys.exit(1)
except Exception as e:
    print(f"Erro ao gerar token ou relatório: {e}")
    sys.exit(1)


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
    """Carrega o arquivo limpo para o banco de dados MySQL."""
    print(f"{datetime.now().strftime('%d-%m-%y %H:%M:%S')} Iniciando carga do arquivo {arquivo}")
    connection = None
    cursor = None
    # CORREÇÃO: Inicializa a variável load_success
    load_success = False 
    try:
        connection = mysql.connector.connect(
            host=db_config["server"], # Obtém o host do objeto config
            user=VAR1_user,           # CORREÇÃO: Usa o parâmetro da função
            password=VAR2_pass,       # CORREÇÃO: Usa o parâmetro da função
            database=db_config["db_name"], # Obtém o nome do banco de dados do objeto config
            allow_local_infile=True,
            # CORREÇÃO: Faltava um '}' no final de config["SSL"]
            ssl_disable=not db_config.get("SSL", False) # Usa .get para ser mais seguro
        )
        cursor = connection.cursor()
        
        # A query SQL (string multi-linha) em si não tem problemas de indentação Python
        query_load = f"""
            LOAD DATA LOCAL INFILE '{arquivo}'
            REPLACE
            INTO TABLE {TABLE}
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (@campo0, @campo1, @campo2, @campo3, @campo4, @campo5, @campo6, @campo7, @campo8, @campo9, @campo10, @campo11, @campo12, @campo13, @campo14, @campo15, @campo16, @campo17, @campo18)
            SET
                SERIAL = NULLIF(@campo0, ''),
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
                FIRST_OCCURRENCE = STR_TO_DATE(NULLIF(@campo12, ''), '%d-%m-%y %H:%i:%s'),
                LAST_OCCURRENCE = STR_TO_DATE(NULLIF(@campo13, ''), '%d-%m-%y %H:%i:%s'),
                INTERNAL_LAST = STR_TO_DATE(NULLIF(@campo14, ''), '%d-%m-%y %H:%i:%s'),
                POLL = NULLIF(@campo15, ''),
                TALLY = NULLIF(@campo16, ''),
                EXPIRE_TIME = NULLIF(@campo17, ''),
                OWNER_UID = NULLIF(@campo18, '')
        """
        cursor.execute(query_load)
        connection.commit()
        # CORREÇÃO: Define sucesso como True após o commit
        load_success = True
        print(f"{datetime.now().strftime('%d-%m-%y %H:%M:%S')} Carga do arquivo {arquivo} concluída com sucesso.")
        
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

# --- CORREÇÃO DE INDENTAÇÃO ---
# Esta linha estava indentada, fazendo parte da função 'carregar_arquivo_no_banco'.
# Ela deve ficar no nível principal do script (sem indentação).
try:
    # CORREÇÃO:
    # 1. A variável era 'VAR' e provavelmente deveria ser 'VAR2'.
    # 2. A função esperava 4 argumentos, faltava o 'config'.
    # 3. Os parâmetros da função são VAR1_user e VAR2_pass, então passamos VAR1 e VAR2.
    sucesso = carregar_arquivo_no_banco(ARQUIVO_LIMPO, VAR1, VAR2, config)
    
    if sucesso:
        print(f"{datetime.now().strftime('%d-%m-%y %H:%M:%S')} Processo finalizado com sucesso.")
    else:
        # A função já levanta uma exceção em caso de falha,
        # mas adicionamos um else por clareza.
        print(f"{datetime.now().strftime('%d-%m-%y %H:%M:%S')} Processo finalizado com falha (ver logs).")
        sys.exit(1)
        
except Exception as e:
    print(f"{datetime.now().strftime('%d-%m-%y %H:%M:%S')} Falha crítica na carga do banco: {e}")
    sys.exit(1)
