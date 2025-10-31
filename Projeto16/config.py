import string
import random
import logging
import mysql.connector
import pandas as pd
import sys
from datetime import datetime

# Define o caminho dos módulos
path = r"/PROD/PGMS/itau-sj7-modules-datalake-conf" # Caminho do servidor
sys.path.append(path)

# Importa a função de configuração do módulo (após adicionar ao path)
from configuracao import obter_configuracao_ambiente # type: ignore

# Obtém a configuração do ambiente com base no argumento da linha de comando
ambient_conf = obter_configuracao_ambiente(sys.argv[-1])

# --- Funções de Criptografia Simples ---
# (Baseado em uma cifra de substituição simples)

# Gera a lista de caracteres base
chars = " " + string.punctuation + string.digits + string.ascii_letters
chars = list(chars)
# Cria uma chave embaralhada (key) a partir da lista base
key = chars.copy()
random.shuffle(key)

def encrypt(token: str):
    """Encripta um token usando a chave embaralhada."""
    cipher_token = ''
    for letter in token:
        index = chars.index(letter)
        cipher_token += key[index]
    return cipher_token

def decrypt(token: str):
    """Decripta um token usando a chave embaralhada."""
    cipher_token = ''
    for letter in token:
        index = key.index(letter)
        cipher_token += chars[index]
    return cipher_token

def findToken(token: str, path: str = r'/PROD/PGMS/.controlif4'): # Path padrão
    """Lê um token/senha de um arquivo de configuração."""
    try:
        with open(path, 'r') as arquivo:
            for linha in arquivo:
                if linha.startswith(f'{token}='):
                    # Retorna o valor do token (após o '=') decriptado
                    return decrypt(linha.split('=')[1].strip())
    except FileNotFoundError:
        logging.ERROR(f"Token {token} não encontrado.")
    return None

def createLogs(status: str, automation: int, df_alerts = None, alertlist:list=[]):
    """Grava os logs de execução da automação em um banco de dados MySQL."""
    # Obtém credenciais para o banco de dados
    racf = findToken('user_token_control')
    senha = findToken('pass_token_control')
    
    # Sobrescreve credenciais se estiver em ambiente AWS (lógica específica)
    if 'aws' in ambient_conf['server']:
        racf = findToken('user', ambient_conf['file_pass'])
        senha = findToken('senha', ambient_conf['file_pass'])

    current_time = datetime.now()

    try:
        # Conecta ao banco de dados
        connection = mysql.connector.connect(
            host=ambient_conf['server'],
            user=decrypt(racf),
            password=decrypt(senha),
            database=ambient_conf['db_name'],
            port=ambient_conf['port'],
            allow_local_infile=True,
            ssl_disabled=not ambient_conf['SSL'] # Habilita/desabilita SSL
        )
    except Exception as e:
        # Loga erro crítico se não conseguir conectar
        logging.critical(f"Erro ao conectar com o banco de Dados: (E)\n\nOs dados não puderam ser salvos no datalake")
        return # Sai da função

    # Define a query de inserção
    insert = "INSERT INTO TB_CTM_LOG (LogTime, AutomationID, StatusID, AlertaID, Old_Value, New_Value) VALUES (%s, %s, %s, %s, %s, %s)"
    data = [] # Lista para armazenar os dados a serem inseridos
    
    cursor = connection.cursor()
    
    # Mapeia o status (string) para um ID numérico
    statusId = {"NOF": -1, "Error": 0, "OK": 1, "NOK": 2}.get(status, 0)
    
    # Transforma dfAlerts novamente em DataFrame (Lógica parece redundante se df_alerts já é um DF)
    if len(alertlist) > 0:
        for i, alertId in enumerate(alertlist):
            if statusId == 1:
                # Se status OK, busca o status anterior do alerta no DataFrame
                currentAlert = df_alerts.loc[df_alerts['Alert ID'] == alertId]
                data.append((current_time, automation, statusId, alertId, None, None))
            else:
                # Se status não for OK, grava o status antigo e o novo
                data.append((current_time, automation, statusId, alertId, currentAlert['Status'].values[0], 'Closed' if automation == 1 else 'New'))
    else:
        # Se nenhuma lista de alertas for passada, grava um log genérico
        data.append((current_time, automation, statusId, None, None, None))

    try:
        logging.info("Inserindo informações da execução no datalake")
        # Executa a inserção em lote
        cursor.executemany(insert, data)
        connection.commit()
    except Exception as e:
        logging.critical(f"Erro ao inserir dados na tabela: (E)\n\nOs dados não puderam ser salvos no datalake")
    
    connection.close()

    # Retorna mensagens de log com base no status
    if statusId == 1:
        return logging.info("Alertas enviados e salvos no banco de dados")
    elif statusId == 2:
        return logging.info("Alertas com erro foram salvos no banco de dados")
    elif statusId == -1:
        return logging.info("Log de execução salvo no banco de dados")
    else:
        return logging.info("Erro na chamada de API, execução salva no banco de dados")
