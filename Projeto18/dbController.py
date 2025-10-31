import requests
import logging
import mysql.connector
import pandas as pd

# Importa o módulo refatorado para controle de tokens/credenciais
import tokensController as tkc

# Desabilita avisos de segurança de requisições HTTPS
requests.urllib3.disable_warnings()

def createLogs(automation:int, status:str, joblist:pd.DataFrame):
    """
    Grava os logs de execução da automação em um banco de dados MySQL.
    Esta é uma versão refatorada que usa o tokensController.
    """
    # Obtém as credenciais do banco de dados através do tokensController
    user, pas = tkc.findToken(tkc.dbpath)
    
    try:
        # Conecta ao banco de dados
        connection = mysql.connector.connect(
            host=tkc.dbServer,
            user=tkc.decrypt(user),
            password=tkc.decrypt(pas),
            database=tkc.dbName,
            port=tkc.dbPort,
            allow_local_infile=True,
            ssl_disabled=not tkc.dbSSL
        )
        cursor = connection.cursor()
        
    except Exception as e:
        # Loga erro crítico se não conseguir conectar
        logging.critical(f"Erro ao conectar com o datalake\nError: {e}")
        return

    # Define a query de inserção
    insert = "INSERT INTO TB_CTM_LOG (StatusID, AutomationID, JobName, LogTime, Incidents, AlertaID, Error_Message) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    
    # Mapeia o status (string) para um ID numérico
    statusId = {"OK": 1, "NOK": 2, "NOF": -1}.get(status, 0) # 'Error' = 0
    
    data = [] # Lista para armazenar os dados a serem inseridos
    
    if statusId > 0: # Se for OK ou NOK
        # Assume que joblist é um DataFrame com os jobs a serem logados
        job = joblist.iloc[0] # Pega a primeira linha (pode ser um bug se joblist tiver várias linhas)
        
        inc = None
        alert = None
        
        # Verifica se as colunas 'INC' e 'Alert_Id' existem no DataFrame
        if "INC" in joblist.columns:
            inc = job['INC']
        if "Alert_Id" in joblist.columns: # Nota: o nome da coluna parece diferente do script anterior
            alert = int(job['Alert_Id'])
            
        if statusId == 1: # OK
            data.append((statusId, automation, job["Job Name"], tkc.current_time, inc, alert, None))
        elif statusId == 2: # NOK
            data.append((statusId, automation, job["Job Name"], tkc.current_time, inc, alert, job["Error_Message"]))
    else:
        # Se for NOF (-1) ou Error (0), grava um log genérico
        data.append((statusId, automation, None, tkc.current_time, None, None, None))

    try:
        # Executa a inserção em lote
        cursor.executemany(insert, data)
        connection.commit()
        logging.info("Log dos jobs foram salvos")
    except Exception as e:
        logging.critical(f"Erro ao inserir dados na tabela: (E)\n\nOs dados não puderam ser salvos no datalake")
    finally:
        # Garante que a conexão seja fechada
        connection.close()
