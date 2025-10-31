import requests
import logging
import random
import string
import sys
import pandas as pd
import time
from io import StringIO
import urllib3
from datetime import datetime
import mysql.connector

# --- Configuração Inicial ---
# Obtém o ambiente da linha de comando (ex: PRD)
ambient = sys.argv[1]
logging.basicConfig(level=logging.INFO) # Configuração inicial de logging

# Adiciona o caminho dos módulos de configuração
sys.path.append(r'/PROD/PGMS/itau-sj7-modules-datalake-conf')

# Importações de módulos locais
from ger_relatorio import gera_relatorio # type: ignore
from configuracao import obter_configuracao_ambiente # type: ignore
from ger_token import gerar_token, finaliza_token # type: ignore

# Obtém a configuração do ambiente
ambient_conf = obter_configuracao_ambiente(ambient)

# Define os endpoints das APIs
endpoint = ambient_conf['ENDPOINTCTM']
snowEndpoint = ambient_conf['ENDPOINTNOW']


# --- Validação de Argumentos ---
if len(sys.argv) < 3:
    logging.error("Erro: O segundo argumento (limite) é obrigatório.")
    sys.exit(1)

if not sys.argv[2].isdigit():
    logging.error("Erro: O segundo argumento deve ser um número inteiro.")
    sys.exit(1)

# --- Configuração de Logging e HTTPS ---
logging.basicConfig(format='%(levelname)s: %(asctime)s - %(message)s\n', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
# logging.basicConfig(level=logging.INFO) # Linha duplicada da imagem
urllib3.exceptions.ConnectTimeoutError
requests.urllib3.disable_warnings() # Desabilita avisos de HTTPS

current_time = datetime.now()

# --- Funções de Criptografia Simples ---
chars = " " + string.punctuation + string.digits + string.ascii_letters
chars = list(chars)
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

def findToken(token: str, path: str = r'/PROD/PGMS/.controlif4'):
    """Lê um token/senha de um arquivo de configuração."""
    try:
        with open(path, 'r') as arquivo:
            for linha in arquivo:
                if linha.startswith(f'{token}='):
                    return decrypt(linha.split('=')[1].strip())
    except FileNotFoundError:
        print(f"Token {token} não encontrado.") # Log melhorado: logging.error(...)
    return None

# --- Obtenção de Credenciais ---
racf = findToken('user_token_control')
senha = findToken('pass_token_control')
snow_token = findToken('snow_token')
authUser = findToken('authorizationuser')
host = findToken('host')

# --- Configuração de Ambiente (Dev/Prd) ---
if ambient == "PRD":
    eventhubUrl = "https://integracao-eventos-vl-iueventhub.api-sp.prod.aws.cloud.ihf/integracao-eventos/v1/controlms"
    authToken = findToken('authorizationtoken_prd')
else:
    eventhubUrl = "https://integracao-eventos-vl-iueventhub.api-sp.dev.aws.cloud.ihf/integracao-eventos/v1/controlms"
    authToken = findToken('authorizationtoken_dev')

def findIncident(Alerts: pd.DataFrame):
    """Verifica incidentes no ServiceNow para uma lista de alertas."""
    headers = {
        "Authorization": f"Basic {decrypt(snow_token)}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    notFoundAlerts = Alerts.copy()
    # Adiciona uma coluna 'Error' para marcar alertas com timeout ou não encontrados
    notFoundAlerts.loc[notFoundAlerts.iloc[:0].index, 'Error'] = ""
    logging.info("Validando Alertas na Service-Now")

    # Calcula a data de 2 dias atrás
    last2days = current_time - datetime.timedelta(days=2)
    last2days = last2days.strftime('%Y-%m-%d')

    for index, alert in Alerts.iterrows():
        alertCreated = alert['Time']
        
        # Ignora alertas criados há mais de 2 dias (lógica parece ser 'se for mais antigo que 2 dias atrás')
        if int(alertCreated[:8]) < int(last2days): # Esta lógica compara 'YYYYMMDD' como inteiros
            continue
            
        inc = None
        # Tenta extrair um número de incidente (INC) das notas
        if not pd.isna(alert['Notes']):
            if 'INC' in alert['Notes'].upper():
                note = alert['Notes']
                if 'INC' in note:
                    inc = note[note.index('INC'):]

        # Converte o timestamp do alerta
        alert_time = datetime.strptime(alert['Time'], '%Y%m%d%H%M%S') # Formato: YYYYMMDDHHMMSS
        alert_time = alert_time + datetime.timedelta(hours=-2) # Ajuste de fuso
        date = alert_time.strftime('%Y-%m-%d')
        hour = alert_time.strftime('%H:%M:%S')

        # Monta a query para o ServiceNow
        if inc is None:
            params = {
                "sysparm_query": f"sys_created_by=system^sys_created_on>=gs.dateGenerate('{date}','{hour}')^u_source=ControlM^category=monitoring^u_datacenter={alert['Control-M Server Name']}^u_alert_idSTARTSWITH{alert['Alert ID']}^u_orderid={alert['Order ID']}",
                "sysparm_limit": 1
            }
        else:
            params = {
                "sysparm_query": f"sys_created_by=system^sys_created_on>=gs.dateGenerate('{date}','{hour}')^u_source=ControlM^category=monitoring^number={inc}^u_datacenter={alert['Control-M Server Name']}^u_alert_idSTARTSWITH{alert['Alert ID']}^u_orderid={alert['Order ID']}",
                "sysparm_limit": 1
            }

        try:
            # Faz a requisição GET para o ServiceNow
            response = requests.get(f"{snowEndpoint}/now/table/incident", headers=headers,
                                    verify=False, params=params, timeout=None)
            response.raise_for_status()
            result = response.json()['result']

            # Se o incidente NÃO for encontrado (lista vazia), marca-o
            if response.status_code == 200 and len(result) == 0:
                logging.error(f"Incident Not Found for alert: {alert['Alert ID']}")
                copy = alert.copy()
                copy['Error'] = 'Not Found'
                notFoundAlerts = pd.concat([notFoundAlerts, pd.DataFrame([copy])], ignore_index=True)

        except requests.exceptions.Timeout:
            # Se der timeout, marca-o
            logging.error(f"Timeout occurred for alert: {alert['Alert ID']}")
            copy = alert.copy()
            copy['Error'] = 'Timeout'
            notFoundAlerts = pd.concat([notFoundAlerts, pd.DataFrame([copy])], ignore_index=True)
        except Exception as e:
            # Captura outros erros de requisição
            logging.error(f"Erro na execução da busca por incidente (Alerta: {alert['Alert ID']}): {e}")
            copy = alert.copy()
            copy['Error'] = 'Request Error'
            notFoundAlerts = pd.concat([notFoundAlerts, pd.DataFrame([copy])], ignore_index=True)


    logging.info(f"Lista de alertas inicial finalizado ao todo foram \nEncontrados: {len(Alerts)-len(notFoundAlerts)} \nNão Encontrados: {len(notFoundAlerts)}")

    # --- Lógica de Retentativa (Busca 2) ---
    if len(notFoundAlerts) > 0:
        logging.info("Rebuscando incidentes não encontrados")
        times = 0
        while True:
            logging.info(f"Busca numero: {times+1}")
            logging.info(f"Existem {len(notFoundAlerts)} alertas para serem validados")
            time.sleep(60) # Espera 1 minuto
            
            toDrop = [] # Lista de índices de alertas a remover dos "não encontrados"

            for index, alert in notFoundAlerts.iterrows():
                alert_time = datetime.strptime(alert['Time'], '%Y%m%d%H%M%S')
                date = alert_time.strftime('%Y-%m-%d')
                hour = alert_time.strftime('%H:%M:%S')

                params = {
                    "sysparm_query": f"sys_created_by=system^sys_created_on>=gs.dateGenerate('{date}','{hour}')^u_source=ControlM^category=monitoring^u_datacenter={alert['Control-M Server Name']}^u_alert_idSTARTSWITH{alert['Alert ID']}^u_orderid={alert['Order ID']}",
                    "sysparm_limit": 1
                }
                
                try:
                    response = requests.get(f"{snowEndpoint}/now/table/incident", headers=headers,
                                            verify=False, params=params, timeout=None)
                    response.raise_for_status()
                    results = response.json()['result']
                    
                    # Se agora for encontrado, marca para remoção
                    if response.status_code == 200 and len(results) != 0:
                        toDrop.append(index)
                
                except Exception as e:
                    logging.error(f"Erro na execução (Busca 2): {e}")
            
            # Remove os alertas que foram encontrados nesta busca
            if len(toDrop) != 0:
                notFoundAlerts = notFoundAlerts.drop(toDrop).reset_index(drop=True)

            times += 1
            if times == 4: # Tenta 4 vezes
                break
        
        logging.info("Busca de 5 minutos concluída, validando casos de abertura externa")

        # --- Lógica de Retentativa (Busca 3 - Casos Externos) ---
        if len(notFoundAlerts) > 0:
            for index, alert in notFoundAlerts.iterrows():
                alert_time = datetime.strptime(alert['Time'], '%Y%m%d%H%M%S')
                date = alert_time.strftime('%Y-%m-%d')
                hour = alert_time.strftime('%H:%M:%S')

                # (A query para a Busca 3 parece ser a mesma da Busca 2 nas imagens)
                params = {
                    "sysparm_query": f"sys_created_by=system^sys_created_on>=gs.dateGenerate('{date}','{hour}')^u_source=ControlM^category=monitoring^u_datacenter={alert['Control-M Server Name']}^u_alert_idSTARTSWITH{alert['Alert ID']}^u_orderid={alert['Order ID']}",
                    "sysparm_limit": 1
                }
                
                try:
                    response = requests.get(f"{snowEndpoint}/now/table/incident", headers=headers,
                                            verify=False, params=params, timeout=None)
                    response.raise_for_status()
                    results = response.json()['result']
                    
                    if response.status_code == 200 and len(results) != 0:
                        toDrop.append(index) # Marca para remoção
                
                except Exception as e:
                    logging.error(f"Erro na execução (Busca 3): {e}")

            # Remove os alertas encontrados na Busca 3
            if len(toDrop) != 0:
                notFoundAlerts = notFoundAlerts.drop(toDrop).reset_index(drop=True)

    logging.info(f"Lista de alertas finalizada ao todo foram \nEncontrados: {len(Alerts)-len(notFoundAlerts)} \nNão Encontrados: {len(notFoundAlerts)}")
    
    return notFoundAlerts

def mergeColumns(dfCancelNoAlert:pd.DataFrame, notFound:pd.DataFrame, dfJobs:pd.DataFrame):
    """Mescla DataFrames de cancelamento e alertas não encontrados."""
    columns = ['JobName', 'ServerName', 'Run ID', 'Time', 'Start Time', 'End Time', 'Rerun Counter']
    toAdjust = pd.DataFrame(columns=columns)
    
    if len(dfCancelNoAlert) > 0:
        for index, row in dfCancelNoAlert.iterrows():
            # Cria uma nova linha padronizada a partir de 'dfCancelNoAlert'
            new_row = [row['name'], row['ctm'], row['orderid'], row['runTime'], row['startTime'], row['endTime'], row['Number of Runs']]
            toAdjust.loc[len(toAdjust)] = new_row
    
    if len(notFound) > 0:
        for index, row in notFound.iterrows():
            # Busca o job correspondente no DataFrame 'dfJobs'
            job = dfJobs.loc[dfJobs['name'] == row['Job Name']]
            
            if len(job) == 0:
                endtime = row['Time']
                starttime = row['Time']
            else:
                endtime = job['EndTime'].values[0]
                starttime = job['startTime'].values[0]
            
            # Cria uma nova linha padronizada a partir de 'notFound'
            new_row = [row['Job Name'], row['Control-M Server Name'], row['Order ID'], row['Time'], starttime, endtime, row['Run Counter']]
            toAdjust.loc[len(toAdjust)] = new_row
            
    return toAdjust

def checkJobsNotFound(noAlerts: pd.DataFrame):
    """Verifica jobs sem alertas, procurando por eventos ou incidentes já abertos."""
    
    # Headers para o ServiceNow
    headers = {
        "Authorization": f"Basic {decrypt(snow_token)}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    def fetch_results(endpoint, params):
        """Função auxiliar aninhada para fazer requisições GET."""
        try:
            response = requests.get(endpoint, headers=headers, params=params, verify=False, timeout=30)
            if response.status_code == 200:
                return response.json().get('result', [])
            else:
                logging.error(f"Erro na chamada de API: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            logging.error(f"Erro ao acessar a API: {e}")
            return []

    if noAlerts.empty:
        return noAlerts # Retorna o DataFrame vazio

    FoundAlerts = pd.DataFrame()
    for index, row in noAlerts.iterrows():
        # Query 1: Busca por eventos de auditoria de cancelamento
        params = {
            "sysparm_query": f"metric_name=Auditoria - Cancelamento de Rotina^u_source=EventHub-AWS^resource={row['ctm']}-{row['Number of Runs']}^{row['orderid']}^u_node={row['name']}",
            "sysparm_limit": 1
        }
        results = fetch_results(f"{snowEndpoint}/now/table/em_event", params)
        if results:
            FoundAlerts = pd.concat([FoundAlerts, pd.DataFrame([row])], ignore_index=True)

    if not FoundAlerts.empty:
        # Remove dos 'noAlerts' os que foram encontrados
        noAlerts = noAlerts[~noAlerts['name'].isin(FoundAlerts['name'])].tolist() # CUIDADO: .tolist() pode não ser o esperado aqui

    FoundAlerts = pd.DataFrame() # Reseta o DataFrame
    for index, job in noAlerts.iterrows():
        try:
            # Converte timestamps
            job_time = datetime.strptime(job['EndTime'], '%Y%m%d%H%M%S') - datetime.timedelta(hours=2)
            
            # Query 2: Busca por incidentes criados pelo sistema
            params = {
                "sysparm_query": f"sys_created_by=system^sys_created_on>=gs.dateGenerate('{job_time.strftime('%Y-%m-%d')}','{job_time.strftime('%H:%M:%S')}')^u_source=ControlM^category=monitoring^u_datacenter={job['ctm']}^u_orderid={job['orderid']}",
                "sysparm_limit": 1
            }
            # ERRO PROVÁVEL: O endpoint /em_event parece incorreto para buscar incidentes. Deveria ser /incident?
            results = fetch_results(f"{snowEndpoint}/now/table/em_event", params) 
            if results:
                FoundAlerts = pd.concat([FoundAlerts, pd.DataFrame([job])], ignore_index=True)
        except Exception as e:
            logging.error(f"Erro ao validar incidentes: {e}")

    if not FoundAlerts.empty:
        # Remove dos 'noAlerts' os que foram encontrados
        noAlerts = noAlerts[~noAlerts['name'].isin(FoundAlerts['name'])].tolist() # CUIDADO: .tolist()

    return noAlerts

def createLogs(status: str, incident: pd.DataFrame = None, incNumber: str = None):
    """Grava logs de execução no banco de dados MySQL."""
    db_user = findToken('user', ambient_conf['file_pass'])
    db_pass = findToken('senha', ambient_conf['file_pass'])
    
    try:
        # Conecta ao banco de dados
        connection = mysql.connector.connect(
            host=ambient_conf['server'],
            user=decrypt(db_user),
            password=decrypt(db_pass),
            database=ambient_conf['db_name'],
            port=ambient_conf['port'],
            allow_local_infile=True,
            ssl_disabled=not ambient_conf['SSL']
        )
        cursor = connection.cursor()
    except Exception as e:
        logging.critical(f"Erro ao conectar com o datalake\nError: {e}")
        return

    # Define a query de inserção
    insert = "INSERT INTO TB_CTM_LOG (LogTime, AutomationID, StatusID, JobName, Incidents, Old_Value, New_Value, Error_Message) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    data = []
    
    # Mapeia o status (string) para um ID numérico
    statusId = {"OK": 1, "NOK": 2, "NOF": -1}.get(status, 0) # 'Error' = 0

    if statusId == 1:
        data.append((current_time, 5, statusId, incident["JobName"], incNumber, 'Event Opened', 'None'))
    elif statusId == 2:
        data.append((current_time, 5, statusId, incident["JobName"], incNumber, None, None, incident['Error']))
    else: # NOF ou Error
        data.append((current_time, 5, statusId, None, None, None, None))

    try:
        cursor.executemany(insert, data)
        connection.commit()
        logging.info("Log dos jobs foram salvos")
    except Exception as e:
        logging.critical(f"Erro ao inserir dados na tabela: (E)\n\nOs dados não puderam ser salvos no datalake")
    finally:
        connection.close()

def retrieveInc(alert:pd.DataFrame):
    """Busca um incidente específico no ServiceNow."""
    headers = {
        "Authorization": f"Basic {decrypt(snow_token)}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    params = {
        "sysparm_query": f"u_alert_idSTARTSWITH^u_source=ControlM^category=monitoring^u_datacenter={alert['ServerName']}^u_orderid={alert['JobName']}",
        "sysparm_first_row": '1', # Busca apenas o primeiro
    }
    time.sleep(10) # Espera 10 segundos
    response = requests.get(f"{snowEndpoint}/now/table/incident", headers=headers,
                            verify=False, params=params, timeout=None)
    response.raise_for_status()
    result = response.json()['result']
    
    if response.status_code == 200 and len(result) > 0:
        inc = result[0]['number'] # Pega o número do incidente
        return inc
    else:
        return None

def openIncident(toOpen: pd.DataFrame):
    """Cria novos incidentes enviando dados para o EventHub."""
    headers = {
        "Content-Type": "application/json",
        "authorizationuser": f"{decrypt(authUser)}",
        "authorizationtoken": f"{decrypt(authToken)}"
    }

    for index, row in toOpen.iterrows():
        try:
            job_member_name = row["JobName"]
            run_time_formatted = datetime.strptime(row['Run Time'], '%Y%m%d%H%M%S').strftime('%d/%m/%Y %H:%M:%S')
            
            # Monta o payload JSON complexo
            json_data = {
                "node": job_member_name,
                "resource": f"{row['ServerName']}-{row['Rerun Counter']}-{row['Run ID']}",
                "metric_name": "Auditoria - Cancelamento de Rotina",
                "severity": 3,
                "description": f"Cancelamento da rotina {job_member_name} referente ao {run_time_formatted}",
                "messageKey": row['Run ID'],
                "additional_info": {
                    "monitoring_tool": "ControlM",
                    "kb": "KB0951709",
                    "group": "",
                    "kb01": "",
                    "kb02": "",
                    "kb03": "",
                    "alert_group": "I",
                    "assignment_group_incident": "CHAPOLIN (S002112)",
                    "fechamento_automatico": "False",
                    "CONTROLMSERVER_ADDINFO": row['ServerName'],
                    "ORDERDATE_ADDINFO": datetime.strptime(row['Run Time'], '%Y%m%d%H%M%S').strftime('%d/%m/%Y %H:%M:%S'),
                    "ORDERID_ADDINFO": row['Run ID'],
                    "RUN_NUMBER": str(row['Rerun Counter']),
                    "JOBNAME_ADDINFO": job_member_name,
                    "DATETIME_START": datetime.strptime(row['Start Time'], '%Y%m%d%H%M%S').strftime('%d/%m/%Y %H:%M:%S'),
                    "DATETIME_END": datetime.strptime(row['End Time'], '%Y%m%d%H%M%S').strftime('%d/%m/%Y %H:%M:%S'),
                    "DATETIME_RUN": run_time_formatted
                }
            }
            
            # Envia o POST para o EventHub
            response = requests.post(eventhubUrl, headers=headers, json=json_data, verify=False)
            
            if response.status_code == 200:
                logging.info(f"Evento da rotina {job_member_name} criado com sucesso")
                inc = retrieveInc(row) # Tenta buscar o incidente recém-criado
                createLogs('OK', row, inc) # Loga o sucesso
            else:
                logging.error(f"Erro ao criara evento da rotina {job_member_name}")
                createLogs('NOK', row) # Loga a falha
                
        except Exception as e:
            logging.error(f"Erro da rotina {job_member_name}\nServiceNow ERRO: {e}")

# --- Ponto de Entrada Principal do Script ---
if __name__ == "__main__":
    # Gera o token de autenticação
    bearer = gerar_token(endpoint, racf, decrypt(senha))
    
    logging.info("Gerando report dos jobs com falha")
    # Lê o relatório de jobs que não terminaram OK
    dfJobs = pd.read_csv(StringIO(gera_relatorio(endpoint, bearer, 'active_ended_notok')), header=0, encoding='utf-8', delimiter=',')
    
    # Renomeia colunas para padronização
    dfJobs = dfJobs.rename(columns={'Run ID': 'orderid'})
    dfJobs = dfJobs.rename(columns={'Server Name': 'ctm'})
    dfJobs = dfJobs.rename(columns={'Job Name': 'name'})
    
    # Define a coluna PLATAFORMA
    listOfColumns = ['PLATAFORMA', 'ctm', 'orderid', 'name', 'Number of Runs', 'Message', 'startTime', 'endTime', 'runTime']
    dfJobs['PLATAFORMA'] = dfJobs.apply(lambda row: 'DISTRIBUIDA' if row.ctm[0:2] == 'DS' else 'MAINFRAME', axis=1)
    
    # Converte colunas de tempo para datetime e formata
    columns_to_convert = ['Start Time', 'End Time', 'Run Time']
    for col in columns_to_convert:
        dfJobs[col] = pd.to_datetime(dfJobs[col]).dt.strftime('%Y%m%d%H%M%S')
        
    dfJobs.rename(columns={'Start Time': 'startTime', 'End Time': 'endTime', 'Run Time': 'runTime'}, inplace=True)
    
    logging.info("Gerando report dos alertas de cancelamento")
    # Lê o relatório de alertas de cancelamento
    dfAlerts = pd.read_csv(StringIO(gera_relatorio(endpoint, bearer, 'Alerts_Cancelamento')), header=0, encoding='utf-8', delimiter=',')
    
    # Trata e formata as colunas de tempo dos alertas
    dfAlerts['Time'] = pd.to_datetime(dfAlerts['Time'], format='%B %d, %Y %I:%M:%S %p', errors='coerce')
    dfAlerts['Time'] = dfAlerts['Time'].dt.strftime('%Y%m%d%H%M%S')
    
    dfAlerts = dfAlerts.rename(columns={'Server Name': 'Control-M Server Name', 'Run ID': 'Order ID'})
    
    # Faz um merge (LEFT JOIN) dos Jobs com os Alertas
    dfMerged = pd.merge(
        dfJobs,
        dfAlerts,
        how='left',
        left_on=['ctm', 'orderid', 'name', 'Number of Runs'],
        right_on=['Control-M Server Name', 'Order ID', 'Job Name', 'Run Counter']
    )
    
    dfMerged = dfMerged[listOfColumns] # Filtra para as colunas de interesse
    
    logging.info("Gerando lista de cancelamentos sem alerta no control M")
    # Filtra os jobs que não tiveram um alerta correspondente (Alert ID está nulo)
    dfCancelNoAlert = dfMerged[dfMerged['Alert ID'].isna()]
    
    if len(dfCancelNoAlert) > 0:
        # Verifica se já existe um job/incidente para esses cancelamentos
        dfCancelNoAlert = checkJobsNotFound(dfCancelNoAlert)
        
    # Busca alertas que não foram encontrados no ServiceNow
    notFound = findIncident(dfAlerts)
    
    if len(dfCancelNoAlert) > 0 or len(notFound) > 0:
        # Mescla os dois DataFrames
        mergedIncident = mergeColumns(dfCancelNoAlert, notFound, dfJobs)
        
        if len(mergedIncident) > sys.argv[2]: # Compara com o limite passado por argumento
            finaliza_token(endpoint, bearer)
            logging.info("Volumetria de casos maior que o comum")
            sys.exit(2)
        
        # Abre os incidentes
        openIncident(mergedIncident)
    else:
        logging.info("Nenhum caso encontrado para abertura")
        createLogs('NOF') # Loga que nada foi feito
    
    finaliza_token(endpoint, bearer)

