import requests
import json
from datetime import datetime, timedelta
import time
import pandas as pd
import logging

# Importa os outros módulos de controle
import controlMController as ctmc
import dbController as dbc
import tokensController as tkc

# Desabilita avisos de segurança de requisições HTTPS
requests.urllib3.disable_warnings()

def close_incident(headers, sys_id):
    """Fecha um incidente no ServiceNow."""
    now = datetime.now()
    # Payload para fechar o incidente
    data = json.dumps({
        "work_notes": "Incidente finalizado e rotina deletada por se passarem mais de 10 dias desde o início do impacto",
        "u_impact_end": now.strftime("%Y-%m-%d %H:%M:%S"),
        "close_code": "AC",
        "close_notes": "Delete da(s) rotina(s) por falta de atuação",
        "state": "6",
        "incident_state": "6", # (Duplicado?)
        "u_automation_status": "SUCESSO"
    })
    
    # Envia a requisição PUT
    response = requests.put(f"{tkc.url_base}/now/table/incident/{sys_id}",
                            headers=headers,
                            data=data,
                            verify=False)
    
    if response.status_code != 200:
        logging.error(f"Problema na atualização do Incidente\n{response.content}")
    else:
        logging.info("Incidente resolvido")

def incidenteFailureCategory(headers, sys_id):
    """Atualiza a categoria de falha de um incidente."""
    # Payload com as categorias de falha
    payload = json.dumps({
        "u_level1": "a62be05db9458108739fcc0cbccb3e",
        "u_level2": "a62be05db9458108739fcc0cbccb3e", # (IDs parecem repetidos)
        "u_level3": "3f58ca37bfe0b9b086ab8e54bcbca7",
        "u_level4": "01e24a61b0714105825be4bcbce",
        "u_level5": "01e24a61b0714105825be4bcbce",
        "u_level6": "7f24d177bfe2b8d0b9da8ee54bc82",
    })

    i = 0
    while i != 10: # Tenta até 10 vezes
        time.sleep(1) # Espera 1 segundo
        response = requests.put(f"{tkc.url_base}/now/table/incident/{sys_id}",
                                headers=headers,
                                data=payload,
                                verify=False)
        i += 1
        if response.status_code == 200:
            logging.info("Atualização categoria de falha")
            return True
            
    # Se falhar 10 vezes
    if response.status_code != 200:
        logging.error(f"Falha na escrita da categoria de falha\n{response.text}")
    else:
        logging.info("Atualização categoria de falha")
        return True
    
    return False # Retorna Falso se falhar

def process_incidents(server, token, is_global):
    """Processa incidentes (globais ou não) para deletar rotinas."""
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Basic {tkc.decrypt(tkc.VAR6)}' # VAR6 = snow_token?
    }

    # Define o número de dias para a busca (10 para DS, 15 para MF)
    days = '10' if server == 'DS' else '15'

    if not is_global:
        # --- Processamento de Incidentes NÃO GLOBAIS ---
        logging.info(f"Incidentes nao globais {server}")
        params = {
            "sysparm_query": f"u_source=ControlM^category=monitoring^parent_incidentISEMPTY^child_incidents=0^u_datacenterSTARTSWITH{server}^u_orderidISNOTEMPTY^sys_created_onRELATIVELT@dayofweek@ago@{days}^stateIN1,2,3^u_additional_infoNOT LIKE%evczx02cto"
        }
        
        response = requests.get(f"{tkc.url_base}/now/table/incident", verify=False, headers=headers, params=params)

        if response.status_code == 200:
            incidentes = response.json()['result']
            logging.info(f"{len(incidentes)} Incidentes encontrados")
            
            for inc in incidentes:
                joblist = []
                logging.info(f"Processando incidente {inc['number']}")
                
                # Carrega 'additional_info' como JSON
                addinfo = json.loads(inc["u_additional_info"])
                jobName = addinfo["JOBNAME_ADDINFO"]
                
                # Converte o timestamp do incidente
                incidenteTime = datetime.strptime(inc["sys_created_on"], "%Y-%m-%d %H:%M:%S")
                
                # Se o incidente for mais antigo que 10 dias
                if incidenteTime < tkc.n_10: 
                    run_number = int(inc["u_run_number"]) if inc["u_run_number"] else 0
                    
                    # Tenta deletar a rotina no Control-M
                    flagRotina, erro = ctmc.delete_rotina(inc["u_datacenter"], inc["u_orderid"], token, run_number)

                    if flagRotina:
                        # Se deletou, prepara o log de sucesso
                        joblist.append((jobName, inc["number"], verificar_alert_id(inc["u_alert_id"])))
                        df = pd.DataFrame(joblist, columns=["Job_Name", "INC", "Alert_Id"])
                        dbc.createLogs(4, "OK", df) # Loga 4 = OK
                        tkc.deleted_jobs += 1
                        
                        # Atualiza a categoria de falha no ServiceNow
                        flagCategoria = incidenteFailureCategory(headers, inc["sys_id"])
                        if flagCategoria:
                            close_incident(headers, inc["sys_id"]) # Fecha o incidente
                            
                            # Verifica se existe um alerta associado para fechar
                            lenDf = ctmc.get_alert_report(token, 'Alerts_TEST', verificar_alert_id(inc["u_alert_id"]))
                            if lenDf > 0:
                                tkc.alert_ids.append(inc["u_alert_id"])
                        else:
                            logging.error("Erro desconhecido flagCategoria = False")
                    else:
                        # Se falhou ao deletar, prepara o log de erro
                        logging.error(f"Erro desconhecido flagRotina = False - Incidente Pai")
                        joblist.append((jobName, inc["number"], verificar_alert_id(inc["u_alert_id"]), erro))
                        df = pd.DataFrame(joblist, columns=["Job_Name", "INC", "Alert_Id", "Error_Message"])
                        dbc.createLogs(4, "NOK", df) # Loga 4 = NOK
                else:
                    logging.info("Incidente com menos de 10 dias desde a data de criacao")
        else:
            logging.error(f"Erro buscando informações do incidente\n{response}")
            
    else:
        # --- Processamento de Incidentes GLOBAIS ---
        logging.info(f"Incidentes Globais {server}")
        params = {
            "sysparm_query": f"u_source=ControlM^descriptionSTARTSWITHCancelamento^category=monitoring^child_incidents>0^u_datacenterSTARTSWITH{server}^u_orderidISNOTEMPTY^sys_created_onRELATIVELT@dayofweek@ago@{days}^stateNOT IN6,7,8^u_additional_infoNOT LIKE%evczx02cto"
        }
        
        response = requests.get(f"{tkc.url_base}/now/table/incident", verify=False, headers=headers, params=params)

        if response.status_code == 200:
            incidentes = response.json()['result']
            logging.info(f"{len(incidentes)} Incidentes encontrados")
            
            for inc in incidentes:
                joblist = []
                addinfo = json.loads(inc["u_additional_info"])
                jobName = addinfo["JOBNAME_ADDINFO"]
                sys_id = inc["sys_id"]
                logging.info(f"Processando incidente {inc['number']}")
                
                incidenteTime = datetime.strptime(inc["sys_created_on"], "%Y-%m-%d %H:%M:%S")
                
                # Se o incidente for mais antigo que 10 dias
                if incidenteTime < tkc.n_10:
                    # 1. Buscar incidentes filhos
                    params_child = {
                        "sysparm_query": f"parent_incident={sys_id}^sys_created_onRELATIVELT@dayofweek@ago@{days}"
                    }
                    response_child = requests.get(f"{tkc.url_base}/now/table/incident", verify=False, headers=headers, params=params_child)
                    
                    if response_child.status_code == 200:
                        childIncidents = response_child.json()['result']
                        qtdChild = len(childIncidents)
                        
                        if qtdChild > 0:
                            for childInc in childIncidents:
                                childjoblist = []
                                addinfo = json.loads(childInc["u_additional_info"])
                                childJobName = addinfo["JOBNAME_ADDINFO"]
                                logging.info(f"Incidente Filho criado em: {childInc['sys_created_on']}")
                                incidenteTimeChild = datetime.strptime(childInc["sys_created_on"], "%Y-%m-%d %H:%M:%S")
                                
                                # Se o incidente filho também for antigo
                                if incidenteTimeChild < tkc.n_10:
                                    run_number = int(childInc["u_run_number"]) if childInc["u_run_number"] else 0
                                    
                                    # 2. Tenta deletar a rotina do incidente filho
                                    flagRotina, erro = ctmc.delete_rotina(childInc["u_datacenter"], childInc["u_orderid"], token, run_number)
                                    
                                    tkc.deleted_jobs += 1 # Incrementa contador
                                    
                                    lenDf = ctmc.get_alert_report(token, 'Alerts_TEST', verificar_alert_id(inc["u_alert_id"]))
                                    if lenDf > 0:
                                        tkc.alert_ids.append(inc["u_alert_id"])
                                        
                                    if flagRotina:
                                        # Log de sucesso para o filho
                                        logging.error("Erro desconhecido flagRotina = False - Incidente Filho") # (Mensagem de log parece incorreta)
                                        childjoblist.append((childJobName, childInc["number"], verificar_alert_id(childInc["u_alert_id"])))
                                        df = pd.DataFrame(childjoblist, columns=["Job_Name", "INC", "Alert_Id"])
                                        dbc.createLogs(4, "OK", df)
                                    else:
                                        # Log de falha para o filho
                                        childjoblist.append((childJobName, childInc["number"], verificar_alert_id(childInc["u_alert_id"]), erro))
                                        df = pd.DataFrame(childjoblist, columns=["Job_Name", "INC", "Alert_Id", "Error_Message"])
                                        dbc.createLogs(4, "NOK", df)
                                else:
                                    logging.info(f"Incidente filho com menos de {days} dias da data de cancelamento")
                            
                            # Após processar todos os filhos, processa o pai
                            run_number = int(inc["u_run_number"]) if inc["u_run_number"] else 0
                            flagRotina, erro = ctmc.delete_rotina(inc["u_datacenter"], inc["u_orderid"], token, run_number)
                            
                            if flagRotina:
                                joblist.append((jobName, inc["number"], verificar_alert_id(inc["u_alert_id"])))
                                df = pd.DataFrame(joblist, columns=["Job_Name", "INC", "Alert_Id"])
                                dbc.createLogs(4, "OK", df)
                                tkc.deleted_jobs += 1
                                
                                flagCategoria = incidenteFailureCategory(headers, inc["sys_id"])
                                if flagCategoria:
                                    close_incident(headers, inc["sys_id"])
                                    lenDf = ctmc.get_alert_report(token, 'Alerts_TEST', verificar_alert_id(inc["u_alert_id"]))
                                    if lenDf > 0:
                                        tkc.alert_ids.append(inc["u_alert_id"])
                                else:
                                    logging.error("Erro desconhecido flagCategoria = False")
                            else:
                                logging.error("Erro desconhecido flagRotina = False - Incidente Pai")
                                joblist.append((jobName, inc["number"], verificar_alert_id(inc["u_alert_id"]), erro))
                                df = pd.DataFrame(joblist, columns=["Job_Name", "INC", "Alert_Id", "Error_Message"])
                                dbc.createLogs(4, "NOK", df)
                        else:
                            logging.error(f"Problema em recuperar incidentes filhos\n{response.content}")
                    else:
                        logging.info(f"Menos de {days} dias da data do cancelamento da rotina")
                else:
                    logging.error(response)

def verificar_alert_id(alert_id_str):
    """Verifica se o ID do alerta é um dígito e o retorna, senão retorna 0."""
    if alert_id_str.isdigit():
        return int(alert_id_str)
    else:
        return 0 # Retorna 0 se não for um ID de alerta válido
