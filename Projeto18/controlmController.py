import requests
import json
from datetime import timedelta, date
import logging
import sys
import time
import pandas as pd
# Importa o módulo de gestão de tokens/configuração
import tokensController as tkc

# Desabilita avisos de segurança de requisições HTTPS
requests.urllib3.disable_warnings()

def controlm_action(jobID, tipo, token, requisitionType):
    """Função genérica para executar uma ação (hold, delete) num job."""
    logging.info(f"Executando {tipo} na rotina")
    headers = {'Authorization': f'Bearer {token}'}
    
    # Executa a ação (POST ou GET)
    log = requests.request(requisitionType, f"{tkc.endpoint}/run/job/{jobID}/{tipo}", headers=headers, verify=False)
    return log

# """
# deleta rotina no controlm, retorna True caso tudo de certo
# retorna False em caso de erro desconhecido
# quebra em caso de falha
# """
def delete_rotina(datacenter, orderid, token, counterINC):
    """Tenta deletar uma rotina específica no Control-M."""
    logging.info("Iniciando delete da rotina")
    jobid = f"{datacenter}:{orderid}"
    
    # 1. Verifica o status do job
    rotina = controlm_action(jobid, 'status', token, 'GET')
    
    if rotina.status_code == 404:
        try:
            erro = rotina.json()["errors"][0]["message"]
            logging.info(erro)
            
            if erro == "Job id {} not found".format(jobid):
                logging.info("Rotina não existe mais")
                return True, None
            else:
                logging.error("Rotina não está mais cancelada")
                return True, None # (A lógica aqui retorna True mesmo em erro)

        except:
            logging.error("Erro ao capturar rotina fora do plano no Control-M")
            erro = "Não foi possivel pegar a mensagem de erro"
            
        logging.error(rotina.content)
        logging.error("Erro desconhecido")
        sys.exit(99)
        
    elif rotina.status_code == 200:
        # Se o job existe, verifica o status
        held = rotina.json()['held']
        status = rotina.json()['status']
        counterJob = rotina.json()['numberOfRuns']
        hold = None # (Variável 'hold' não está definida aqui, pode ser um bug)
        
        if status == "Ended Not OK":
            if counterJob == counterINC:
                if held != True:
                    # 2. Coloca o job em "Hold"
                    hold = controlm_action(jobid, 'hold', token, 'POST')
                
                if (hold is not None and hold.status_code == 200) or held:
                    # 3. Deleta o job
                    delete = controlm_action(jobid, 'delete', token, 'POST')
                    
                    if delete.status_code == 200:
                        logging.info("Rotina deletada com sucesso!")
                        return True, None
                    else:
                        logging.info("Erro no delete da rotina")
                        if hold is not None:
                            erro = hold.json()["errors"][0]["message"]
                        else:
                            erro = "Erro desconhecido"
                else:
                    erro = hold.json()["errors"][0]["message"]
                    logging.error(f"Problema no Hold da Rotina ({erro})")
                return False, erro
            else:
                logging.error("Run Counter Job != INC") # Contador de execuções difere
                return True, None
        else:
            logging.error("Rotina não está mais cancelada") # Job não está em "Ended Not OK"
            return True, None
    else:
        return False, None # Outros status HTTP

def delete_rotina_hold(jobid, token):
    """Deleta uma rotina que já está em status 'Hold'."""
    headers = {'Authorization': f'Bearer {token}'}
    
    # 1. Busca o status da vizinhança do job (para verificar o status)
    rotina = requests.get(f"{tkc.endpoint}/run/jobs/status?neighborhood={jobid}&depth=10&direction=radial", headers=headers, verify=False)
    
    if rotina.status_code == 404:
        try:
            erro = rotina.json()["errors"][0]["message"]
            logging.error(erro)
            if erro == "Job id {} not found".format(jobid):
                return True, None # Job não existe mais
        except:
            logging.error("Erro ao capturar rotina fora do plano no Control-M")
            erro = "Nao foi possivel pegar a mensagem de erro"
        
        logging.error(rotina.content)
        logging.error("Erro desconhecido")
        
    elif rotina.status_code == 200:
        if len(rotina.json()['statuses']) == 1:
            logging.info(f"Deletando Rotina {rotina.json()['statuses'][0]['name']}")
            
            # 2. Deleta o job
            delete = requests.post(f"{tkc.endpoint}/run/job/{jobid}/delete", headers=headers, verify=False)
            
            if delete.status_code == 200:
                return True, None # Sucesso
            else:
                erro = delete.json()['errors'][0]['message']
                logging.error(f"Problema no Delete da Rotina\n{erro}")
        else:
            erro = "Rotina com sucessores"
            logging.error(erro)
    else:
        erro = "Erro desconhecido" # Outros status HTTP
        
    return False, erro # Retorna falha e a mensagem de erro

def close_alerts(alert_ids:list, token:str):
    """Envia uma requisição POST para fechar uma lista de alertas."""
    try:
        logging.info("Fechando alertas")
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # Preparar os dados para envio
        data = {
            "alertIds": alert_ids,
            "status": "Closed"
        }
        
        # Fazer a requisição POST para fechar os alertas
        response = requests.post(
            f"{tkc.endpoint}/run/alerts/status", headers=headers, json=data, verify=False
        )

        if response.status_code == 200: # Sucesso
            logging.info("Alertas fechados com sucesso")
        else:
            # Trata erros
            logging.error(f"Erro ao fechar alertas: {response.status_code} - {response.text}")
            error_data = response.json()
            
            # Extrai IDs de alertas que falharam da resposta de erro
            error_ids = error_data['errors'][0]['message'].split('[')[1].split(']')[0]
            alerts_failed = [int(id.strip()) for id in error_ids.split(',')]
            
            alert_ids = pd.DataFrame(alert_ids, columns=['Alert ID'])
            # Filtra os alertas que tiveram sucesso
            sucessAlerts = alert_ids[~alert_ids['Alert ID'].isin(alerts_failed)]

            if len(sucessAlerts) > 0:
                sucessAlerts = sucessAlerts['Alert ID'].tolist()
                
            logging.info(f'Lista de alertas fechados: {sucessAlerts}\nLista de alertas falhados: {alerts_failed}')
            response.raise_for_status() # Levanta um erro HTTP
            
    except Exception as e:
        logging.critical(e)

def get_job_report(token:str, reportName:str):
    """Método para buscar reports em geral (Jobs)."""
    try:
        logging.info("Gerando report das Rotinas")
        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
        
        # Calcula data de 15 dias atrás
        today = date.today()
        ago_15 = today - timedelta(days=15)
        str_ago_15 = ago_15.strftime('%Y%m%d')
        
        # Define o payload para solicitar o relatório
        data = {
            "name": reportName,
            "format": "csv",
            "filters": {
                "name": "Run Date",
                "value": str_ago_15
            }
        }
        
        # 1. Solicita a geração do relatório
        response = requests.post(f"{tkc.endpoint}/reporting/report", headers=headers, verify=False, json=data)
        responseData = response.json()
        reportId = responseData['reportId']

        # 2. Espera o relatório ficar pronto (Polling)
        while True:
            response = requests.get(f"{tkc.endpoint}/reporting/status/{reportId}", verify=False, headers=headers)
            responseData = response.json()
            downloadStatus = responseData['status']
            
            if downloadStatus.upper() == 'SUCCEEDED':
                break
            elif downloadStatus.upper() in ['PENDING', 'PROCESSING']:
                time.sleep(5) # Espera 5 segundos
            else:
                raise ValueError(f"Status do download invalido ou nao encontrado: {downloadStatus}")
        
        # 3. Baixa o relatório pronto
        response = requests.get(f"{tkc.endpoint}/reporting/download/{reportId}", verify=False, headers=headers)
        string = response.content.decode('utf-8')
        df = pd.read_csv(StringIO(string), header=0, encoding='utf-8', delimiter=',')
        
    except Exception as e:
        logging.critical(e)
        logging.critical(response)
        sys.exit(3)
        
    if len(df) > 0:
        report_rotinas_json = df.to_json(orient='records')
        report_rotinas_dict = json.loads(report_rotinas_json)
        return report_rotinas_dict
    else:
        return "Dataframe vazio"

def get_alert_report(token:str, reportName:str, alertID):
    """Método para buscar reports em geral (Alertas)."""
    try:
        logging.info("Gerando report do Alerta")
        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
        
        # Define o payload para solicitar o relatório
        data = {
            "name": reportName,
            "format": "csv",
            "filters": [
                {
                    "name": "Alert ID",
                    "value": alertID
                }
            ]
        }

        # 1. Solicita a geração do relatório
        response = requests.post(f"{tkc.endpoint}/reporting/report", headers=headers, verify=False, json=data)
        responseData = response.json()
        reportId = responseData['reportId']

        # 2. Espera o relatório ficar pronto (Polling)
        while True:
            response = requests.get(f"{tkc.endpoint}/reporting/status/{reportId}", verify=False, headers=headers)
            responseData = response.json()
            downloadStatus = responseData['status']
            
            if downloadStatus.upper() == 'SUCCEEDED':
                break
            elif downloadStatus.upper() in ['PENDING', 'PROCESSING']:
                time.sleep(5) # Espera 5 segundos
            else:
                raise ValueError(f"Status do download invalido ou nao encontrado: {downloadStatus}")

        # 3. Baixa o relatório pronto
        response = requests.get(f"{tkc.endpoint}/reporting/download/{reportId}", verify=False, headers=headers)
        string = response.content.decode('utf-8')
        df = pd.read_csv(StringIO(string), header=0, encoding='utf-8', delimiter=',')
        
    except Exception as e:
        logging.error(e)
        logging.error(response)
        sys.exit(3)
        
    return len(df)
