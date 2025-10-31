
import requests
import sys
import pandas as pd
from io import StringIO
import logging
from datetime import datetime
# Importações de módulos locais
from config import findToken, decrypt, createLogs, path, ambient_conf

# Configuração dos caminhos para módulos locais
# Caminho do servidor
# #/PROD/PGMS/itau-sj7-modules-datalake-conf 
# Caminho local
# #'C:Users\gdagyae\OneDrive - Banco Itaú SA\Área de Trabalho\Projects\itau-sj7-modules-datalake-conf'
sys.path.append(path)

# Importações de módulos locais (após adicionar ao path)
from ger_relatorio import gera_relatorio # type: ignore
from ger_token import gerar_token, finaliza_token # type: ignore

# Configuração básica do logging
logging.basicConfig(format='%(levelname)s: %(asctime)s - %(message)s\n', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
# Reduz o nível de log das bibliotecas 'requests' e 'urllib3' para WARNING
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
# Desabilita avisos de segurança de requisições HTTPS
requests.urllib3.disable_warnings()

# Obtém o endpoint da configuração de ambiente
endpoint = ambient_conf['ENDPOINTCTM']

# Obtém credenciais usando a função findToken
racf = findToken('user_token_control')
senha = findToken('pass_token_control')
splunkToken = findToken('splunk')
host = findToken('host')
current_time = datetime.now()

def compare_alerts_with_active_jobs(alerts_report: pd.DataFrame, active_jobs_report: pd.DataFrame, alerts_late: pd.DataFrame):
    """Valida regras para fechar alertas"""
    matchedAlert = [] # Lista para guardar IDs de alertas que NÃO devem ser fechados

    for index, row in alerts_report.iterrows():
        # Verifica se o 'Run ID' do alerta existe nos jobs ativos
        if row['Run ID'] in active_jobs_report['Run ID'].values:
            # Localiza o job ativo correspondente
            current_job = active_jobs_report.loc[active_jobs_report['Run ID'] == row['Run ID']]
            loc = 0
            
            # Lógica para tratar múltiplos jobs com mesmo Run ID, mas servidores diferentes
            if current_job['Server Name'].isin([row['Server Name']]).any():
                loc = current_job[current_job['Server Name'] == row['Server Name']].index
                loc = int(loc[0])
            else:
                matchedAlert.append(row['Alert ID']) # Marca para não fechar se o servidor não bater
                continue # Pula para o próximo alerta

            # Extrai detalhes do job ativo
            job_status = current_job['Job Status'][loc]
            start_time = current_job['Start Time'][loc]
            cyclic = current_job['Cyclic'][loc]
            number_of_runs = int(current_job['Number of Runs'][loc])

            # Condição 1: Verifica se o job NÃO está 'Executing' E se a mensagem indica 'ended not ok'
            if (job_status != 'Executing' and not pd.isna(start_time) and (row['Message'].lower() == 'ended not ok' or
                (row['Message'].lower() == 'ended not ok' and job_status.lower() == 'ended not ok' and cyclic == 'No'))):
                matchedAlert.append(row['Alert ID']) # Marca para não fechar
            # Condição 2: Se for cíclico
            elif cyclic == 'Yes':
                # E o contador de execuções do alerta for menor que o total de execuções do job
                if row['Run Counter'] < number_of_runs:
                    matchedAlert.append(row['Alert ID']) # Marca para não fechar
            # Condição 3: Outros casos
            else:
                matchedAlert.append(row['Alert ID']) # Marca para não fechar

    # Loop secundário para comparar com 'alerts_late' (Lógica parece similar, talvez para outro tipo de job)
    for index, row in alerts_late.iterrows():
        if row['Run ID'] in active_jobs_report['Run ID'].values:
            matchedAlert.append(row['Alert ID']) # Marca para não fechar se estiver em jobs ativos
            continue
            
            # O código abaixo deste 'continue' nunca será executado.
            # Mantido conforme a imagem.
            current_job = active_jobs_report.loc[active_jobs_report['Run ID'] == row['Run ID']]
            loc = 0
            if current_job['Server Name'].isin([row['Server Name']]).any():
                loc = current_job[current_job['Server Name'] == row['Server Name']].index
            else:
                matchedAlert.append(row['Alert ID'])
                continue
            
            job_status = current_job['Job Status'][loc]
            cyclic = current_job['Cyclic'][loc]
            number_of_runs = int(current_job['Number of Runs'][loc])
            
            if cyclic == 'No':
                if row['Run Counter'] != number_of_runs:
                    matchedAlert.append(row['Alert ID'])
                    continue
            if job_status == 'Executing':
                matchedAlert.append(row['Alert ID'])

    return matchedAlert # Retorna a lista de Alertas que NÃO devem ser fechados

def close_alerts(alert_ids:list, token:str):
    """Envia uma requisição POST para fechar uma lista de alertas."""
    try:
        logging.info("Fechando alertas")
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        data = {
            "alertIds": alert_ids,
            "status": "Closed"
        }
        
        # Envia a requisição para a API
        response = requests.post(
            f"{endpoint}/run/alerts/status", headers=headers, json=data, verify=False
        )

        if response.status_code == 200:
            logging.info("Alertas fechados com sucesso")
            createLogs('OK', 1, df_alerts_fechados) # df_alerts_fechados não está definido neste escopo
        
        else:
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
                createLogs('OK', 1, df_alerts_fechados) # df_alerts_fechados não está definido
                logging.info(f'Lista de alertas fechados: {sucessAlerts}')
            
            logging.info(f'Lista de alertas falhados: {alerts_failed}')
            response.raise_for_status() # Levanta um erro HTTP
            
    except Exception as e:
        logging.critical(e)
        sys.exit(3)

# Bloco principal de execução do script
if __name__ == "__main__":
    logging.info('Inicio')
    
    # Obtém o token de autenticação
    token = gerar_token(endpoint, racf, decrypt(senha))
    
    # Gera e lê relatórios da API (presumivelmente CSVs)
    df_alerts = pd.read_csv(StringIO(gera_relatorio(endpoint, token, 'Alerts_Tratamento')), header=0, encoding='utf-8', delimiter=',')
    df_active_jobs = pd.read_csv(StringIO(gera_relatorio(endpoint, token, 'Active_Jobs_Execution')), header=0, encoding='utf-8', delimiter=',')
    df_alerts_late = pd.read_csv(StringIO(gera_relatorio(endpoint, token, 'Alerts_LateSub/LateTime')), header=0, encoding='utf-8', delimiter=',')
    
    logging.info('Comparando alertas com jobs ativos')
    
    # Compara os alertas para decidir quais não fechar
    unmatched_alerts = compare_alerts_with_active_jobs(df_alerts, df_active_jobs, df_alerts_late)
    
    # Filtra os alertas para obter a lista final dos que devem ser fechados
    df_alerts_fechados = pd.concat([df_alerts, df_alerts_late], ignore_index=True)
    
    if len(unmatched_alerts) > 0:
        logging.info('Nenhum alerta encontrado para limpeza') # Esta mensagem parece incorreta, deveria ser o oposto?
        createLogs('NOF', 1)
    else:
        # Fecha os alertas que não estão na lista 'unmatched_alerts'
        close_alerts(unmatched_alerts, token) # Esta lógica parece invertida. Está passando os alertas que NÃO devem ser fechados.
        
    finaliza_token(endpoint, token)
    logging.info("FIM")
