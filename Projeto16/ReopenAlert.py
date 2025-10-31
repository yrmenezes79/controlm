

import requests
import sys
import pandas as pd
from io import StringIO
import logging
from datetime import datetime
# Importações de módulos locais
from config import findToken, decrypt, createLogs, path, ambient_conf

# Configuração dos caminhos para módulos locais
# Caminho local
# #'C:Users\gdagyae\OneDrive - Banco Itaú SA\Área de Trabalho\Projects\itau-sj7-modules-datalake-conf'
# Caminho do servidor
# #/PROD/PGNS/itau-sj7-modules-datalake-conf
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
host = findToken('host')
current_time = datetime.now()

def compare_alerts_with_active_jobs(df_alerts: pd.DataFrame, df_activeNOK_jobs: pd.DataFrame):
    """Compara alertas com jobs que não terminaram OK para decidir quais reabrir."""
    alerts_to_open = [] # Lista para guardar IDs de alertas que devem ser reabertos

    for index, alert in df_alerts.iterrows():
        # Verifica se o 'Run ID' do alerta existe nos jobs que não terminaram OK
        if alert['Run ID'] in df_activeNOK_jobs['Run ID'].values:
            # Localiza o job correspondente
            current_job = df_activeNOK_jobs.loc[df_activeNOK_jobs['Run ID'] == alert['Run ID']]
            loc = 0
            
            # Se não houver correspondência de servidor, pula para o próximo alerta
            if not current_job['Server Name'].isin([alert['Server Name']]).any():
                continue
            
            # Obtém o índice correto para o job (caso haja duplicatas de Run ID)
            loc = current_job[current_job['Server Name'] == alert['Server Name']].index
            loc = int(loc[0])
            
            # Extrai detalhes do job
            number_of_runs = int(current_job['Number of Runs'][loc])
            
            # Se o contador de execuções do alerta for igual ao do job, reabre
            if number_of_runs == alert['Run Counter']:
                alerts_to_open.append(alert['Alert ID'])
                continue # Pula para o próximo alerta
                
    return alerts_to_open # Retorna a lista de alertas para reabrir

def open_alerts(alert_ids: list, token: str):
    """Envia uma requisição POST para reabrir uma lista de alertas."""
    
    logging.info("Reabrindo alertas")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Preparar os dados para enviar
    data = {
        "alertIds": alert_ids,
        "status": "New" # Define o novo status como "New"
    }
    
    # Fazer a requisição POST para fechar (reabrir) os alertas
    response = requests.post(
        f"{endpoint}/run/alerts/status", headers=headers, json=data, verify=False
    )

    if response.status_code == 200: # Sucesso
        logging.info("Alertas abertos com sucesso")
        createLogs('OK', 2, df_alerts) # df_alerts não está definido neste escopo
    else:
        # Trata erros
        logging.error(f"Erro ao abrir alertas: {response.status_code} - {response.text}")
        error_data = response.json()
        
        # Extrai IDs de alertas que falharam da resposta de erro
        error_ids = error_data['errors'][0]['message'].split('[')[1].split(']')[0]
        alerts_failed = [int(id.strip()) for id in error_ids.split(',')]
        
        alert_ids = pd.DataFrame(alert_ids, columns=['Alert ID'])
        # Filtra os alertas que tiveram sucesso
        sucessAlerts = alert_ids[~alert_ids['Alert ID'].isin(alerts_failed)]

        if len(sucessAlerts) > 0:
            sucessAlerts = sucessAlerts['Alert ID'].tolist()
            createLogs('OK', 2, df_alerts_sucess) # df_alerts_sucess não está definido
            
            createLogs('NOK', 2, alerts_failed) # alerts_failed é uma lista, não um DataFrame
            logging.info(f'Lista de Alertas Reabertos: {sucessAlerts}\nLista de Alertas falhados: {alerts_failed}')
        
        response.raise_for_status() # Levanta um erro HTTP

# Bloco principal de execução do script
if __name__ == "__main__":
    try:
        logging.info('Inicio')
        
        # Obtém o token de autenticação
        token = gerar_token(endpoint, racf, decrypt(senha))
        
        # Gera e lê relatórios da API (presumivelmente CSVs)
        # Nota: O nome do relatório 'Alerts_Tratamento (Cancelamento)' é diferente do script anterior
        df_alerts = pd.read_csv(StringIO(gera_relatorio(endpoint, token, 'Alerts_Tratamento (Cancelamento)')), header=0, encoding='utf-8', delimiter=',')
        
        # Converte a coluna 'Time' para um formato de data/hora mais padronizado
        df_alerts['Time'] = df_alerts['Time'].apply(lambda value: datetime.strptime(value, "%B %d, %Y %I:%M:%S %p").strftime('%Y-%m-%d %H:%M:%S'))
        
        df_activeNOK_jobs = pd.read_csv(StringIO(gera_relatorio(endpoint, token, 'Active_Jobs_NOTOK')), header=0, encoding='utf-8', delimiter=',')
        
        logging.info("Comparando alertas com jobs ativos")
        
        # Compara os alertas para decidir quais devem ser reabertos
        alerts_to_open = compare_alerts_with_active_jobs(df_alerts, df_activeNOK_jobs)
        
        if len(alerts_to_open) == 0:
            logging.warning("Nenhum alerta encontrado, finalizando processo")
            createLogs('NOF', 2)
            finaliza_token(endpoint, token)
            sys.exit(0)
            
        logging.info(f"Realizando a abertura dos alertas encontrados: {alerts_to_open}")
        
        # Chama a função para reabrir os alertas
        open_alerts(alerts_to_open, token)
        
        finaliza_token(endpoint, token)
        logging.info("FIM")

    except Exception as e:
        logging.critical(e)
        sys.exit(3)
