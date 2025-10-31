import requests
import logging
# Importa os módulos de controle (provavelmente outros scripts .py)
import controlMController as ctmc
import dbController as dbc
import serviceNowController as snowc
import tokensController as tkc
import sys

# --- Configuração de Caminho e Módulos ---
# Define o caminho dos módulos de configuração
# #/PROD/PGMS/itau-sj7-modules-datalake-conf
sys.path.append(r'/PROD/PGMS/itau-sj7-modules-datalake-conf')

# Importa funções específicas do módulo ger_token
from ger_token import gerar_token, finaliza_token # type: ignore

# --- Configuração Inicial ---
# Obtém o ambiente da linha de comando (ex: PRD)
ambient = sys.argv[-1]

# Configuração básica de logging
logging.basicConfig(format='%(levelname)s: %(asctime)s - %(message)s\n', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
# Reduz o nível de log das bibliotecas 'requests' e 'urllib3' para WARNING
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
# Desabilita avisos de segurança de requisições HTTPS
requests.urllib3.disable_warnings()


# --- Ponto de Entrada Principal do Script ---
if __name__ == "__main__":
    logging.info("Iniciando execução...")
    
    # Gera o token de autenticação principal usando as credenciais do tkc
    token = gerar_token(tkc.endpoint, tkc.decrypt(tkc.VAR1), tkc.decrypt(tkc.VAR2))

    # --- Processamento de Incidentes ---
    
    # Processa incidentes não globais DS
    # snowc.process_incidents('DS', token, False)
    
    # Processa incidentes globais DS
    snowc.process_incidents('DS', token, True)
    
    # Processa incidentes não globais MF
    # #snowc.process_incidents('MF', token, False)
    
    # Processa incidentes globais MF
    # #snowc.process_incidents('MF', token, True)

    # --- Fechamento de Alertas ---
    
    # Verifica se há alertas a serem fechados (variável tkc.alert_ids é preenchida pelo snowc)
    if len(tkc.alert_ids) > 0:
        ctmc.close_alerts(tkc.alert_ids, token)
    else:
        logging.info("Sem alertas para fechar")

    # --- Finalização e Log ---
    
    # Finaliza o token de autenticação
    finaliza_token(tkc.endpoint, token)

    # Se nenhum job foi deletado (variável global do tkc), loga 'NOF'
    if tkc.deleted_jobs == 0:
        dbc.createLogs(4, "NOF", None) # O '4' pode ser um ID de automação
        logging.info("Nenhuma Rotina deletada nessa execução")

    logging.info("FIM")
