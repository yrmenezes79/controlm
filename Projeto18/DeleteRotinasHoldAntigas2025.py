import requests
import logging
import pandas as pd
import sys

# --- Importações de Módulos Locais ---
# (Assumindo que estes módulos estão no path ou no mesmo diretório)
import controlMController as ctmc
import dbController as dbc
import tokensController as tkc

# --- Configuração de Logging ---
logging.basicConfig(format='%(levelname)s: %(asctime)s - %(message)s\n', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
requests.urllib3.disable_warnings() # Desabilita avisos de HTTPS

# --- Adição de Caminho para Módulos (se necessário) ---
# (O caminho do C: e /PROD/PGMS parece estar comentado ou ausente nesta versão)
# sys.path.append(r'C:\Users\gdagyae\OneDrive - Banco Itaú SA\Área de Trabalho\Projects\itau-sj7-modules-datalake-conf')
from ger_token import gerar_token, finaliza_token # type: ignore

# --- Comentários Explicativos ---
# """
# deleta rotina no controlm, retorna True caso tudo de certo
# retorna False em caso de erro desconhecido
# quebra em caso de falha
# """

# --- Ponto de Entrada Principal do Script ---
if __name__ == "__main__":
    logging.info("Iniciando execução...")
    
    # Gera o token de autenticação principal
    token = gerar_token(tkc.endpoint, tkc.decrypt(tkc.VAR1), tkc.decrypt(tkc.VAR2))
    
    # Define o nome do relatório a ser buscado
    reportNameJobs = "Jobs_OnHold_15_Dias"

    # 1. Obter o relatório de rotinas em Hold
    report_rotinas_dict = ctmc.get_job_report(token, reportNameJobs)

    if report_rotinas_dict != "Dataframe vazio":
        logging.info(f"Total de {len(report_rotinas_dict)} rotina(s)")
        # 2. Processar cada registro no relatório
        for record in report_rotinas_dict:
            joblist = [] # Lista para armazenar o job processado (para o log)
            
            # Extrai informações do registro
            server_name = record["Server Name"]
            run_id = record["Run ID"]
            job_name = record["Job Name"]
            
            logging.info(f"Processando rotina {job_name}")
            
            if server_name and run_id:
                # 3. Gerar o job_id (formato: "servidor:run_id")
                # (A imagem mostra f"{server_name}:{run_id}", mas a lógica abaixo
                #  parece focar em job_id = run_id)
                job_id = run_id 
                
                # 4. Chamar a função delete_rotina
                datacenter = server_name # Assumindo que datacenter é o mesmo que server_name
                orderid = run_id
                
                # Deleta a rotina em hold
                rotina_deletada, erro = ctmc.delete_rotina_hold(job_id, token)

                if rotina_deletada:
                    # Sucesso
                    tkc.deleted_jobs += 1
                    logging.info(f"Rotina {job_id} deletada com sucesso.")
                    joblist.append(job_name)
                    df = pd.DataFrame(joblist, columns=["Job_Name"])
                    dbc.createLogs(6, "OK", df) # Loga sucesso (6 = ID da automação?)
                else:
                    # Falha
                    tkc.deleted_jobs += 1 # (?? Esta linha parece incorreta, incrementa mesmo em falha)
                    logging.error(f"Falha ao deletar rotina {job_name}.")
                    joblist.append((job_name, erro)) # Adiciona o job e o erro
                    df = pd.DataFrame(joblist, columns=["Job_Name", "Error_Message"])
                    dbc.createLogs(6, "NOK", df) # Loga falha
            
            else:
                logging.info("Rotina sem server ou run id cadastrado")
                
    else:
        logging.info("Sem rotina em hold a mais de 15 dias")

    # 5. Log final da execução
    if tkc.deleted_jobs == 0:
        dbc.createLogs(6, "NOF", None) # Loga "Nada Feito"
        logging.info("Nenhuma Rotina deletada nessa execução")

    # (A finalização do token e o log "FIM" não estão na imagem,
    #  mas provavelmente deveriam estar aqui)
    # finaliza_token(tkc.endpoint, token)
    # logging.info("FIM")
    
