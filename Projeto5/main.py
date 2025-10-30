import requests
import mysql.connector
import time
import os
import glob
from datetime import datetime
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

JOBNAME = "SJ7PA012"
DATABASE = "datalake"
SERVER = "scxxp0816cto.d.itau"
ARQUIVO = f"/PROD/POMG/FILE/{JOBNAME}.FILE"
ENDPOINT = "https://vcxx148cto:8443/automation-api"
REPORT_NAME = "bim_servicenow"
TABLE = "tb_bim_servicenow"

def ler_arquivo_configuracao(caminho_arquivo):
    try:
        with open(caminho_arquivo, 'r') as arquivo:
            usuario, senha = None, None
            for linha in arquivo:
                if linha.startswith("user="):
                    usuario = linha.split("=")[1].strip()
                elif linha.startswith("senha="):
                    senha = linha.split("=")[1].strip()
            if usuario and senha:
                return usuario, senha
            else:
                raise ValueError("Usuário ou senha não encontrados no arquivo.")
    except FileNotFoundError:
        print(f"Arquivo {caminho_arquivo} não encontrado.")
        return None, None
    except Exception as e:
        print(f"Erro ao ler arquivo de configuração: {str(e)}")
        return None, None

caminho_arquivo = "/PROD/POMG/pf_datalake"
VAR1, VAR2 = ler_arquivo_configuracao(caminho_arquivo)

caminho_arquivo = "/PROD/POMG/cps049c"
VAR3, VAR4 = ler_arquivo_configuracao(caminho_arquivo)

def gerar_token_api(ENDPOINT, usuario, password):
    print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Geracao do Token")
    response = requests.post(
        f"{ENDPOINT}/session/login",
        json={"username": usuario, "password": password},
        verify=False
    )
    response.raise_for_status()
    return response.json().get("token")

def gerar_relatorio(token, report_name):
    print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Geracao do relatorio - {report_name}")
    response = requests.post(
        f"{ENDPOINT}/reporting/report",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        },
        json={"name": report_name, "format": "CSV"},
        verify=False
    )
    response.raise_for_status()
    return response.json().get("reportId")

def verificar_status(token, report_id):
    while True:
        response = requests.get(
            f"{ENDPOINT}/reporting/status/{report_id}",
            headers={
                "Authorization": f"Bearer {token}",
            },
            verify=False
        )
        response.raise_for_status()
        status = response.json().get("status")
        if status == "SUCCEEDED":
            print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Geracao do Relatorio: {status}")
            return True
        elif status in ["PENDING", "PROCESSING"]:
            print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Geracao do Relatorio: {status} - Testar novamente")
            time.sleep(30)
        else:
            print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Erro na geracao do relatorio: {status}")
            return False

def baixar_relatorio(token, report_id, ARQUIVO):
    response = requests.get(
        f"{ENDPOINT}/reporting/download?reportId={report_id}",
        headers={"Authorization": f"Bearer {token}"},
        verify=False
    )
    response.raise_for_status()
    with open(ARQUIVO, "wb") as file:
        file.write(response.content)

def carregar_arquivo_no_banco(arquivo, database, server, user, password):
    print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Carga do Arquivo {arquivo}")
    connection = mysql.connector.connect(
        host=server,
        user=user,
        password=password,
        database=database,
        allow_local_infile=True
    )
    cursor = connection.cursor()
    cursor.execute(f"TRUNCATE TABLE {database}.{TABLE};")
    load_query = f"""
        LOAD DATA LOCAL INFILE '{arquivo}'
        INTO TABLE {TABLE}
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\\n'
        (@campo1, @campo2, @campo3)
        SET
        service_name = @campo1,
        service_lob = CONCAT(@campo2, @campo3),
        sigla = TRIM(SUBSTRING(@campo1, -3));
    """
    cursor.execute(f"""{load_query}""")
    cursor.execute(f"""
        UPDATE {TABLE}
        LEFT JOIN tb_lobs_desc
            ON {TABLE}.service_lob = tb_lobs_desc.lob
        SET {TABLE}.ROTINA_BIM = tb_lobs_desc.jobname;
    """)
    connection.commit()
    connection.close()


if __name__ == "__main__":
    token = gerar_token_api(ENDPOINT, VAR3, VAR4)
    report_id = gerar_relatorio(token, REPORT_NAME)

    if verificar_status(token, report_id):
        baixar_relatorio(token, report_id, ARQUIVO)
        
        with open(ARQUIVO, "r") as file:
            lines = file.readlines()
        
        with open(f"{ARQUIVO}.tmp", "w") as temp_file:
            for line in lines:
                if "SERVICE" not in line:
                    temp_file.write(line.replace('"', ''))
        
        carregar_arquivo_no_banco(f"{ARQUIVO}.tmp", DATABASE, SERVER, VAR1, VAR2)

    # Nota: 'directory' não está definido neste escopo, 
    # o script original pode depender de uma variável global ou de ambiente.
    # Vou assumir que o diretório é o mesmo do ARQUIVO.
    directory = "/PROD/POMG/FILE/" 
    pattern = os.path.join(directory, f"{JOBNAME}*")
    files_to_remove = glob.glob(pattern)

    for file_path in files_to_remove:
        try:
            os.remove(file_path)
            print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Arquivo removido: {file_path}")
        except Exception as e:
            print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Erro ao remover {file_path}: {e}")
