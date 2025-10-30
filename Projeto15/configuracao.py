import time
from datetime import datetime

def obter_configuracao_ambiente(ambiente):
    ambiente = ambiente.upper()
    print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Ambiente selecionado {ambiente}")
    if ambiente == "PRD":
        return {
            "server": "prod",
            "file_pass": "/PROD/POMG/pf_datalake",
            "db_name": "datalake",
            "porta": 3306,
            "SSL": True,
            "ENDPOINTCTM": "https://prod:8443/automation-api",
            "ENDPOINTSNOW": "https://prod.service-now.com/api"
        }
    elif ambiente == "HOM":
        return {
            "server": "dev",
            "file_pass": "/PROD/POMG/pf_datalake_aws_dev",
            "db_name": "batchlake",
            "porta": 8033,
            "SSL": False,
            "ENDPOINTCTM": "https://hom:8443/automation-api",
            "ENDPOINTSNOW": "https://hom.service-now.com/api"
        }
    elif ambiente == "DEV":
        return {
            "server": "dev",
            "file_pass": "/PROD/POMG/pf_datalake_aws_dev",
            "db_name": "batchlake",
            "porta": 8033,
            "SSL": False,
            "ENDPOINTCTM": "https://dev:8443/automation-api",
            "ENDPOINTSNOW": "https://dev.service-now.com/api"
        }
    else:
        raise ValueError("Ambiente inválido! Use PRD, DEV ou HOM.")
