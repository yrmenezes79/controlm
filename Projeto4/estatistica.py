import mysql.connector
import time
import re
from datetime import datetime, timedelta
from ler_password import ler_arquivo_configuracao
from ger_relatorio import gerar_relatorio
from delete_file import delete_files

# Variables
JOBNAME = "SJ7PA020"
caminho_shield = f"/c/scj/pasd/log"
ARQUIVO = f"{JOBNAME}_ESTATIC"
ARQUIVO2 = f"{ARQUIVO}_DS"
TABELA = "TB_CMR_STATIS"

caminho_arquivo = "/PROD/POMG/pf_datalake"
VAR1, VAR2 = ler_arquivo_configuracao(caminho_arquivo)

caminho_arquivo = "/PROD/POMG/cps049c"
VAR3, VAR4 = ler_arquivo_configuracao(caminho_arquivo)

gerar_relatorio(
    endpoint="https://vcxx148cto:8443/automation-api",
    nome_relatorio="Jobs_Executions_D1_estatistica_ds",
    usuario = VAR3,
    senha = VAR4,
    arquivo_saida=ARQUIVO2
)

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Retirando duplicade {ARQUIVO2}")
with open(f"{ARQUIVO2}", 'r') as file:
    lines = file.readlines()

unique_sorted_lines = sorted(set(lines))

with open(f"{ARQUIVO}", 'w') as output_file:
    output_file.writelines(unique_sorted_lines)

# Conexão com o MySQL
db_connection = mysql.connector.connect(
    host=SERVER,
    user=VAR1,
    password=VAR2,
    database="datalake",
    allow_local_infile=True
)
cursor = db_connection.cursor()

load_query = f"""
    LOAD DATA LOCAL INFILE '{ARQUIVO}'
    REPLACE INTO TABLE {TABELA}
    FIELDS TERMINATED BY ';'
    LINES TERMINATED BY '\\n'
    IGNORE 1 LINES
    (@campo1, @campo2, @campo3, @campo4)
    SET DATACENTER = trim(@campo3),
        JOBNAME = trim(@campo2),
        MEDIA_PROC = TIME_FORMAT(SEC_TO_TIME(@campo4), '%H:%i:%s');
"""
cursor.execute(load_query)
db_connection.commit()

delete_files(JOBNAME)
