import subprocess
import mysql.connector
from datetime import datetime, timedelta
import paramiko
import os
import glob
import sys

sys.path.append('/PROD/POMG/itau-sj7-modules-datalake-conf')
from configuracao import obter_configuracao_ambiente
from ler_password import ler_arquivo_configuracao

if len(sys.argv) != 2:
    print(f"Uso: python main.py <ambiente>")
    sys.exit(1)

amb = sys.argv[1].upper()

try:
    ambiente = amb
    config = obter_configuracao_ambiente(ambiente)
    print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Configurações para o ambiente {ambiente}")
except ValueError as e:
    print(e)
    sys.exit(1)

VAR1, VAR2 = ler_arquivo_configuracao(config["file_pass"])

if not VAR1 or not VAR2:
    print(f"ERRO: Usuário ou senha não encontrados no arquivo de configuração.")
    sys.exit(1)

JOBNAME = "SJ7PA013"
DIRETORIO = "/PROD/FILE"
DATA = datetime.now().strftime("%Y%m")
ARQUIVO = "arq_alter_jobs_alterados_{DATA}.csv"
TABELA = "TB_ALTER_JOB"

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Copia de arquivo - {ARQUIVO}")

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(config["server"], port=config["porta"], username=VAR1, password=VAR2)

sftp = ssh.open_sftp()
try:
    sftp.get(f'/c/manager/auxilib/Central/{ARQUIVO}', f'/PROD/FILE/{ARQUIVO}')
    print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Arquivo copiado com sucesso")
except FileNotFoundError:
    print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Nao existe arquivo {ARQUIVO}")
    sys.exit(0)
finally:
    sftp.close()
    ssh.close()

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Import Tabela - {TABELA}")

# Conexão com o banco de dados
db_connection = mysql.connector.connect(
    host=config["server"],
    user=VAR1,
    password=VAR2,
    database=config["db_name"],
    port=config["porta"],
    allow_local_infile=True,
    ssl_disabled=not config["SSL"]
)
cursor = db_connection.cursor()

try:
    cursor.execute(f"""
        LOAD DATA LOCAL INFILE '/PROD/FILE/{ARQUIVO}'
        INTO TABLE {TABELA}
        FIELDS TERMINATED BY ';'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES
    """)
    db_connection.commit()
except mysql.connector.Error as err:
    print(f"Erro: {err}")

print("Geracao de arquivo de EXPURGO")
# Gerar lista de expurgo
cursor.execute(f"""
    SELECT
        data_operacao
    FROM {TABELA}
    WHERE data_operacao < DATE_SUB(CURRENT_DATE(), INTERVAL 730 DAY)
    GROUP BY data_operacao
""")

expurgo_list = cursor.fetchall()

if expurgo_list:
    print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Expurgo vai ser realizado")
    for (data_operacao,) in expurgo_list:
        print(f"EXPURGO: {data_operacao}")
        cursor.execute(f"DELETE FROM {TABELA} WHERE data_operacao = '{data_operacao}'")
        db_connection.commit()
else:
    print("nao existe Expurgo a ser realizado")

# Fechar conexão
cursor.close()
db_connection.close()

pattern = os.path.join(DIRETORIO, f"{ARQUIVO}")
files_to_remove = glob.glob(pattern)

for file_path in files_to_remove:
    try:
        os.remove(file_path)
        print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Arquivo removido: {file_path}")
    except Exception as e:
        print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Erro ao remover {file_path}: {e}")
