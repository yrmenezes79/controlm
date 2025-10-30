import datetime
import mysql.connector
import os
import sys
from datetime import datetime
import time

sys.path.append('/PROD/POMG/itau-sj7-modules-datalake-conf')
from configuracao import obter_configuracao_ambiente
from ler_password import ler_arquivo_configuracao

TABLE = "TB_DEF_JOB_SERVICE_NOW_INC"
TABLE2 = "TB_DEF_JOB_SERVICE_NOW"
TABLE3 = "TB_DEF_JOB_SERVICE_NOW_D1"
TABLE4 = "TB_DEF_JOB_SERVICE_NOW_SUM"
TABLE5 = "TB_DEF_SIGLA_SUM"

if len(sys.argv) != 2:
    print(f"Uso: python main.py <ambiente>")
    sys.exit(1)

amb = sys.argv[1].upper()

try:
    ambiente = amb
    config = obter_configuracao_ambiente(ambiente)
    print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Configurações para o ambiente {ambien")
except ValueError as e:
    print(e)
    sys.exit(1)

user, password = ler_arquivo_configuracao(config["file_pass"])

if not user or not password:
    print(f"ERRO: Usuário ou senha não encontrados no arquivo de configuração.")
    sys.exit(1)

config = {
    "host": config["server"],
    "user": user,
    "password": password,
    "database": config["db_name"],
    "port": config["porta"],
    "allow_local_infile": True,
    "ssl_disabled": not config["SSL"]
}

try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Truncate Table")
    cursor.execute(f"TRUNCATE TABLE {TABLE}")
    conn.commit()

    print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Insert {TABLE} I")
    cursor.execute(f"INSERT INTO {TABLE}( SELECT '*' FROM {TABLE2} AS A WHERE NOT EXISTS ( SELECT * FROM {TABLE3} AS B WHERE TRIM(A.JOB_NAME) = TRIM(B.JOB_NAME)))")
    conn.commit()

    print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Insert {TABLE} D")
    cursor.execute(f"INSERT INTO {TABLE}( SELECT '*' FROM {TABLE3} AS A WHERE NOT EXISTS ( SELECT * FROM {TABLE2} AS B WHERE A.JOB_NAME = B.JOB_NAME)))")
    conn.commit()

    print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Truncate {TABLE3}")
    cursor.execute(f"TRUNCATE TABLE TB_DEF_JOB_SERVICE_NOW_D1")
    conn.commit()

    print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Insert {TABLE3}")
    cursor.execute(f"INSERT INTO TB_DEF_JOB_SERVICE_NOW_D1 (SELECT * FROM TB_DEF_JOB_SERVICE_NOW)")
    conn.commit()

    print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Insert {TABLE4}")
    cursor.execute(f"INSERT INTO {TABLE4}( SELECT * FROM TB_DEF_JOB_SERVICE_NOW_INC")
    conn.commit()

    print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Insert {TABLE5}")
    cursor.execute(f'''INSERT INTO {TABLE5} (
        SELECT SUBSTRING(DATA_CENTER, 1, 2) AS B, SIGLA, COUNT(*), CURDATE() AS A
        FROM {TABLE}
        GROUP BY B, SIGLA''')
    conn.commit()

    cursor.execute(f"SELECT COUNT(*) FROM TB_DEF_JOB_SERVICE_NOW_INC")
    result = cursor.fetchone()
    print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Trava de segurança para quantidade de rotinas acima de 5000 - Valor real:", result[0])
    if result[0] > 5000:
        print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Executando TRUNCATE na tabela TB_DEF_JOB_SERVICE_NOW_INC")
        cursor.execute(f"TRUNCATE TABLE TB_DEF_JOB_SERVICE_NOW_INC")
        conn.commit()

except mysql.connector.Error as err:
    print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Erro ao conectar ou executar comandos no banco de dados: {err}")
    sys.exit(1)

if 'cursor' in locals():
    cursor.close()
if 'conn' in locals() and conn.is_connected():
    conn.close()
