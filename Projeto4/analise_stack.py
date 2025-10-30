$ cat analise_stack.py
import mysql.connector
from datetime import datetime
from delete_file import delete_files
from ler_password import ler_arquivo_configuracao
import subprocess

DATABASE = "datalake"
SERVER = "scxxp0816cto.d.itau"
TABELA = "TB_SHOUT_SUM"
TABELA2 = "TB_EXECUTION_MODELO"

caminho_arquivo = "/PROD/POMG/pf_datalake"
VAR1, VAR2 = ler_arquivo_configuracao(caminho_arquivo)

db_connection = mysql.connector.connect(
    host=SERVER,
    user=VAR1,
    password=VAR2,
    database=DATABASE,
    allow_local_infile=True
)
cursor = db_connection.cursor()

print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Insert dados {TABELA2}")
# List of queries to execute
queries = [
    f"TRUNCATE TABLE {TABELA2};",
    f"""
    INSERT INTO TB_EXECUTION_MODELO (
        DATA_CENTER,
        JOB_NAME,
        START_TIME,
        END_TIME,
        AVAREGE_RUNTIME
    )
    SELECT
        TB_EXECUTION.DATA_CENTER,
        TB_SHOUT_SUM.JOB_NAME,
        TB_EXECUTION.START_TIME,
        TB_EXECUTION.END_TIME,
        TB_EXECUTION.AVAREGE_RUNTIME
    FROM TB_EXECUTION
    RIGHT JOIN TB_SHOUT_SUM
        ON TB_EXECUTION.DATA_CENTER = TB_SHOUT_SUM.Server_Name
        AND TB_EXECUTION.JOB_NAME = TB_SHOUT_SUM.Job_Name
        AND TB_EXECUTION.SCHED_TABLE = TB_SHOUT_SUM.Folder_Name
    WHERE TB_EXECUTION.COMPLETION_STATUS = 'Ended OK'
        AND TB_EXECUTION.DATA_CENTER LIKE 'DS%'
        AND TB_EXECUTION.START_TIME >= DATE_SUB(NOW(), INTERVAL 60 DAY);
    """,
    f"""
    UPDATE TB_EXECUTION_MODELO
    SET AVAREGE_RUNTIME = SEC_TO_TIME(FLOOR(RAND() * (58 - 50 + 1) + 50))
    WHERE AVAREGE_RUNTIME <= '00:24:00';
    """
]
for query in queries:
    cursor.execute(query)
    db_connection.commit()

print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Final insert {TABELA2}")

db_connection.close()
$
