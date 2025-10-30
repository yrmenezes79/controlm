import mysql.connector
from datetime import datetime
from delete_file import delete_files
from ger_password import ler_arquivo_configuracao

server = 'scxxp0816cto.d.itau'
database = 'datalake'

tabela1 = 'TB_DEF_JOB_SERVICE_EXEC'
tabela2 = 'TB_EXECUTION'
tabela3 = 'TB_DEF_JOB_SERVICE_NOW'
tabelas = 'TB_DEF_JOB_SERVICE_EXEC'

caminho_arquivo = "/PROD/POMG/pf_datalake"
VAR1, VAR2 = ler_arquivo_configuracao(caminho_arquivo)

conn = mysql.connector.connect(
    host=server,
    user=VAR1,
    password=VAR2,
    database=database,
    allow_local_infile=True
)

cursor = conn.cursor()

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Insert Tabela (tabela4)")
cursor.execute(f"TRUNCATE TABLE {tabelas}")
cursor.execute(f"""
    INSERT INTO {tabelas} (
        SIGLA, DATA_CENTER, SCHED_TABLE, JOB_NAME, MEMNAME, CRITICALITY,
        NODE_GROUP, LOB_BIN, date_load, AMBIENTE, END_TIME
    )
    SELECT
        SIGLA, DATA_CENTER, SCHED_TABLE, JOB_NAME, MEMNAME, CRITICALITY,
        NODE_GROUP, LOB_BIN, date_load, AMBIENTE, '0000-00-00 00:00:00'
    FROM {tabela3}
    WHERE DATA_CENTER NOT LIKE 'MP%'
""")

conn.commit()

# Generate job list
print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} update (tabela1)")
cursor.execute(f"SELECT DATA_CENTER, JOB_NAME FROM {tabela3} WHERE DATA_CENTER NOT LIKE 'MP%'")
job_list = cursor.fetchall()

for datacenter, jobname in job_list:
    cursor.execute(f"""
        UPDATE {tabela1} AS exec
        JOIN (
            SELECT DATA_CENTER, JOB_NAME, MAX(END_TIME) as latest_end_time
            FROM {tabela2}
            WHERE DATA_CENTER = %s AND JOB_NAME = %s
            GROUP BY DATA_CENTER, JOB_NAME
        ) AS latest_exec
            ON exec.DATA_CENTER = latest_exec.DATA_CENTER
            AND exec.JOB_NAME = latest_exec.JOB_NAME
        SET exec.END_TIME = latest_exec.latest_end_time
    """, (datacenter, jobname))
    conn.commit()

cursor.close()
conn.close()

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Final do processo")
