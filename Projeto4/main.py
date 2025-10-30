import requests
import mysql.connector
import time
import os
import subprocess
import re
from datetime import datetime, timedelta
from ler_password import ler_arquivo_configuracao
from ger_relatorio import gerar_relatorio
from delete_file import delete_files

# variables
JOBNAME = "SJ7PA017"
SERVER = "scxxp0816cto.d.itau"
ARQUIVO = f"{JOBNAME}_FILE"
ARQUIVO2 = f"{JOBNAME}_LOAD"
ARQUIVO3 = f"{ARQUIVO}_DS"
ARQUIVO4 = f"{ARQUIVO}_AR"
ARQUIVO5 = f"{JOBNAME}_MAX"
ARQUIVO6 = f"{JOBNAME}_SHOUT"
ARQUIVO7 = f"{JOBNAME}_EXTRA"
TABELA = "TB_EXECUTION"
TABELA2 = "TB_EXECUTION_ULT"
TABELA3 = "TB_DEF_JOB_SERVICE_NOW"
TABELA4 = "TB_SHOUT"
TABELA5 = "TB_EXECUTION_30_OPEN"

DATA_UP = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")

caminho_arquivo = "/PROD/POMG/pf_datalake"
VAR1, VAR2 = ler_arquivo_configuracao(caminho_arquivo)

caminho_arquivo = "/PROD/POMG/cps049c"
VAR3, VAR4 = ler_arquivo_configuracao(caminho_arquivo)

gerar_relatorio(
    endpoint="https://vcxx148cto:8443/automation-api",
    nome_relatorio="Jobs_Executions_D1_ds",
    usuario = VAR3,
    senha = VAR4,
    arquivo_saida=ARQUIVO3
)

gerar_relatorio(
    endpoint="https://vcxx148cto:8443/automation-api",
    nome_relatorio="Jobs_Executions_d1_ar",
    usuario = VAR3,
    senha = VAR4,
    arquivo_saida=ARQUIVO4
)

gerar_relatorio(
    endpoint="https://vcxx148cto:8443/automation-api",
    nome_relatorio="Job_definitions_parameters_notifications",
    usuario = VAR3,
    senha = VAR4,
    arquivo_saida=ARQUIVO6
)

def combine_files(file1, file2, output_file):
    with open(file1, 'r') as f1, open(file2, 'r') as f2, open(output_file, 'w') as outfile:
        # Write contents of the first file
        for line in f1:
            outfile.write(line)
        
        # Write contents of the second file
        for line in f2:
            outfile.write(line)

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Tratamento de arquivo {ARQUIVO3} e {ARQUIVO4} para {ARQUIVO}")
combine_files(ARQUIVO3, ARQUIVO4, ARQUIVO)

# Funcao para remover virgulas dentro das datas
def remove_commas_in_dates(line):
    # Encontrar datas no formato "May 20, 2025"
    return re.sub(r'(")(\w+\s\d{1,2}),(\s\d{4}")', r'\1\2\3', line)

# Abrir o arquivo de entrada e saida
with open(ARQUIVO, 'r') as infile, open(ARQUIVO2, "w") as outfile:
    for line in infile:
        processed_line = remove_commas_in_dates(line)
        outfile.write(processed_line)

# Connect to MySQL
db_connection = mysql.connector.connect(
    host=SERVER,
    user=VAR1,
    password=VAR2,
    database="datalake",
    allow_local_infile=True
)
cursor = db_connection.cursor()

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Import {ARQUIVO2} para base {TABELA}")
load_query = f"""
    LOAD DATA LOCAL INFILE '{ARQUIVO2}'
    REPLACE
    INTO TABLE {TABELA}
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\\n'
    IGNORE 1 LINES
    (@campo1,@campo2,@campo3,@campo4,@campo5,@campo6,@campo7,@campo8,@campo9,@campo10,@campo11,@campo12,@campo13)
    SET
    DATA_CENTER=TRIM(@campo1),
    SCHED_TABLE=TRIM(@campo2),
    APPLICATION=TRIM(@campo3),
    GROUP_NAME=TRIM(@campo4),
    NODE_ID=TRIM(@campo5),
    JOB_NAME=TRIM(@campo6),
    ODATE = DATE_FORMAT(STR_TO_DATE(REPLACE(@campo7, ',', ''), '%b %d %Y'), '%Y-%m-%d'),
    START_TIME = STR_TO_DATE(REPLACE(@campo8, ',', ''), '%b %d %Y %H:%i:%s %p'),
    END_TIME = STR_TO_DATE(REPLACE(@campo9, ',', ''), '%b %d %Y %H:%i:%s %p'),
    RERUN_COUNTER=TRIM(@campo10),
    COMPLETION_STATUS=TRIM(@campo11),
    AVERAGE_CPU_TIME=TRIM(@campo12),
    ORDER_ID=TRIM(@campo13),
    AVAREGE_RUNTIME=TIMEDIFF(end_time,start_time)
"""
cursor.execute(load_query)
db_connection.commit()

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Import {ARQUIVO} para base {TABELA5}")
load_query = f"""
    LOAD DATA LOCAL INFILE '{ARQUIVO}'
    REPLACE
    INTO TABLE {TABELA5}
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\\n'
    IGNORE 1 LINES
    (@campo1,@campo2,@campo3,@campo4,@campo5,@campo6,@campo7,@campo8,@campo9,@campo10,@campo11,@campo12,@campo13)
    SET
    DATA_CENTER=TRIM(@campo1),
    SCHED_TABLE=TRIM(@campo2),
    APPLICATION=TRIM(@campo3),
    GROUP_NAME=TRIM(@campo4),
    NODE_ID=TRIM(@campo5),
    JOB_NAME=TRIM(@campo6),
    ODATE = DATE_FORMAT(STR_TO_DATE(REPLACE(@campo7, ',', ''), '%b %d %Y'), '%Y-%m-%d'),
    START_TIME = STR_TO_DATE(REPLACE(@campo8, ',', ''), '%b %d %Y %H:%i:%s %p'),
    END_TIME = STR_TO_DATE(REPLACE(@campo9, ',', ''), '%b %d %Y %H:%i:%s %p'),
    RERUN_COUNTER=TRIM(@campo10),
    COMPLETION_STATUS=TRIM(@campo11),
    AVERAGE_CPU_TIME=TRIM(@campo12),
    ORDER_ID=TRIM(@campo13),
    AVAREGE_RUNTIME=TIMEDIFF(end_time,start_time)
"""
cursor.execute(load_query)
db_connection.commit()

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Delete MP e CREATED - {TABELA5}")
load_query = f"""
DELETE FROM {TABELA5} WHERE DATA_CENTER LIKE 'MP%' OR SCHED_TABLE = 'CREATED'
"""
cursor.execute(load_query)
db_connection.commit()

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Expurgo {TABELA5}")
load_query = f"""
DELETE FROM {TABELA5}
WHERE END_TIME < NOW() - INTERVAL 30 DAY
"""
cursor.execute(load_query)
db_connection.commit()

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} update base mysql {TABELA2}")
# update MySQL
update_query = f"""
    UPDATE {TABELA}
    (SELECT
        TMP.ORDER_ID,
        TMP.JOB_NAME,
        TMP.RERUN_COUNTER,
        TIMEDIFF(TMP.END_TIME, TMP.START_TIME) AS MTTR
    FROM
        (SELECT
            TNO.END_TIME, ORDER_ID, JOB_NAME, RERUN_COUNTER
        FROM
            {TABELA}
        WHERE ODATE >= DATE_SUB(NOW(), INTERVAL 10 DAY)
        AND COMPLETION_STATUS = 'Ended Not OK') AS TMP
    INNER JOIN
        (SELECT
            START_TIME, ORDER_ID, JOB_NAME, RERUN_COUNTER
        FROM {TABELA}
        WHERE ODATE >= DATE_SUB(NOW(), INTERVAL 10 DAY)
        AND {TABELA}.COMPLETION_STATUS = 'Ended Not OK'
        ) AS TMP_2 ON TMP.ORDER_ID = TMP_2.ORDER_ID
        AND TMP_2.RERUN_COUNTER < TMP_1.RERUN_COUNTER
        AND TMP.RERUN_COUNTER = TMP_1.RERUN_COUNTER + 1
        AND TMP.MTTR.JOB_NAME = {TABELA}.JOB_NAME
    )
    SET {TABELA}.MTTR = TMP.MTTR.MTTR
    WHERE ODATE >= DATE_SUB(NOW(), INTERVAL 10 DAY) AND {TABELA}.COMPLETION_STATUS = 'Ended Not OK';
"""
cursor.execute(update_query)
db_connection.commit()

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Update base mysql {TABELA} - Max")
# Generate max file
max_query = f"""
    SELECT T1.DATA_CENTER, T1.JOB_NAME, T1.end_time
    FROM {TABELA} T1
    INNER JOIN (
        SELECT JOB_NAME, MAX(END_TIME) AS max_end_time
        FROM {TABELA}
        GROUP BY JOB_NAME
    ) T2 ON T1.JOB_NAME = T2.JOB_NAME AND T1.end_time = T2.max_end_time
"""
cursor.execute(max_query)
max_data = cursor.fetchall()

with open(ARQUIVO5, 'w') as file:
    for row in max_data:
        file.write(','.join(map(str, row)) + "\n")

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Truncate {TABELA2}")
# Load max file into Table 2
truncate_query = f"TRUNCATE TABLE {TABELA2}"
cursor.execute(truncate_query)

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Load {ARQUIVO5} para tabela {TABELA2}")
load_max_query = f"""
    LOAD DATA LOCAL INFILE '{ARQUIVO5}'
    REPLACE
    INTO TABLE {TABELA2}
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\\n'
    IGNORE 1 LINES
    (@campo1,@campo2,@campo3,@campo4)
    SET
    DATA_CENTER=TRIM(@campo1),
    JOB_NAME=TRIM(@campo2),
    END_DATE=TRIM(@campo3),
    END_TIME=TRIM(@campo4)
"""
cursor.execute(load_max_query)
db_connection.commit()

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Truncate - {TABELA4}")
print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Load arquivo {ARQUIVO6} paa base {TABELA4}")

try:
    cursor.execute(f"TRUNCATE TABLE {TABELA4}")
    load_query = f"""
        LOAD DATA LOCAL INFILE '{ARQUIVO6}'
        REPLACE
        INTO TABLE {TABELA4}
        FIELDS TERMINATED BY ';'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES
    """
    cursor.execute(load_query)
    
    update_queries = [
        f"UPDATE {TABELA4} SET WHEN = 'EXECTIME' WHERE `WHEN` = 'Job execution time';",
        f"UPDATE {TABELA4} SET WHEN = 'LATESUB' WHERE `WHEN` = 'Job not submitted by spec';",
        f"UPDATE {TABELA4} SET WHEN = 'LATEEXEC' WHERE `WHEN` = 'Job not finished by speci'"
    ]
    
    for query in update_queries:
        cursor.execute(query)
    
    db_connection.commit()

except mysql.connector.Error as err:
    print(f"Erro ao executar consultas: {err}")

finally:
    cursor.close()
    db_connection.close()

delete_files(JOBNAME)
