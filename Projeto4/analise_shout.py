import mysql.connector
from datetime import datetime
from delete_files import delete_files
from ler_password import ler_arquivo_configuracao
import subprocess

JOBNAME = "SJ7PA021"
DATABASE = "datalake"
caminho_shield_itau = "shield.itau"
LISTA_JOB = f"{JOBNAME}_SHOUT"
TABELA1 = "TB_SHOUT_SUM"
TABELA2 = "TB_SHOUT_SUM_TEMP"

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

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Insert dados {TABELA1}")

# List of queries to execute
queries = [
    f"TRUNCATE TABLE {TABELA1}",
    f"""
    INSERT INTO TB_SHOUT_SUM (
        Server_Name, Folder_Name, Job_Name, MEDIA_PROC, Notification_Time,
        Urgency, Message, END_TIME, Quant_Alert, Valor_Correto
    )
    SELECT
        TB_DEF_VER_TABLES.DATA_CENTER,
        TB_DEF_VER_JOB.PARENT_TABLE,
        TB_DEF_VER_JOB.JOB_NAME,
        TB_CMR_STATIS.MEDIA_PROC,
        TB_DEF_VER_SHOUT.SHOUT_TIME,
        TB_DEF_VER_SHOUT.URGENCY,
        TB_DEF_VER_SHOUT.MESSAGE,
        '0000-00-00 00:00:00',
        '0',
        '+03%'
    FROM TB_DEF_VER_SHOUT
    INNER JOIN TB_DEF_VER_JOB
        ON TB_DEF_VER_SHOUT.TABLE_ID = TB_DEF_VER_JOB.TABLE_ID
    INNER JOIN TB_DEF_VER_TABLES
        ON TB_DEF_VER_SHOUT.TABLE_ID = TB_DEF_VER_TABLES.TABLE_ID
    INNER JOIN TB_CMR_STATIS
        ON TB_CMR_STATIS.DATACENTER = TB_DEF_VER_TABLES.DATA_CENTER
        AND TB_CMR_STATIS.JOBNAME = TB_DEF_VER_JOB.JOB_NAME
    WHERE TB_DEF_VER_SHOUT.WHEN_COND = 'EXECUTING'
        AND TB_DEF_VER_TABLES.DATA_CENTER LIKE 'PRD%'
        AND TB_CMR_STATIS.MEDIA_PROC >= '00:30:00'
        AND TB_DEF_VER_SHOUT.SHOUT_TIME >= '+03%'
        AND TB_DEF_VER_SHOUT.SHOUT_TIME != '+070%'
        AND TB_CMR_STATIS.MEDIA_PROC IS NOT NULL
    """,
    f"""
    INSERT INTO TB_SHOUT_SUM (
        Server_Name, Folder_Name, Job_Name, MEDIA_PROC, Notification_Time,
        Urgency, Message, END_TIME, Quant_Alert, Valor_Correto
    )
    SELECT
        TB_DEF_VER_TABLES.DATA_CENTER,
        TB_DEF_VER_JOB.PARENT_TABLE,
        TB_DEF_VER_JOB.JOB_NAME,
        TB_CMR_STATIS.MEDIA_PROC,
        TB_DEF_VER_SHOUT.SHOUT_TIME,
        TB_DEF_VER_SHOUT.URGENCY,
        TB_DEF_VER_SHOUT.MESSAGE,
        '0000-00-00 00:00:00',
        '0',
        '+070%'
    FROM TB_DEF_VER_SHOUT
    INNER JOIN TB_DEF_VER_JOB
        ON TB_DEF_VER_SHOUT.TABLE_ID = TB_DEF_VER_JOB.TABLE_ID
    INNER JOIN TB_DEF_VER_TABLES
        ON TB_DEF_VER_SHOUT.TABLE_ID = TB_DEF_VER_TABLES.TABLE_ID
    INNER JOIN TB_CMR_STATIS
        ON TB_CMR_STATIS.DATACENTER = TB_DEF_VER_TABLES.DATA_CENTER
        AND TB_CMR_STATIS.JOBNAME = TB_DEF_VER_JOB.JOB_NAME
    WHERE TB_DEF_VER_SHOUT.WHEN_COND = 'EXECUTING'
        AND TB_DEF_VER_TABLES.DATA_CENTER LIKE 'PRD%'
        AND TB_CMR_STATIS.MEDIA_PROC >= '00:50:00'
        AND TB_DEF_VER_SHOUT.SHOUT_TIME >= '+070%'
        AND TB_DEF_VER_SHOUT.SHOUT_TIME != '+03%'
        AND TB_CMR_STATIS.MEDIA_PROC IS NOT NULL
    """,
    f"""
    INSERT INTO TB_SHOUT_SUM (
        Server_Name,
        Folder_Name,
        Job_Name,
        MEDIA_PROC,
        Notification_Time,
        Urgency,
        Message,
        END_TIME,
        Quant_Alert,
        Valor_Correto
    )
    SELECT
        TB_DEF_VER_TABLES.DATA_CENTER,
        TB_DEF_VER_JOB.PARENT_TABLE,
        TB_DEF_VER_JOB.JOB_NAME,
        '00:00:00',
        TB_DEF_VER_SHOUT.SHOUT_TIME,
        TB_DEF_VER_SHOUT.URGENCY,
        TB_DEF_VER_SHOUT.MESSAGE,
        '0000-00-00 00:00:00',
        '0',
        'OK'
    FROM TB_DEF_VER_SHOUT
    INNER JOIN TB_DEF_VER_JOB ON TB_DEF_VER_SHOUT.TABLE_ID = TB_DEF_VER_JOB.TABLE_ID
    INNER JOIN TB_DEF_VER_TABLES ON TB_DEF_VER_SHOUT.TABLE_ID = TB_DEF_VER_TABLES.TABLE_ID
    WHERE TB_DEF_VER_SHOUT.WHEN_COND = 'NOTOK'
    AND TB_DEF_VER_TABLES.DATA_CENTER LIKE 'PRD%'
    """,
    f"""
    INSERT INTO {TABELA2} (Server_Name, Job_Name, Quant_Alert)
    SELECT T1.Server_Name, T1.Job_Name, (TIME_TO_SEC(T1.END_TIME) - TIME_TO_SEC(T1.TIME_INICIO_PREV)) / 1.5 - 1800,
    ((TIME_TO_SEC(T1.END_TIME) - TIME_TO_SEC(T1.TIME_INICIO_PREV)) / 1.5 - 1800) / 60 AS Total_Minutes
    FROM TB_DEF_JOB_SUM_NOW T1
    INNER JOIN TB_DEF_JOB_SUM_NOW_D1 T2 ON T1.JOB_NAME = T2.JOB_NAME AND T1.Server_Name = T2.Server_Name
    INNER JOIN TB_DEF_JOB_SERVICE_EXEC T3 ON T1.JOB_NAME = T3.JOB_NAME AND T1.Server_Name = T3.Server_Name
    WHERE T3.END_TIME != '0000-00-00 00:00:00'
    AND T1.Server_Name LIKE 'PRD%'
    AND NOT EXISTS (
        SELECT 1 FROM TB_SHOUT_SUM_TEMP T4
        WHERE T4.JOB_NAME = T1.JOB_NAME AND T4.Server_Name = T1.Server_Name
    )
    """,
    f"""
    INSERT INTO {TABELA2} (Server_Name, Job_Name, Quant_Alert)
    SELECT TB_CMR_STATIS.DATACENTER, TB_CMR_STATIS.JOBNAME,
    (TIME_TO_SEC(TB_CMR_STATIS.ENDTIME) - TIME_TO_SEC(TB_CMR_STATIS.STARTTIME)) - TIME_TO_SEC(TB_CMR_STATIS.MEDIA_PROC)
    FROM TB_CMR_STATIS
    INNER JOIN TB_DEF_JOB_SERVICE_EXEC ON TB_CMR_STATIS.JOBNAME = TB_DEF_JOB_SERVICE_EXEC.JOB_NAME AND TB_CMR_STATIS.DATACENTER = TB_DEF_JOB_SERVICE_EXEC.Server_Name
    WHERE TB_DEF_JOB_SERVICE_EXEC.END_TIME != '0000-00-00 00:00:00'
    AND TB_CMR_STATIS.MEDIA_PROC != '00:00:00'
    AND TB_CMR_STATIS.STARTTIME != '00:00:00'
    AND TB_CMR_STATIS.ENDTIME != '00:00:00'
    AND TIME_FORMAT(TB_CMR_STATIS.ENDTIME, '%H:%i:%s') > '00:00:00'
    AND TB_CMR_STATIS.DATACENTER LIKE 'PRD%'
    AND (TIME_TO_SEC(TB_CMR_STATIS.ENDTIME) - TIME_TO_SEC(TB_CMR_STATIS.STARTTIME)) > TIME_TO_SEC(TB_CMR_STATIS.MEDIA_PROC)
    AND (TIME_TO_SEC(TB_CMR_STATIS.ENDTIME) - TIME_TO_SEC(TB_CMR_STATIS.STARTTIME)) BETWEEN TIME_TO_SEC(TB_CMR_STATIS.MEDIA_PROC) * 1.5 - 1800 AND TIME_FORMAT(TB_CMR_STATIS.ENDTIME, '%H:%i:%s') > TIME_FORMAT(TB_CMR_STATIS.STARTTIME, '%H:%i:%s')
    AND TB_CMR_STATIS.MEDIA_PROC IS NOT NULL;
    """
]

for query in queries:
    cursor.execute(query)
    db_connection.commit()

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Update dados {TABELA1}")

# update query
update_query = f"""
UPDATE {TABELA1} T1
INNER JOIN TB_DEF_JOB_SERVICE_EXEC T2
    ON T1.Job_Name = T2.Job_Name AND T1.Server_Name = T2.Server_Name
INNER JOIN TB_DEF_JOB_SERVICE_EXEC_D1 T3
    ON T1.Job_Name = T3.Job_Name AND T1.Server_Name = T3.Server_Name
SET T1.END_TIME = T2.END_TIME
WHERE T1.Valor_Correto != 'OK'
"""
cursor.execute(update_query)
db_connection.commit()

# update start time
insert_query = f"""
INSERT INTO TB_ALARM (Server_Name, Job_Name, Quant_Alert)
SELECT TB_ALARM.DATA_CENTER, TB_ALARM.JOB_NAME, COUNT(*)
FROM TB_ALARM
WHERE TB_ALARM.DATA_CENTER LIKE 'PRD%'
    AND TB_ALARM.WHEN_COND = 'NOTOK'
    AND DATE(TB_ALARM.HOST_TIME) >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY TB_ALARM.DATA_CENTER, TB_ALARM.JOB_NAME;
"""
cursor.execute(insert_query)
db_connection.commit()

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} update Quantidade alertas ultimos 30 dias - {TABELA1}")
update_query = f"""
UPDATE {TABELA1} t1
INNER JOIN (
    SELECT t2.Server_Name, t2.Job_Name, t3.Quant_Alert
    FROM {TABELA1} t2
    JOIN TB_ALARM t3
        ON t2.Job_Name = t3.Job_Name
        AND t2.Server_Name = t3.Server_Name
    WHERE t3.Quant_Alert >= 1
) t1_sub
ON t1.Job_Name = t1_sub.Job_Name
AND t1.Server_Name = t1_sub.Server_Name
SET t1.Quant_Alert = t1_sub.Quant_Alert
WHERE t1.END_TIME = '0000-00-00 00:00:00'
AND t1.Job_Name != '';
"""
cursor.execute(update_query)
db_connection.commit()

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Remoção de duplicatas - {TABELA1}")
dedup_query = f"""
WITH RankedJobs AS (
    SELECT
        id,
        server_name,
        folder_name,
        job_name,
        notification_time,
        ROW_NUMBER() OVER (
            PARTITION BY server_name, folder_name, job_name, notification_time
            ORDER BY id
        ) AS rn
    FROM {TABELA1}
)
DELETE FROM {TABELA1}
WHERE id IN (
    SELECT id
    FROM RankedJobs
    WHERE rn > 1
);
"""
cursor.execute(dedup_query)
db_connection.commit()

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Acerto de rotinas que rodam com mais de 999 minutos para +30% - {TABELA1}")
dedup_query = f"""
UPDATE {TABELA1}
SET Valor_Correto = '+30%'
WHERE CAST(REPLACE(valor_correto, '%', '') AS UNSIGNED) > 999;
"""
cursor.execute(dedup_query)
db_connection.commit()

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Delete de rotinas que estao corretas com +30% - {TABELA1}")
dedup_query = f"""
DELETE FROM {TABELA1}
WHERE valor_correto = Notification_Time;
"""
cursor.execute(dedup_query)
db_connection.commit()

cursor.close()
db_connection.close()

delete_files(JOBNAME)

if __name__ == "__main__":
    subprocess.run(["python", "analise_shout_report.py"])
