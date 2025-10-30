import mysql.connector
from datetime import datetime
from ler_password import ler_arquivo_configuracao

# Database connection details
server = "scxxp0816cto.d.itau"
database = "datalake"

# Table names
TABELA1 = "TB_SHOUT_SUM"
TABELA2 = "TB_SHOUT"
TABELA3 = "TB_CMR_STATIS"
TABELA4 = "TB_DEF_JOB_SERVICE_EXEC"
TABELA_TEMP = "TB_SHOUT_SUM_TEMP"

caminho_arquivo = "/PROD/POMG/pf_datalake"
VAR1, VAR2 = ler_arquivo_configuracao(caminho_arquivo)
print(VAR1)

# Connect to the database
conn = mysql.connector.connect(
    host=server,
    user=VAR1,
    password=VAR2,
    database=database
)
cursor = conn.cursor()

# truncate and insert operations
truncate_insert_queries = [
    f"TRUNCATE TABLE {TABELA1}",
    f"""
    INSERT INTO {TABELA1} (
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
        {TABELA3}.Server_Name,
        {TABELA2}.Folder_Name,
        {TABELA2}.Job_Name,
        {TABELA3}.MEDIA_PROC,
        {TABELA2}.Notification_Time,
        {TABELA2}.Urgency,
        {TABELA2}.MESSAGE,
        '0000-00-00 00:00:00',
        '0',
        '+03%'
    FROM {TABELA2}
    INNER JOIN {TABELA3}
        ON {TABELA2}.Server_Name = {TABELA3}.DATACENTER
        AND {TABELA2}.Job_Name = {TABELA3}.JOBNAME
    WHERE {TABELA2}.when = 'EXECTIME'
        AND {TABELA2}.Server_Name LIKE 'DS%'
        AND {TABELA3}.MEDIA_PROC >= '00:30:00'
        AND {TABELA2}.Notification_Time != '>35'
        AND {TABELA2}.Notification_Time != '>035'
        AND {TABELA3}.MEDIA_PROC is not null;
    """,
    f"""
    INSERT INTO {TABELA1} (
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
        {TABELA3}.Server_Name,
        {TABELA2}.Folder_Name,
        {TABELA2}.Job_Name,
        {TABELA3}.MEDIA_PROC,
        {TABELA2}.Notification_Time,
        {TABELA2}.Urgency,
        {TABELA2}.MESSAGE,
        '0000-00-00 00:00:00',
        '0',
        '+070%'
    FROM {TABELA2}
    INNER JOIN {TABELA3}
        ON {TABELA2}.Server_Name = {TABELA3}.DATACENTER
        AND {TABELA2}.Job_Name = {TABELA3}.JOBNAME
    WHERE {TABELA2}.when = 'EXECTIME'
        AND {TABELA2}.Server_Name LIKE 'DS%'
        AND {TABELA3}.MEDIA_PROC >= '00:50:00'
        AND {TABELA2}.Notification_Time != '+070%'
        AND {TABELA2}.Notification_Time != '+070%'
        AND {TABELA3}.MEDIA_PROC is not null;
    """,
    f"""
    INSERT INTO {TABELA1} (
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
        {TABELA3}.Server_Name,
        {TABELA2}.Folder_Name,
        {TABELA2}.Job_Name,
        {TABELA3}.MEDIA_PROC,
        {TABELA2}.Notification_Time,
        {TABELA2}.Urgency,
        {TABELA2}.MESSAGE,
        '0000-00-00 00:00:00',
        '0',
        CONCAT('=', FLOOR((TIME_TO_SEC({TABELA3}.MEDIA_PROC) * 1.5 + 1800) / 60)) AS Total_Minutes
    FROM {TABELA2}
    INNER JOIN {TABELA3}
        ON {TABELA2}.Server_Name = {TABELA3}.DATACENTER
        AND {TABELA2}.Job_Name = {TABELA3}.JOBNAME
    WHERE {TABELA2}.when = 'EXECTIME'
        AND {TABELA2}.Server_Name LIKE 'DS%'
        AND {TABELA3}.MEDIA_PROC >= '01:30:00'
        AND {TABELA2}.Notification_Time not like '=%'
        AND TIME_FORMAT(SEC_TO_TIME(TIME_TO_SEC({TABELA3}.MEDIA_PROC)*1.5 + 1800), '%H:%i') not between TIME_FORMAT(SEC_TO_TIME({TABELA3}.MEDIA_PROC), '%H:%i') AND TIME_FORMAT(SEC_TO_TIME({TABELA3}.MEDI
        AND {TABELA3}.MEDIA_PROC is not null;
    """,
    f"""
    INSERT INTO {TABELA1} (
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
        {TABELA3}.Server_Name,
        {TABELA2}.Folder_Name,
        {TABELA2}.Job_Name,
        {TABELA3}.MEDIA_PROC,
        {TABELA2}.Notification_Time,
        {TABELA2}.Urgency,
        {TABELA2}.MESSAGE,
        '0000-00-00 00:00:00',
        '0',
        CONCAT('=', FLOOR((TIME_TO_SEC({TABELA3}.MEDIA_PROC) * 1.5 + 1800) / 60)) AS Total_Minutes
    FROM {TABELA2}
    INNER JOIN {TABELA3}
        ON {TABELA2}.Server_Name = {TABELA3}.DATACENTER
        AND {TABELA2}.Job_Name = {TABELA3}.JOBNAME
    WHERE {TABELA2}.when = 'EXECTIME'
        AND {TABELA2}.Server_Name LIKE 'OS'
        AND {TABELA3}.MEDIA_PROC >= '01:00:00'
        AND {TABELA2}.Notification_Time like '%>%'
        AND {TABELA3}.MEDIA_PROC is not null;
    """
]

for query in truncate_insert_queries:
    cursor.execute(query)

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} // update {TABELA1}")

update_queries = [
    f"""
    UPDATE {TABELA1}
    INNER JOIN {TABELA4}
        ON {TABELA1}.Job_Name = {TABELA4}.Job_Name
        AND {TABELA1}.Server_Name = {TABELA4}.Server_Name
        AND {TABELA4}.SCHED_TABLE = {TABELA1}.Folder_Name
    SET {TABELA1}.END_TIME = {TABELA4}.END_TIME
    WHERE {TABELA1}.Valor_Correto != 'OK'
        AND {TABELA1}.END_TIME = {TABELA4}.END_TIME;
    """,
]

for query in update_queries:
    cursor.execute(query)

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Delete rotinas duplicadas")

delete_duplicates_query = f"""
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

cursor.execute(delete_duplicates_query)
conn.commit()
cursor.close()
conn.close()

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} update ultima exec {TABELA1}")
