import datetime
import mysql.connector
from mysql.connector import Error

def ler_arquivo_configuracao(caminho_arquivo):
    usuario = None
    senha = None
    try:
        with open(caminho_arquivo, 'r') as arquivo:
            for linha in arquivo:
                if linha.startswith("user="):
                    usuario = linha.split("=")[1].strip()
                elif linha.startswith("senha="):
                    senha = linha.split("=")[1].strip()
        return usuario, senha
    except FileNotFoundError:
        print(f"Arquivo {caminho_arquivo} não encontrado.")
        return None, None

def get_user_pass_from_file(caminho_arquivo):
    try:
        usuario, senha = ler_arquivo_configuracao(caminho_arquivo)
        if not usuario or not senha:
            raise Exception("Usuário ou senha não encontrados no arquivo.")
    except Exception as e:
        print(f"Exception during read User and Password: {str(e)}")
        return ("user", "password", None) # Retorna defaults ou None

    return usuario, senha

def execute_query_with_reconnect(conn_config, query, max_retries=3):
    for attempt in range(max_retries):
        try:
            conn = mysql.connector.connect(**conn_config)
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            conn.close()
            return
        except Error as e:
            print(f"Erro ao executar query: {e}")
            if attempt < max_retries - 1:
                print("Tentando reconectar...")
            else:
                print("Falha após várias tentativas.")

caminho_arquivo = "/PROD/POMG/pf_datalake"
user, password = get_user_pass_from_file(caminho_arquivo)

config = {
    'user': user,
    'password': password,
    'host': 'scxxp0816cto',
    'database': 'datalake',
}

print(datetime.now(), "Truncate - TB_JOB_BIM_SUMMARY")
query = "TRUNCATE TABLE TB_JOB_BIM_SUMMARY"
execute_query_with_reconnect(config, query)

print(datetime.now(), "Insert - ", "TB_JOB_BIM_SUMMARY", "DS")
query = """
    INSERT INTO TB_JOB_BIM_SUMMARY (DATA_CENTER, JOBNAME, QLOBIS)
    (SELECT DATA_CENTER, JOBNAME, COUNT(DISTINCT SERVICE_NAME) FROM TB_JOB_BIM WHERE DATA_CENTER LIKE 'DS%' GROUP BY DATA_CENTER, JOBNAME);
"""
execute_query_with_reconnect(config, query)

print(datetime.now(), "Insert - ", "TB_JOB_BIM_SUMMARY", "WS")
query = """
    INSERT INTO TB_JOB_BIM_SUMMARY (DATA_CENTER, JOBNAME, QLOBIS)
    (SELECT DATA_CENTER, JOBNAME, COUNT(DISTINCT SERVICE_NAME) FROM TB_JOB_BIM WHERE DATA_CENTER LIKE 'WS%' GROUP BY DATA_CENTER, MEMNAME);
"""
execute_query_with_reconnect(config, query)

print(datetime.now(), "Update - ", "TB_JOB_BIM_SUMMARY", " + ")
query = """
    UPDATE TB_JOB_BIM_SUMMARY T1
    JOIN
    (
        SELECT TMP.JOB_NAME, TMP.DATA_CENTER, MAX(END_TIME) as max_end_time
        FROM (SELECT * FROM TB_EXECUTION UNION ALL SELECT * FROM TB_JOB_BIM_SUMMARY) as TMP
        INNER JOIN TB_EXECUTION ON TMP.JOBNAME = TB_EXECUTION.JOBNAME
        INNER JOIN TB_JOB_BIM_SUMMARY ON TMP.JOBNAME = TB_JOB_BIM_SUMMARY.JOBNAME
        group by TMP.JOB_NAME, TMP.DATA_CENTER
    ) AS SUMMARY_MAX
    ON SUMMARY_MAX.JOBNAME = T1.JOB_NAME
    SET T1.DATA_EXEC = SUMMARY_MAX.max_end_time
    WHERE T1.DATA_CENTER LIKE 'DS%';
"""
execute_query_with_reconnect(config, query)

print(datetime.now(), "Update - ", "TB_JOB_BIM_SUMMARY", "T")
query = """
    UPDATE TB_JOB_BIM_SUMMARY JOIN TB_DEF_VER_JOB ON TB_JOB_BIM_SUMMARY.JOBNAME = TB_DEF_VER_JOB.JOB_NAME
    SET TB_JOB_BIM_SUMMARY.CYCLIC = TB_DEF_VER_JOB.CYCLIC;
"""
execute_query_with_reconnect(config, query)
