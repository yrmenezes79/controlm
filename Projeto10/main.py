import datetime
import mysql.connector
import os

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
        print(f"Arquivo {caminho_arquivo} nao encontrado.")
        return None, None

def get_user_pass_from_file(caminho_arquivo):
    try:
        usuario, senha = ler_arquivo_configuracao(caminho_arquivo)
        if usuario is None or senha is None:
            raise Exception("Usuario ou senha nao encontrados no arquivo.")
    except Exception as e:
        print(f"Exception during read User and Password: {str(e)}")
        return ("user", "password", None) # Retorna defaults ou None

    return usuario, senha

caminho_arquivo = "/PROD/POMG/pf_datalake"
user, password = get_user_pass_from_file(caminho_arquivo)

config = {
    'user': user,
    'password': password,
    'host': 'scxxp0816cto',
    'database': 'datalake',
}

try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    print(datetime.now(), "Truncate - TB_JOB_BIM_SUMMARY")
    cursor.execute("TRUNCATE TABLE TB_JOB_BIM_SUMMARY")
    conn.commit()

    print(datetime.now(), "Insert - ", "TB_JOB_BIM_SUMMARY", "DS")
    cursor.execute("""
        INSERT INTO TB_JOB_BIM_SUMMARY (DATA_CENTER, JOBNAME, QLOBIS)
        (SELECT DATA_CENTER, JOBNAME, COUNT(DISTINCT SERVICE_NAME)
         FROM TB_JOB_BIM WHERE DATA_CENTER LIKE 'DS%'
         GROUP BY DATA_CENTER, JOBNAME);
    """)
    conn.commit()

    print(datetime.now(), "Insert - ", "TB_JOB_BIM_SUMMARY", "WS")
    cursor.execute("""
        INSERT INTO TB_JOB_BIM_SUMMARY (DATA_CENTER, JOBNAME, QLOBIS)
        (SELECT DATA_CENTER, MEMNAME, COUNT(DISTINCT SERVICE_NAME)
         FROM TB_JOB_BIM WHERE DATA_CENTER LIKE 'WS%'
         GROUP BY DATA_CENTER, MEMNAME);
    """)
    conn.commit()

    print(datetime.now(), "Update - ", "TB_JOB_BIM_SUMMARY", " + ")
    cursor.execute("""
        UPDATE TB_JOB_BIM_SUMMARY
        JOIN (
            SELECT TMP.JOB_NAME, MAX(TB_EXECUTION.END_TIME) AS max_end_time
            FROM TB_JOB_BIM_SUMMARY AS TMP
            INNER JOIN TB_EXECUTION ON TMP.JOB_NAME = TB_EXECUTION.JOB_NAME
            GROUP BY TMP.JOB_NAME, TB_EXECUTION.JOB_NAME
        ) AS summary_max
            ON summary_max.JOB_NAME = TB_JOB_BIM_SUMMARY.JOBNAME
        SET TB_JOB_BIM_SUMMARY.DATA_EXEC = summary_max.max_end_time
        WHERE TB_JOB_BIM_SUMMARY.DATA_CENTER LIKE 'DS%';
    """)
    conn.commit()

    print(datetime.now(), "Update - ", "TB_JOB_BIM_SUMMARY", " Z")
    cursor.execute("""
        UPDATE TB_JOB_BIM_SUMMARY
        JOIN TB_DEF_VER_JOB ON TB_JOB_BIM_SUMMARY.JOBNAME = TB_DEF_VER_JOB.JOB_NAME
        SET TB_JOB_BIM_SUMMARY.CYCLIC = TB_DEF_VER_JOB.CYCLIC;
    """)
    conn.commit()

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
