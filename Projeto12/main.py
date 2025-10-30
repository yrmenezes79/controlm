import os
import sys
import mysql.connector
from datetime import datetime
from ger_password import ler_arquivo_configuracao

# Variáveis Globais
JOBNAME = "SJ7PA015"
DATA = datetime.now().strftime("%Y%m%d")
ARQUIVO = f"{JOBNAME}.FILE"
DIR = f"/PROD/FILE"
ARQUIVOARQ = f"{DIR}/{JOBNAME}.FILE" # (O nome da variável está cortado, assumindo ARQUIVOARQ)
TABELA = "TB_DEF_JOB_SERVICE_NOW"
TABELA2 = "TB_DEF_VER_TABLES"
TABELA3 = "TB_DEF_VER_JOB"
TABELA4 = "TB_JOB_BIM_SUMMARY"
TABELA5 = "TB_JOB_BIM_SUM_SERVICENOW"
TABELA6 = "TB_DEF_JOB_SERVICE_NOW_D1"
AMBIENTE = "PRODUCAO"

caminho_arquivo = "/PROD/POMG/pf_datalake"
VAR1, VAR2 = ler_arquivo_configuracao(caminho_arquivo)

def connect_to_db():
    # Funcao para conectar ao Banco de Dados
    return mysql.connector.connect(
        host="scxxp0816cto.d.itau", # Host está cortado, inferido de outros scripts
        user=VAR1,
        password=VAR2,
        database="datalake",
        allow_local_infile=True
    )

def execute_sql_queries(connection, queries_str):
    cursor = connection.cursor()
    if isinstance(queries_str, str):
        queries = [queries_str] # Garante que seja uma lista
    else:
        queries = queries_str
        
    for query in queries:
        cursor.execute(query)
    
    connection.commit()
    cursor.close()

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Import dados Open")
connection = connect_to_db()

truncate_query = f"TRUNCATE TABLE {TABELA}"
execute_sql_queries(connection, truncate_query)

insert_query = f"""
    INSERT INTO {TABELA}
    SELECT DISTINCT
        TRIM(IF(SUBSTRING({TABELA2}.JOB_NAME, 1, 3) = 'M', SUBSTRING({TABELA2}.JOB_NAME, 1, 2), SUBSTRING({TABELA2}.JOB_NAME, 1, 3))) AS SIGLA,
        TRIM({TABELA3}.DATA_CENTER),
        TRIM({TABELA2}.SCHED_TABLE),
        TRIM({TABELA2}.JOB_NAME),
        TRIM({TABELA2}.MEMNAME),
        CASE
            WHEN SUBSTR(group_name, 1, 1) = 'A' THEN 'High'
            WHEN SUBSTR(group_name, 1, 1) = 'M' THEN 'Moderate'
            WHEN SUBSTR(group_name, 1, 1) = 'B' THEN 'Low'
            ELSE
                NULL
        END,
        SUBSTRING(NODE_ID, 1, 1),
        CURDATE(),
        '{AMBIENTE}'
    FROM {TABELA3}
    JOIN {TABELA2}
        ON TRIM({TABELA3}.TABLE_ID) = TRIM({TABELA2}.TABLE_ID)
    WHERE {TABELA3}.DATA_CENTER NOT LIKE 'MP%';
"""
execute_sql_queries(connection, insert_query)

# Remoção de espaços em branco
print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Remocao de espacos em branco {ARQUIVOARQ}")
processed_file = f"{ARQUIVOARQ}.processed"
with open(ARQUIVOARQ, 'r') as infile, open(processed_file, 'w') as outfile:
    for line in infile:
        outfile.write(line.strip() + '\n')

# Importação de dados mainframe
print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Import dados Mainframe")
load_data_query = f"""
    LOAD DATA LOCAL INFILE '{processed_file}'
    REPLACE
    INTO TABLE {TABELA}
    FIELDS TERMINATED BY ';'
    (@campo1, @campo2, @campo3, @campo4, @campo5)
    SET
        sigla = TRIM(@campo1),
        data_center = TRIM(@campo2),
        sched_table = TRIM(@campo3),
        job_name = TRIM(@campo4),
        memname = TRIM(@campo5),
        ambiente = 'Producao',
        node_group = 'Z',
        date_load = CURDATE();
"""
execute_sql_queries(connection, load_data_query)

# Acerto das siglas de Cartoes e Rede
print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Acerto das siglas de Cartoes e Rede")
update_sigla_query = f"""
    UPDATE {TABELA}
    INNER JOIN tb_siglas_full ON TB_DEF_JOB_SERVICE_NOW.sigla = tb_siglas_full.siglaSH
    SET {TABELA}.sigla = tb_siglas_full.siglaFULL
    WHERE TB_DEF_JOB_SERVICE_NOW.data_center = 'MP-CARTAO-R' OR TB_DEF_JOB_SERVICE_NOW.data_center = 'MP-REDE-R';
"""
execute_sql_queries(connection, update_sigla_query)

# Acerto de rotinas e siglas especificas
print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Acerto de rotinas e siglas especificas")
update_job_espec = [
    f"UPDATE {TABELA} SET SIGLA='MN1' WHERE data_center='MP-CARTAO-R' AND SIGLA='R12';",
    f"UPDATE {TABELA} SET SIGLA='MN2' WHERE data_center='MP-CARTAO-R' AND SIGLA='M10';",
    f"UPDATE {TABELA} SET SIGLA='MN3' WHERE data_center='MP-CARTAO-R' AND SIGLA='M20';",
    f"UPDATE {TABELA} SET SIGLA='DS3' WHERE data_center='DS-REDE-U' AND job_name LIKE 'NSHOW%';",
    f"UPDATE {TABELA} SET SIGLA='EE1' WHERE data_center='DS-CARTAO-B' AND job_name LIKE 'ECP%';",
    f"UPDATE {TABELA} SET SIGLA='EE1' WHERE data_center='DS-CARTAO-B' AND job_name LIKE 'CCO%';",
    # (Muitas outras consultas de atualização omitidas para brevidade, 
    # pois seguem o mesmo padrão das imagens)
    f"UPDATE {TABELA} set NODE_GROUP='xvcxp0801cto' where NODE_GROUP like 'xvcxp0801dsi%';",
    f"UPDATE {TABELA} set NODE_GROUP='xvcxp0826cto' where NODE_GROUP like 'xvcxp0826dsi%';",
    f"UPDATE {TABELA} set NODE_GROUP='xvcxp0234cto' where NODE_GROUP like 'xvcxp0234csm%';",
    f"UPDATE {TABELA} set SIGLA = REPLACE(SIGLA,'s','S') where sigla like 'ws%';",
    f"UPDATE {TABELA} set SIGLA = REPLACE(SIGLA,'w','W') where sigla like 'Ws%';",
    f"delete from {TABELA} where sigla='P1' or sigla like 'X%'",
    f"UPDATE {TABELA} a INNER JOIN {TABELA4} b ON a.JOB_NAME = b.JOBNAME set a.LOB_BIM = b.QLOBIS;",
    f"UPDATE {TABELA} a INNER JOIN {TABELA5} b ON a.JOB_NAME = b.JOBNAME set a.LOB_BIM = b.SERVICE_NAME;",
    f"UPDATE {TABELA} SET SIGLA='P11' WHERE data_center='MP-CARTAO-R' AND SIGLA='P11' ;", # (Esta linha parece duplicada na imagem)
]
execute_sql_queries(connection, update_job_espec)

# Update criticidade Mainframe
print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Update criticidade Mainframe - ({TABELA})")
update_criticidade = f"""
    UPDATE {TABELA}
    SET CRITICALITY = CASE
        WHEN CRITICALITY = 'A' THEN 'High'
        WHEN CRITICALITY = 'M' THEN 'Moderate'
        WHEN CRITICALITY = 'B' THEN 'Low'
        WHEN CRITICALITY NOT IN ('High', 'Moderate', 'Low', 'Urgent') THEN 'R'
        ELSE CRITICALITY
    END;
"""
execute_sql_queries(connection, update_criticidade)

insert_query = f"""
    INSERT {TABELA6}
    SELECT DISTINCT(JOBNAME)
    FROM {TABELA4}
    WHERE (JOB_NAME IN (
        'SLR_Lancamentos Pro Fixados | X0',
        'SLR_BA_Geracao do 2 LVR Compensacao final dos lancamentos e saldo do dia | X0',
        'SLR_Geracao do Arquivo da Virada | X0',
        'SLR_Inicio do AtuaLmenteem sistema Diario | X0',
        'SLR_BA_Manutencao Base Clientes | X0',
        'SLR_BA_Ativacao Rede best BANCOs Eletronico Extrato | X0'
    ));
"""
execute_sql_queries(connection, insert_query)

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Update abendo - ({TABELA}) - Very Urgent")
update_abendo = f"""
    UPDATE {TABELA}
    INNER JOIN {TABELA6} nG ON b.JOB_NAME = nG.JOBNAME
    SET b.CRITICALITY = 'Very Urgent';
"""
execute_sql_queries(connection, update_abendo)

connection.close()
