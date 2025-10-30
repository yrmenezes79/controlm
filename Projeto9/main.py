import os
import sys
import subprocess
import mysql.connector
from datetime import datetime
from ger_password import ler_arquivo_configuracao

# Variáveis
JOBNAME = "SJ7PA018"
SERVER = "scxxp0816cto.d.itau"
DATABASE = "datalake"
DIRFILE = "/PROD/FILE"
LISTA = f"{JOBNAME}_LISTA"
TABELA = "datalake.TB_JOB_BIM"

caminho_arquivo = "/PROD/POMG/pf_datalake"
VAR1, VAR2 = ler_arquivo_configuracao(caminho_arquivo)

# Funcao para carregar dados no MySQL
def funcao_load(arquivo, data_car_formatted):
    try:
        conn = mysql.connector.connect(
            host=SERVER,
            user=VAR1,
            password=VAR2,
            database=DATABASE,
            allow_local_infile=True
        )
        cursor = conn.cursor()

        # Load data para a tabela
        load_data_query = f"""
            LOAD DATA LOCAL INFILE '{DIRFILE}/{arquivo}.TMP'
            INTO TABLE {TABELA}
            FIELDS TERMINATED BY ';'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            SET DATA_CAR = '{data_car_formatted}';
        """
        cursor.execute(load_data_query)
        conn.commit()

        print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Dados carregados com sucesso na tabela {TABELA}")
    except mysql.connector.Error as err:
        print(f"Erro ao carregar dados para a tabela {TABELA}: {err}")
        sys.exit(1)
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# Listar arquivos e processar
arquivos = subprocess.getoutput(f"ls -ltr {DIRFILE} | grep BIM_BANCO_PROD | grep -v TMP | awk '{{print $9}}'")
if not arquivos:
    print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Arquivo {LISTA} VAZIO")
else:
    for arquivo in arquivos.splitlines():
        
        print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Inicio Tratamento Arquivo: {arquivo}")
        tmp_file = f"{DIRFILE}/{arquivo}.TMP"
        subprocess.run(f"cat {DIRFILE}/{arquivo} | sort | uniq -u | sed 's/\"//g' > {tmp_file}", shell=True)
        
        # Nota: O código shell para verificar erros (if [ $? -ne 0 ]) foi omitido por não ser Python.

        data_car = arquivo.split('.')[1]
        data_car_formatted = f"{data_car[0:4]}-{data_car[4:6]}-{data_car[6:8]}"
        
        print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Data de carregamento: {data_car_formatted}")
        print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Carregando arquivo: {arquivo}")
        
        quant = subprocess.getoutput(f"cat {DIRFILE}/{arquivo}.TMP | wc -l")
        print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Quantidade de registros {quant}")
        print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Inicio do load do arquivo: {arquivo}")
        
        funcao_load(arquivo, data_car_formatted)
        os.remove(tmp_file)

# Expurgo
print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Inicio do processo de expurgo")
connection = mysql.connector.connect(host=SERVER, user=VAR1, password=VAR2, database=DATABASE)
try:
    cursor = connection.cursor()
    cursor.execute(f"SELECT DISTINCT(data_car) FROM {TABELA} WHERE data_car < SUBDATE(CURDATE(), 39);")
    lista_expurgo = [row[0] for row in cursor.fetchall()]
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()

if not lista_expurgo:
    print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Arquivo de EXPURGO VAZIO")
else:
    for data in lista_expurgo:
        print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} EXPURGO - {data}")
        connection = mysql.connector.connect(host=SERVER, user=VAR1, password=VAR2, database=DATABASE)
        try:
            cursor = connection.cursor()
            cursor.execute(f"DELETE FROM {TABELA} WHERE data_car = '{data}';")
            connection.commit()
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

# Carga tabela TB_JOB_BIM_SERVICENOW_LOB
print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Carga tabela TB_JOB_BIM_SERVICENOW_LOB")
connection = mysql.connector.connect(host=SERVER, user=VAR1, password=VAR2, database=DATABASE)
try:
    cursor = connection.cursor()
    cursor.execute("TRUNCATE TABLE TB_JOB_BIM_SERVICENOW_LOB;")
    cursor.execute(f"""
        INSERT INTO TB_JOB_BIM_SERVICENOW_LOB
            (SELECT DISTINCT SERVICE_NAME, JOBNAME
            FROM {TABELA}
            ***)
    """)
    connection.commit()
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
