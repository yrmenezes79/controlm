import pandas as pd
import os
import sys
from datetime import datetime
import mysql.connector

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
        return ("user", "password", None)
    
    return usuario, senha

caminho_arquivo = "/PROD/POMG/pf_datalake"
user, password = get_user_pass_from_file(caminho_arquivo)

mydb = mysql.connector.connect(
  host="scxxp0816cto.d.itau",
  user=user,
  password=password,
  database="datalake"
)

mycursor = mydb.cursor()

mycursor.execute("""
    SELECT
        TB_JOB_BIM_SUMMARY.DATA_CENTER,
        TB_JOB_BIM_SUMMARY.JOBNAME,
        TB_DEF_VER_JOB.GROUP_NAME,
        TB_JOB_BIM_SUMMARY.QLOBIS
    FROM
        TB_JOB_BIM_SUMMARY
    INNER JOIN
        TB_DEF_VER_JOB
    ON
        TB_JOB_BIM_SUMMARY.JOBNAME = TB_DEF_VER_JOB.JOB_NAME
    WHERE
        TB_JOB_BIM_SUMMARY.QLOBIS > '0'
        AND TB_JOB_BIM_SUMMARY.DATA_CENTER LIKE 'DS-%'
        AND SUBSTRING(TB_DEF_VER_JOB.GROUP_NAME, 1, 1) != 'A'
    ORDER BY
        TB_JOB_BIM_SUMMARY.QLOBIS DESC;
""")

myresult = mycursor.fetchall()

filename = 'resultado.txt'
with open(filename, 'w') as file:
    for row in myresult:
        file.write(f'{row}\n')

if myresult:
    print("Existem rotinas que estao com criticidade errada, favor verificar")
    with open(filename, 'r') as file:
        print(file.read())
    sys.exit(10) # Retorna código 10
else:
    print('O arquivo esta vazio.')
