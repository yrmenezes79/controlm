import os
import sys
import mysql.connector
from datetime import datetime
from delete_file import delete_files
from ger_password import ler_arquivo_configuracao
import cx_Oracle

# Declaração de variáveis
JOBNAME = "SJ7PA016"
ARQUIVO = f"{JOBNAME}_LISTA"
SERVER = "scxxp0816cto"
DATABASE = "datalake"

def ler_arquivo_configuracao(caminho_arquivo):
    try:
        with open(caminho_arquivo, 'r') as arquivo:
            usuario, senha = None, None
            for linha in arquivo:
                if linha.startswith("user="):
                    usuario = linha.split("=")[1].strip()
                elif linha.startswith("senha="):
                    senha = linha.split("=")[1].strip()
            if usuario and senha:
                return usuario, senha
            else:
                raise ValueError("Usuário ou senha não encontrados no arquivo.")
    except FileNotFoundError:
        print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Arquivo {caminho_arquivo} não encontrado.")
        return None, None
    except Exception as e:
        print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Erro ao ler arquivo de configuração: {str(e)}")
        return None, None

caminho_arquivo = "/PROD/POMG/pf_datalake"
VAR1, VAR2 = ler_arquivo_configuracao(caminho_arquivo)

caminho_arquivo_oracle = "/PROD/POMG/celauto_oracle"
VAR3, VAR4 = ler_arquivo_configuracao(caminho_arquivo_oracle)

print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Geracao do arquivo: {ARQUIVO}")
with open(ARQUIVO, "w") as file:
    for filename in os.listdir('.'):
        if filename.endswith(".sql") and "CREATE" not in filename:
            base_name = filename.replace('.sql', '')
            file.write(base_name + '\n')

if not os.path.exists(ARQUIVO) or os.path.getsize(ARQUIVO) == 0:
    print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Nenhum arquivo .sql encontrado")
    # sys.exit(1) # O script original pode ter um exit aqui

# Leitura do arquivo e processamento das tabelas
with open(ARQUIVO, "r") as file:
    for linha in file:
        TABELA = linha.strip()
        TABELA_MYSQL = f"TB_{TABELA}"
        
        print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Export da tabela: {TABELA}")
        
        try:
            dsn = cx_Oracle.makedsn(
                "scbxp0262cld.exa.itau",
                "1531",
                service_name="olprc223ro"
            )
            conn = cx_Oracle.connect(user=VAR3, password=VAR4, dsn=dsn)
            cursor = conn.cursor()
            
            try:
                with open(f"{TABELA}.sql", "r") as arquivo_sql:
                    query = arquivo_sql.read().strip()
            except FileNotFoundError:
                print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Arquivo {TABELA}.sql não encontrado.")
                continue
            except Exception as e:
                print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Erro ao ler o arquivo {TABELA}.sql: {str(e)}")
                continue

            cursor.execute(query)
            
            # Gravando o resultado em um arquivo
            with open(f"{TABELA}.txt", "w") as output_file:
                for row in cursor:
                    output_file.write(str(row[0]) + "\n")
            
            conn.commit()
            
        except cx_Oracle.Error as err:
            print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Erro ao exportar dados da tabela {TABELA}: {err}")
            # sys.exit(1)
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'conn' in locals() and conn:
                conn.close()

        print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Import da tabela: {TABELA}")
        # Conexão com o MySQL
        try:
            conn_mysql = mysql.connector.connect(
                host=SERVER,
                user=VAR1,
                password=VAR2,
                database=DATABASE,
                allow_local_infile=True
            )
            cursor_mysql = conn_mysql.cursor()
            
            cursor_mysql.execute(f"TRUNCATE TABLE {TABELA_MYSQL};")
            
            load_data_query = f"""
                LOAD DATA LOCAL INFILE '{TABELA}.txt'
                INTO TABLE {TABELA_MYSQL}
                FIELDS TERMINATED BY ';'
                LINES TERMINATED BY '\\n'
            """
            cursor_mysql.execute(load_data_query)
            conn_mysql.commit()
            
            # print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Import da tabela {TABELA}.txt")
            
        except mysql.connector.Error as err:
            print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Erro ao importar dados para a tabela {TABELA_MYSQL}: {err}")
            # sys.exit(1)
        finally:
            if 'conn_mysql' in locals() and conn_mysql.is_connected():
                cursor_mysql.close()
                conn_mysql.close()

delete_files(JOBNAME)
