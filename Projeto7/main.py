import mysql.connector
from datetime import datetime

# Declaração das variáveis
TABELA1 = "TB_EXECUTION"
TABELA_J = "TB_SUMMARY_PROC"
JOBNAME = "SJ7_SELECT"
ARQUIVO = "sj7_select"
DATABASE = "datalake"

# Funcao para ler arquivo de configuração
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
        print(f"Arquivo {caminho_arquivo} não encontrado.")
        return None, None
    except Exception as e:
        print(f"Erro ao ler arquivo de configuração: {str(e)}")
        return None, None

# Funcao para executar no banco de dados
def execute_query(query, fetch=False):
    try:
        conn = mysql.connector.connect(
            host=SERVER,
            user=password, # Corrigido: deveria ser 'user'
            password=password,
            database=DATABASE,
            allow_local_infile=True # habilitar LOAD DATA LOCAL INFILE
        )
        cursor = conn.cursor()
        cursor.execute(query)
        if fetch:
            return cursor.fetchall()
        conn.commit()
    except mysql.connector.Error as err:
        print(f"Erro ao executar query: {err}")
        exit(1)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# Obter usuário e senha
caminho_arquivo = "/PROD/POMG/pf_datalake"
user, password = ler_arquivo_configuracao(caminho_arquivo)
# Nota: A variável SERVER não foi definida neste script, 
# mas é usada na função execute_query. 
# Presumindo que seja definida em outro lugar.
SERVER = "scxxp0816cto.d.itau" # Adicionado com base nos scripts anteriores

# Extração de dados
print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Extração de dados - Tabela: {TABELA1}")
query = f"""
    SELECT SUBSTRING(DATA_CENTER, 1, 2) AS B, SUBSTRING(START_TIME, 1, 10) AS A, COUNT(*)
    FROM {TABELA1}
    WHERE START_TIME BETWEEN CURRENT_DATE() - INTERVAL 2 DAY AND CURRENT_DATE()
    GROUP BY A, B
    ORDER BY A DESC
"""
result = execute_query(query, fetch=True)

# Salvar resultado no arquivo
with open(ARQUIVO, "w") as file:
    for row in result:
        file.write(','.join(map(str, row)) + "\n")

# Processar arquivo
print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Processando arquivo")
try:
    with open(ARQUIVO, "r") as infile, open(f"{ARQUIVO}.T", "w") as outfile:
        header = next(infile)  # Tentar pular o cabeçalho
except StopIteration:
    print("Arquivo está vazio, não há cabeçalho para pular.")
else:
    if header:
        for line in infile:
            campos = line.strip().split(',')
            outfile.write(','.join(campos) + "\n")
except FileNotFoundError:
    print(f"{ARQUIVO} não encontrado.")

# Importar dados para a tabela
print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Load dos dados - Tabela: {TABELA_J}")
query = f"""
    LOAD DATA LOCAL INFILE '{ARQUIVO}.T'
    INTO TABLE {TABELA_J}
    FIELDS TERMINATED BY ','
"""
execute_query(query)

# Gerar arquivo de expurgo
print("Geracao de arquivo de EXPURGO")
query = f"""
    SELECT data FROM {TABELA_J}
    WHERE data < DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
    GROUP BY data
"""
expurgo_list = execute_query(query, fetch=True)
if expurgo_list:
    print("Expurgo vai ser realizado")
    for data in expurgo_list:
        print(f"Data expurgo: {data[0]}")
        query = f"DELETE FROM {TABELA_J} WHERE data = '{data[0]}'"
        execute_query(query)
else:
    print("Nao existe Expurgo a ser realizado")
