import subprocess
import mysql.connector
from datetime import datetime, timedelta

# Configurações iniciais
JOBNAME = "SJ7PA010"
SERVER = "scxxp0816cto.d.itau"
DATA = (datetime.now() - timedelta(days=0)).strftime("%Y-%m-%d")
TABELA1 = "TB_EXECUTION_SERVICENOW_TEMP"
TABELA_TEMP = "TB_EXECUTION"
TABELA_FINAL = "TB_EXECUTION_SERVICENOW"

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

# Obter usuário e senha
caminho_arquivo = "/PROD/POMG/pf_datalake"
VAR1, VAR2 = ler_arquivo_configuracao(caminho_arquivo)

# Funcao para executar comandos shell
def run_command(command):
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Erro ao executar comando: {command}")
        print(result.stderr)
        exit(1)
    return result.stdout.strip()

# Conexão com o banco de dados
def execute_query(query, params=None):
    try:
        conn = mysql.connector.connect(
            host=SERVER,
            user=VAR1,
            password=VAR2,
            database="datalake"
        )
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Erro ao executar query: {err}")
        exit(1)

# Queries SQL
queries = [
    {
        "description": "Replace Base Open sem cartao e rede",
        "query": f"""
            REPLACE INTO {TABELA_FINAL} (data, ambiente, data_center, sigla, status, volume)
            SELECT
                SUBSTRING(END_TIME, 1, 10) AS Data,
                SUBSTRING(DATA_CENTER, 1, 2) AS Plataforma,
                DATA_CENTER,
                IF(SUBSTRING(JOB_NAME, 1, 3) = 'M', SUBSTRING(JOB_NAME, 1, 2), SUBSTRING(JOB_NAME, 1, 3)) AS SIGLA,
                completion_status,
                COUNT(*) AS volume
            FROM {TABELA1}
            WHERE (DATA_CENTER, 1, 2) = 'M' AND data_center != 'M-REDE-R' AND data_center != 'M-CARTAO-R'
            AND SUBSTRING(END_TIME, 1, 10) = '{DATA}'
            GROUP BY SUBSTRING(END_TIME, 1, 10), SUBSTRING(DATA_CENTER, 1, 2), data_center,
            IF(SUBSTRING(JOB_NAME, 1, 3) = 'M', SUBSTRING(JOB_NAME, 1, 2), SUBSTRING(JOB_NAME, 1, 3)), completion_status;
        """
    },
    {
        "description": "Replace Base MP Banco sem cartao e rede",
        "query": f"""
            REPLACE INTO {TABELA_FINAL} (data, ambiente, data_center, sigla, status, volume)
            SELECT
                SUBSTRING(END_TIME, 1, 10) AS Data,
                IF(DATA_CENTER = 'MP', 1, 2) AS Plataforma,
                DATA_CENTER,
                IF(SUBSTRING(JOB_NAME, 1, 3) = 'P', SUBSTRING(JOB_NAME, 1, 2), SUBSTRING(JOB_NAME, 1, 3)) AS SIGLA,
                completion_status,
                COUNT(*) AS volume
            FROM {TABELA1}
            WHERE SUBSTRING(DATA_CENTER, 1, 2) = 'MP'
            AND data_center != 'MP-CARTAO-R' AND data_center != 'MP-REDE-R'
            AND SUBSTRING(END_TIME, 1, 10) = '{DATA}'
            GROUP BY SUBSTRING(END_TIME, 1, 10), SUBSTRING(DATA_CENTER, 1, 2), data_center,
            IF(SUBSTRING(JOB_NAME, 1, 3) = 'P', SUBSTRING(JOB_NAME, 1, 2), SUBSTRING(JOB_NAME, 1, 3)), completion_status;
        """
    }
]
# Adicione as demais queries aqui seguindo o mesmo formato

# Executar as queries
for q in queries:
    print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} {q['step']} {q['description']}")
    execute_query(q["query"])

print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Fim do processo")
