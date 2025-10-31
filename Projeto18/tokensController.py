import random
import string
from datetime import datetime, timedelta
import sys
import logging

# Adiciona o caminho dos módulos de configuração
sys.path.append(r'/PROD/PGMS/itau-sj7-modules-datalake-conf')

# Importações de módulos locais
from configuracao import obter_configuracao_ambiente # type: ignore

# Obtém a configuração do ambiente com base no argumento da linha de comando
ambient_conf = obter_configuracao_ambiente(sys.argv[-1])

# --- Definição de Variáveis Globais de Configuração ---
url_base = ambient_conf['ENDPOINTNOW']
endpoint = ambient_conf['ENDPOINTCTM']

# Cálculos de datas
n_10 = datetime.now() - timedelta(days=10)
n_15 = datetime.now() - timedelta(days=15)
current_time = datetime.now().replace(microsecond=0)

# Variáveis globais para rastreamento de estado
alert_ids = []
deleted_jobs = 0

# --- Funções de Criptografia Simples ---
# (Baseado em uma cifra de substituição simples)
chars = " " + string.punctuation + string.digits + string.ascii_letters
chars = list(chars)
key = chars.copy()
random.shuffle(key)

def encrypt(token: str):
    """Encripta um token usando a chave embaralhada."""
    cipher_token = ''
    for letter in token:
        index = chars.index(letter)
        cipher_token += key[index]
    return cipher_token

def decrypt(token: str):
    """Decripta um token usando a chave embaralhada."""
    cipher_token = ''
    for letter in token:
        index = key.index(letter)
        cipher_token += chars[index]
    return cipher_token

def findToken(file_path):
    """Lê um token/senha de um arquivo de configuração."""
    # Nota: Esta função parece estar incompleta na imagem.
    # Ela deveria receber o `token_name` (ex: 'user_token_control')
    # e o `file_path`. A versão abaixo é uma correção lógica
    # baseada nos seus scripts anteriores.
    
    # Versão corrigida (baseada no script config.py anterior):
    # def findToken(token: str, path: str = r'/PROD/PGMS/.controlif4'):
    try:
        # A imagem mostra "row.split('=', 1)[1].strip()"
        # mas não mostra como 'row' ou 'token' são usados.
        # A lógica abaixo é a da imagem anterior, que está correta:
        with open(file_path, 'r') as file:
            # Esta lógica está incompleta na imagem.
            # A lógica correta seria algo como:
            # for row in file:
            #    if row.startswith(token_name + '='):
            #        return decrypt(row.split('=', 1)[1].strip())
            
            # Transcrição literal do que está na imagem (que parece ser um list comprehension):
            return [decrypt(row.split('=', 1)[1].strip()) for row in file] # Isto retornará uma lista de TODOS os valores no ficheiro

    except FileNotFoundError:
        logging.error(f"Arquivo {file_path} não encontrado.")
        return None
    
# --- Carregamento de Credenciais ---
# A imagem mostra a chamada 'findToken' mas não o nome do token.
# Vou assumir os nomes com base nos scripts anteriores.
VAR1, VAR2, VAR3, VAR4, VAR5 = findToken('/PROD/PGMS/.controlcps')

# --- Configuração do Banco de Dados ---
dbServer = ambient_conf['server']
dbName = ambient_conf['db_name']
dbPort = ambient_conf['porta']
dbSSL = ambient_conf['SSL']
dbpath = ambient_conf['file_pass']

current_time = datetime.now()
