$ cat extracao_stack.py
import pandas as pd
import mysql.connector
import os
from datetime import datetime
from ger_password import ler_arquivo_configuracao
import pandas as pd
from statsmodels.tsa.seasonal import seasonal_decompose

directory = "csv"
data_atual = datetime.now()
limite_exec=80
data_formatada = data_atual.strftime("%Y-%m-%d")

if os.path.exists(directory):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            # Verifica se é um arquivo ou link simbólico
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)  # Remove o arquivo ou link
                print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Limpeza {file_path}: concluída")
        except Exception as e:
            print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Erro ao excluir {file_path}: {e}")
else:
    print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} O diretório {directory} não existe.")


caminho_arquivo = "/PROD/POMG/pf_datalake"
VAR1, VAR2 = ler_arquivo_configuracao(caminho_arquivo)

DB_CONFIG = {
    "host": "scxxp0816cto",
    "port": 3306,
    "user": VAR1,
    "password": VAR2,
    "database": "datalake",
}

conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor(dictionary=True)
print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Extracao de dados")

# Nota: A query SQL parece ter uma lógica mista de formatação de string
# (para limite_exec) que pode ser um erro de digitação. 
# Vou transcrever como está, mas idealmente usaria um placeholder.
cursor.execute(f"SELECT job_name FROM TB_EXECUTION_MODELO WHERE job_name not like 'A%' AND job_name not like 'CM%' GROUP BY job_name HAVING COUNT(*) > {limite_exec}")
job_names = [row["job_name"] for row in cursor.fetchall()]

print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Encontrados {len(job_names)} job_names distintos")

for job_name in job_names:
    query = "SELECT * FROM TB_EXECUTION_MODELO WHERE job_name = %s order by start_time"
    cursor.execute(query, (job_name,))
    rows = cursor.fetchall()
    df = pd.DataFrame(rows)
    # Novamente, uma formatação de string mista.
    if len(df) > int(f"{limite_exec}"): # Corrigido para uma comparação de inteiros
        filename = f"csv/{job_name}.csv"
        df.to_csv(filename, index=False, encoding="utf-8")
        print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Extracao de arquivo - {job_name}.csv")
        print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} {filename} - {len(df)} registros")
    else:
        print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} {job_name} - Nenhum arquivo gerado (registros: {len(df)})")

cursor.close()
conn.close()

print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Processo de extracao de arquivo concluido")

def analisar_rotinas_batch(pasta_series, arquivo_saida, period=7, corte_sazonal=20):
    resultados = []
    for arquivo in os.listdir(pasta_series):
        if arquivo.endswith(".csv"):
            caminho_arquivo = os.path.join(pasta_series, arquivo)
            try:
                df = pd.read_csv(
                    caminho_arquivo,
                    header=None, # Assumindo que o CSV não tem cabeçalho na releitura
                    names=[
                        # Os nomes das colunas precisam corresponder ao SELECT *
                        # Esta parte pode precisar de ajuste
                        "DATA_CENTER",
                        "JOB_NAME",
                        "START_TIME",
                        "END_TIME",
                        "AVAREGE_RUNTIME",
                        "TIPO", # Esta coluna 'TIPO' vem do SELECT *
                    ],
                )
            except Exception as e:
                print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Erro ao ler o arquivo {arquivo}: {e}")
                continue

            try:
                df["DURATION"] = pd.to_timedelta(
                    df["AVAREGE_RUNTIME"], errors="coerce"
                ).dt.total_seconds()
            except Exception as e:
                print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Erro ao converter a coluna 'AVAREGE_RUNTIME' no arquivo {arquivo}: {e}")
                continue
            
            df = df.dropna(subset=["DURATION"])
            if df.empty:
                print(
                    f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} O arquivo {arquivo} não possui dados válidos após a remoção de valores ausentes."
                )
                continue

            df["DATE"] = pd.to_datetime(df["START_TIME"]).dt.date
            # Agrupa por dia e calcula a média da duração
            df_diario = df.groupby("DATE")["DURATION"].mean().reset_index()

            # Verificar se há observações suficientes para decomposição
            if len(df_diario) >= 2 * period:
                # Decompor a série temporal
                try:
                    decomposicao = seasonal_decompose(
                        df_diario["DURATION"], model="additive", period=period
                    )
                    
                    # Calcular a variância dos componentes
                    variancia_sazonal = decomposicao.seasonal.var()
                    variancia_tendencia = decomposicao.trend.var()
                    variancia_ruido = decomposicao.resid.var()
                    variancia_total = df_diario["DURATION"].var()

                    # Calcular o percentual da variância explicada pela sazonalidade
                    sazonal_percentual = (variancia_sazonal / variancia_total) * 100

                    # Determinar se a série é sazonal ou linear
                    tipo = "Sazonal" if sazonal_percentual > corte_sazonal else "Linear"

                    # Adicionar aos resultados
                    resultados.append(
                        {
                            "job_name": df["JOB_NAME"].iloc[0],
                            "tipo": tipo,
                            "sazonal_percentual": sazonal_percentual,
                            "variancia_sazonal": variancia_sazonal,
                            "variancia_tendencia": variancia_tendencia,
                            "variancia_ruido": variancia_ruido,
                            "variancia_total": variancia_total,
                        }
                    )
                except Exception as e:
                    print(
                        f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Erro ao decompor a série temporal no arquivo {arquivo}: {e}"
                    )
            else:
                print(
                    f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} O arquivo {arquivo} não possui observações suficientes para decomposição."
                )

    # Criar um DataFrame com os resultados
    if resultados:
        df_resultados = pd.DataFrame(resultados)
        # Arredondar colunas numéricas para 2 casas decimais
        colunas_numericas = [
            "sazonal_percentual",
            "variancia_sazonal",
            "variancia_tendencia",
            "variancia_ruido",
            "variancia_total",
        ]
        df_resultados[colunas_numericas] = df_resultados[colunas_numericas].round(2)
        # Salvar os resultados em um arquivo CSV
        df_resultados.to_csv(arquivo_saida, index=False)
        print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Resultados salvos em {arquivo_saida}")
    else:
        print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Nenhum resultado foi gerado. Verifique os arquivos de entrada.")


pasta_series = "./csv/"
arquivo_saida = f"SAIDA_{data_formatada}.csv"
analisar_rotinas_batch(pasta_series, arquivo_saida, period=7, corte_sazonal=20)
$
