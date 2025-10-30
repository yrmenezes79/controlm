import mysql.connector
from datetime import datetime, timedelta
from ler_password import ler_arquivo_configuracao

TABELA = "TB_SHOUT_SUM"
TABELA2 = "TB_EXECUTION_30_OPEN"
SERVER = "scxxp0816cto.d.itau"

caminho_arquivo = "/PROD/POMG/pf_datalake"
VAR1, VAR2 = ler_arquivo_configuracao(caminho_arquivo)


def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host=SERVER,
            user=VAR1,
            password=VAR2,
            database="datalake"
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Erro ao conectar ao banco de dados: {err}")
        return None


def convert_to_time(minutes):
    """Converte minutos para o formato de tempo (HH:MM:SS)."""
    hours = minutes // 60
    remaining_minutes = minutes % 60
    return f"{hours:02}:{remaining_minutes:02}:00"


def apply_percentage_to_time(time_obj, percentage):
    """Aplica um percentual ao tempo."""
    total_seconds = time_obj.total_seconds()
    additional_seconds = total_seconds * (percentage / 100)
    new_total_seconds = total_seconds + additional_seconds
    hours = new_total_seconds // 3600
    remaining_seconds = new_total_seconds % 3600
    minutes = remaining_seconds // 60
    seconds = remaining_seconds % 60
    return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"


def process_data():
    """Processa os dados da tabela TB_SHOUT_NEW."""
    connection = connect_to_database()
    if not connection:
        return

    try:
        cursor = connection.cursor(dictionary=True)
        # Consulta os dados da tabela
        cursor.execute(f"SELECT id, valor_Correto, MEDIA_PROC FROM {TABELA}")
        rows = cursor.fetchall()

        for row in rows:
            valor_correto = row["valor_Correto"]
            media_proc = row["MEDIA_PROC"]  # Este é um objeto timedelta
            id = row["id"]

            # processa o campo Valor_Correto
            if valor_correto.startswith("="):
                # caso o valor seja em minutos
                minutes = int(valor_correto[1:])
                result = convert_to_time(minutes)
            elif valor_correto.startswith("+"):
                # caso o valor seja um percentual
                percentage = int(valor_correto[1:-1])  # Remove o "+" e "%"
                result = apply_percentage_to_time(media_proc, percentage)
            else:
                result = None  # Formato inválido

            print(f"ID: {id}, New_Time: {result}")
            # Atualiza o New_Time no banco de dados
            update_query = f"UPDATE {TABELA} SET New_Time = %s WHERE id = %s"
            cursor.execute(update_query, (result, id))

        # Adiciona a query para atualizar o campo Calc_Shout
        update_shout_ant_query = f"""
        UPDATE {TABELA} AS tsn
        SET tsn.calc_shout = (
            SELECT COUNT(*)
            FROM {TABELA2} AS teo
            WHERE teo.JOB_NAME = tsn.JOB_NAME
                AND tsn.SERVER_NAME = teo.DATA_CENTER
                AND tsn.New_Time < teo.AVAREGE_RUNTIME
        )
        """
        cursor.execute(update_shout_ant_query)
        print("Campo Calc_Shout atualizado com sucesso.")

        # Confirma as alterações no banco de dados
        connection.commit()
    except mysql.connector.Error as err:
        print(f"Erro ao executar a consulta: {err}")
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    process_data()
