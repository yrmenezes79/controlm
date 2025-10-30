import cx_Oracle
import mysql.connector
import os

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

def export_to_csv(host, port, schema, service_name, file_password, datacenter):
    # Conexão com o Oracle
    caminho_arquivo = file_password
    user, password = ler_arquivo_configuracao(caminho_arquivo)
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    conn_oracle = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    
    query = f"""
        SELECT
            '{datacenter}' || ';' ||
            GRPNAME || ';' ||
            NODGRP
        FROM {schema}.CMS_NODGRP
    """
    
    try:
        cursor_oracle = conn_oracle.cursor()
        cursor_oracle.execute(query)
        
        # Salvar em CSV
        output_file = f"{datacenter}.csv"
        with open(output_file, 'w') as file:
            for row in cursor_oracle:
                file.write(str(row[0]) + "\n")
                
    finally:
        cursor_oracle.close()
        conn_oracle.close()

    # Conexão com o MySQL
    caminho_arquivo_mysql = "/PROD/POMG/pf_datalake"
    user_mysql, pass_mysql = ler_arquivo_configuracao(caminho_arquivo_mysql)
    
    conn_mysql = mysql.connector.connect(
        host="scxxp0816cto",
        user=user_mysql,
        password=pass_mysql,
        database="datalake",
        allow_local_infile=True
    )
    cursor = conn_mysql.cursor()
    
    # Limpar dados antigos e importar novos
    try:
        cursor.execute(f"DELETE FROM TB_CMS_NODGRP WHERE DATA_CENTER='{datacenter}';")
        
        load_data_query = f"""
            LOAD DATA LOCAL INFILE '{datacenter}.csv'
            INTO TABLE TB_CMS_NODGRP
            FIELDS TERMINATED BY ';'
            LINES TERMINATED BY '\\n'
        """
        cursor.execute(load_data_query)
        conn_mysql.commit()
        
        print(f"Dados importados com sucesso para a tabela TB_CMS_NODGRP datacenter: {datacenter}")
        
    except mysql.connector.Error as err:
        print(f"Erro ao importar dados para o MySQL: {err}")
    finally:
        if conn_mysql.is_connected():
            cursor.close()
            conn_mysql.close()

# Executando a funcao
export_to_csv(
    host="scbxp2603cld.exa.itau",
    port="1531",
    schema="ctm1v90",
    service_name="olprc225ro",
    file_password="/PROD/POMG/celauto_olprc225ro_1533",
    datacenter="DS-ANALITICO-G"
)

export_to_csv(
    host="scbxp2603cld.exa.itau",
    port="1531",
    schema="ctm1v90",
    service_name="olprc224ro",
    file_password="/PROD/POMG/celauto_olprc224ro_1531",
    datacenter="DS-VAREJO1-G"
)

export_to_csv(
    host="scbxp2603cld.exa.itau",
    port="1531",
    schema="ctm4v90",
    service_name="olprc224ro",
    file_password="/PROD/POMG/celauto_olprc224ro_1531",
    datacenter="DS-VAREJO2-G"
)

export_to_csv(
    host="scbxp2603cld.exa.itau",
    port="1531",
    schema="ctm5v90",
    service_name="olprc226ro",
    file_password="/PROD/POMG/celauto_olprc226ro_1533",
    datacenter="DS-ATACADO-S"
)
