import requests
from datetime import datetime
import urllib3

# Desabilitar avisos de certificado SSL inseguros
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def gerar_token(endpoint, usuario, senha):
    print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Gerando o Token...\n")
    try:
        # Enviar requisição POST para o endpoint de login
        login_response = requests.post(
            f"{endpoint}/session/login",
            json={"username": usuario, "password": senha},
            verify=False
        )

        # Verificar se a requisição foi bem-sucedida
        if login_response.status_code == 200:
            token = login_response.json().get("token")
            if token:
                print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Token gerado com sucesso.\n")
                return token
            else:
                print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Erro: Token não encontrado na resposta.\n")
        else:
            print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Erro: Falha na requisição. Código HTTP: {login_response.status_code}\n")

    except Exception as e:
        print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Erro ao gerar o token: {e}\n")

    return None


def finaliza_token(endpoint, token):
    requests.post(
        f"{endpoint}/session/logout",
        headers={"Authorization": f"Bearer {token}"},
        verify=False
    )
    print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Token finalizado\n")
