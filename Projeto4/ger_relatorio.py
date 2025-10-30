import requests
import time
from datetime import datetime
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def gerar_relatorio(endpoint, usuario, senha, nome_relatorio, arquivo_saida):
    # Gerar Token
    print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Geracao do Token")
    login_response = requests.post(
        f"{endpoint}/session/login",
        json={"username": usuario, "password": senha},
        verify=False
    )
    token = login_response.json().get("token")
    if not token:
        print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Erro ao gerar o token")
        return

    # Gerar Relatorio
    print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Geracao do relatorio - {nome_relatorio}")
    report_response = requests.post(
        f"{endpoint}/reporting/report",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        json={"name": nome_relatorio, "format": "CSV"},
        verify=False
    )

    report_id = report_response.json().get("reportId")
    if not report_id:
        print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Erro ao gerar o relatório")
        return

    # Acompanhar Status do Relatório
    while True:
        status_response = requests.get(
            f"{endpoint}/reporting/status/{report_id}",
            headers={"Authorization": f"Bearer {token}"},
            verify=False
        )
        status = status_response.json().get("status")

        if status == "SUCCEEDED":
            print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Status do relatorio: {status}")
            download_response = requests.get(
                f"{endpoint}/reporting/download?reportId={report_id}",
                headers={"Authorization": f"Bearer {token}"},
                verify=False
            )
            with open(arquivo_saida, "wb") as file:
                file.write(download_response.content)
            print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Relatorio salvo em {arquivo_saida}")
            break
        elif status in ["PENDING", "PROCESSING"]:
            print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Tracking do relatorio: {status}")
            time.sleep(30)
        else:
            print(f"{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')} Erro na geracao do relatorio: {status}")
            break

    # close token
    requests.post(
        f"{endpoint}/session/logout",
        headers={"Authorization": f"Bearer {token}"},
        verify=False
    )
