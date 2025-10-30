import requests
import time
from datetime import datetime
import urllib3
import sys

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def gerar_relatorio(endpoint, token, nome_relatorio, arquivo_saida):
    """Relatorio Salvo localmente"""
    print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Geracao do relatorio - {nome_relatorio}")
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
        print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Erro ao gerar o relatório")
        return # Adicionado para consistência

    # Acompanhar Status do Relatório
    while True:
        status_response = requests.get(
            f"{endpoint}/reporting/status/{report_id}",
            headers={"Authorization": f"Bearer {token}"},
            verify=False
        )
        status = status_response.json().get("status")

        if status == "SUCCEEDED":
            print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Status do relatorio: {status}")
            download_response = requests.get(
                f"{endpoint}/reporting/download?reportId={report_id}",
                headers={"Authorization": f"Bearer {token}"},
                verify=False
            )
            with open(arquivo_saida, "wb") as file:
                file.write(download_response.content)
            print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Relatório salvo em {arquivo_saida}")
            break
        elif status in ["PENDING", "PROCESSING"]:
            print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Tracking do relatorio: {status}")
            time.sleep(30) # O da imagem é 30
        else:
            print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Erro na geracao do relatorio: {status}")
            break

def gera_relatorio(endpoint:str, token:str, nome_relatorio:str):
    """Metodo para buscar reports como variavel"""
    try:
        print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Buscando no Control M report - {nome_relatorio}.\n")
        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
        
        response = requests.post(f'{endpoint}/reporting/report', headers=headers, verify=False, json={'name': nome_relatorio, 'format': 'CSV'})
        responseData = response.json()
        reportId = responseData['reportId']

        while True:
            response = requests.get(f'{endpoint}/reporting/status/{reportId}', verify=False, headers=headers)
            responseData = response.json()
            downloadStatus = responseData['status']
            if downloadStatus.upper() == 'SUCCEEDED':
                break
            elif downloadStatus.upper() in ['PENDING', 'PROCESSING']:
                print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Report ainda nao disponivel, Status = {downloadStatus.upper()}\n")
                time.sleep(5)
            else:
                raise ValueError(f"Status do download invalido ou nao encontrado: {downloadStatus}\n")
        
        # (Esta função não parece baixar o arquivo, apenas espera que ele esteja pronto)

    except Exception as e:
        print(E) # A imagem mostra 'print(E)'
        print(response)
        sys.exit(2)
    return None # Inferido da função abaixo


def gera_relatorio_filtro(endpoint:str, token:str, nome_relatorio:str, filtros:dict):
    """Metodo para buscar reports como variavel"""
    try:
        print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Buscando no Control M report - {nome_relatorio}.\n")
        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
        
        filters = []
        if len(filtros) > 0:
            for filtro, valor in filtros.items():
                filters.append({'name': filtro, 'value': valor})
        
        request_json = {'name': nome_relatorio, 'format': 'CSV', 'filters': filters}
        response = requests.post(f'{endpoint}/reporting/report', headers=headers, verify=False, json=request_json)
        responseData = response.json()
        reportId = responseData['reportId']

        while True:
            response = requests.get(f'{endpoint}/reporting/status/{reportId}', verify=False, headers=headers)
            responseData = response.json()
            downloadStatus = responseData['status']
            if downloadStatus.upper() == 'SUCCEEDED':
                break
            elif downloadStatus.upper() in ['PENDING', 'PROCESSING']:
                print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Report ainda nao disponivel, Status = {downloadStatus.upper()}\n")
                time.sleep(5)
            else:
                raise ValueError(f"Status do download invalido ou nao encontrado: {downloadStatus}\n")

        response = requests.get(f'{endpoint}/reporting/download', params={'reportId': reportId}, verify=False, headers=headers)
        return response.content.decode("utf-8")
        
    except Exception as e:
        print(E) # A imagem mostra 'print(E)'
        print(response)
        sys.exit(2)
    return None
