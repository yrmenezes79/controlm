import glob
import os
from datetime import datetime

def delete_files(jobname):
    # Adiciona "**" ao final do jobname para buscar arquivos que começam com o nome
    files_to_delete = glob.glob(f"{jobname}**")
    print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Delecao de arquivos {jobname}")

    # Itera sobre a lista de arquivos e os deleta
    for file_path in files_to_delete:
        try:
            os.remove(file_path)
            print(f'{file_path} deletado com sucesso.')
        except Exception as e:
            print(f'Erro ao deletar {file_path}: {e}')

print(f"{datetime.now().strftime('%d-%m-%y_%H:%M:%S')} Final do Processo")
