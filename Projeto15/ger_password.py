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
        print(f"Arquivo {caminho_arquivo} nao encontrado.")
        return None, None
    except Exception as e:
        print(f"Erro ao ler arquivo de configuração: {str(e)}")
        return None, None
