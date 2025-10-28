# Documentação do Processo de Carga de Dados e Limpeza de Arquivos

Introdução

Este processo automatizado realiza a extração, limpeza, carga e remoção de arquivos relacionados a alertas do sistema. Ele utiliza diversas bibliotecas e ferramentas para interagir com APIs, manipular arquivos e carregar dados em um banco de dados MySQL.

Objetivo

O objetivo deste processo é:

Gerar um relatório de alertas utilizando uma API, Limpar o arquivo gerado, removendo aspas, Carregar os dados limpos em uma tabela específica no banco de dados MySQL, Remover os arquivos temporários gerados durante o processo.

Fluxo do Processo

Geração do Relatório: A função gerar_relatorio é utilizada para interagir com a API de automação e gerar um relatório de alertas. Os parâmetros fornecidos incluem:

endpoint: URL da API.

nome_relatorio: Nome do relatório gerado.

usuario e senha: Credenciais para autenticação na API.

arquivo_saida: Caminho onde o relatório será salvo.

Limpeza do Arquivo: A função limpar_aspas_do_arquivo remove todas as aspas (") do arquivo gerado, criando um novo arquivo limpo. Isso é necessário para garantir que os dados estejam no formato correto para serem carregados no banco de dados.

Carga no Banco de Dados: A função carregar_arquivo_no_banco realiza a carga dos dados limpos na tabela TB_ALARM do banco de dados MySQL.

Remoção de Arquivos Temporários: Após a carga dos dados, todos os arquivos temporários gerados durante o processo são removidos. Isso inclui o arquivo original e o arquivo limpo.

Tabela Utilizada: TB_ALARM
