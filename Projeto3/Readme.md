Documentação do Programa de Carga e Expurgo de Dados
Introdução
Este programa é responsável por realizar a transferência de arquivos via SCP, carregar dados em uma tabela de um banco de dados MySQL e realizar operações de expurgo com base em critérios de data. O objetivo principal é manter a tabela datalake.TB_ALTER_JOB atualizada com dados de um arquivo CSV e remover registros antigos.

Objetivo
Transferir um arquivo CSV de um servidor remoto (scor063cta) para um diretório local. Carregar os dados do arquivo CSV na tabela datalake.TB_ALTER_JOB do banco de dados MySQL. Realizar expurgo de registros antigos na tabela TB_ALTER_JOB com base em uma condição de data.

Fluxo do Processo
Leitura de Configuração A função ler_arquivo_configuracao é utilizada para ler as credenciais de acesso ao banco de dados a partir de um arquivo de configuração. O arquivo deve conter as informações de usuário e senha.

Cópia de Arquivo via SCP O programa utiliza a biblioteca paramiko para estabelecer uma conexão SSH e transferir um arquivo CSV de um servidor remoto para um diretório local.

Carga de Dados no Banco de Dados Os dados do arquivo CSV transferido são carregados na tabela datalake.TB_ALTER_JOB do banco de dados MySQL.

Geração de Arquivo de Expurgo O programa gera uma lista de datas de registros antigos (com mais de 2 anos) na tabela TB_ALTER_JOB e os remove.

Remoção de Arquivos Locais Após a execução das operações no banco de dados, o programa remove o arquivo CSV transferido para o diretório local.

Resumo das Tabelas Utilizadas
TB_ALTER_JOB

Versão do python: Python 3.11.11
