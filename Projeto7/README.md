# README

## Documentação do Processo de Extração, Processamento e Carga de Dados

### Introdução

Este processo automatiza a extração, processamento e carga de dados em um banco de dados MySQL. Ele também realiza a limpeza de dados antigos (expurgo) com base em critérios predefinidos. O objetivo principal é garantir que os dados estejam atualizados e organizados para análise e uso posterior.

### Objetivo

Extrair dados de uma tabela específica no banco de dados. Processar os dados extraídos e salvá-los em um arquivo temporário. Carregar os dados processados em uma tabela de destino no banco de dados. Realizar expurgo de dados antigos com mais de 365 dias. Garantir a integridade e limpeza do ambiente ao final do processo.

### Fluxo do Processo

1.  **Leitura de Configuração** O programa lê as credenciais de acesso ao banco de dados a partir de um arquivo de configuração. Este arquivo deve conter as informações de usuário e senha necessárias para autenticação.
2.  **Extração de Dados** Os dados são extraídos de uma tabela específica (`TB_EXECUTION`) no banco de dados. A extração considera apenas os registros com `END_TIME` nos últimos dois dias a partir da data atual. Os dados extraídos são agrupados e ordenados por data e centro de dados.
3.  **Processamento de Arquivo** Os dados extraídos são salvos em um arquivo temporário. O arquivo é processado para remover cabeçalhos e ajustar o formato dos dados, garantindo que estejam prontos para a carga no banco de dados.
4.  **Carga de Dados** Os dados processados são carregados em uma tabela de destino (`TB_SUMMARY_PROC`) no banco de dados. A carga utiliza o comando `LOAD DATA LOCAL INFILE`, que permite a inserção eficiente de grandes volumes de dados.
5.  **Expurgo de Dados Antigos** O programa verifica a tabela de destino (`TB_SUMMARY_PROC`) em busca de registros com mais de 365 dias. Caso existam registros antigos, eles são removidos da tabela.

### Resumo das Tabelas utilizadas

**TB_EXECUTION**

**TB_SUMMARY_PROC**

**TB_EXECUTION**

**TB_SUMMARY_PROC**
