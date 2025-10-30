# README

## Documentação do Processo de Carga e Expurgo de Dados

### Objetivo

Carregar dados de arquivos externos para uma tabela específica no banco de dados. Tratar os dados para remover duplicidades e formatar corretamente. Realizar expurgo de registros antigos com base em critérios de data. Atualizar tabelas auxiliares para consolidar informações de execução de jobs.

### Estrutura do Processo

1.  **Carga de Dados** Arquivos de Entrada:
    Os arquivos de entrada são listados no diretório especificado (DIRFILE) e filtrados com base em padrões específicos. Cada arquivo é processado para: Remover duplicidades usando comandos de sistema (sort e uniq). Formatar os dados para remover aspas e preparar um arquivo temporário (FILE\_TMP). Carga no Banco de Dados.

2.  **Expurgo de Dados** Critério de Expurgo:
    Registros com data (DATA\_CAR) anterior a 39 dias da data atual são identificados para expurgo. O processo de expurgo é realizado por meio de comandos SQL que removem os registros antigos da tabela principal. Tabela Impactada:
    `TB_JOB_BIM`. Registros antigos são excluídos com base no critério de data.

3.  **Atualização de Tabelas Auxiliares** Tabela Auxiliar: `TB_JOB_BIM_SERVICENOW_LOB`. Tabela utilizada para consolidar informações de execução de jobs. O processo inclui: Truncar a tabela para remover dados antigos. Inserir novos dados agrupados por `SERVICE_NAME` e `JOBNAME`.

Resumo das Tabelas Utilizadas

`TB_JOB_BIM`

`TB_JOB_BIM_SERVICENOW_LOB`

### Rotina batch: SJ7PA018
