# README

## Documentação do Processo de Manipulação de Dados

### Etapas do Processo

1.  **Configuração Inicial** Objetivo: Configurar variáveis de ambiente e ler credenciais de acesso ao banco de dados. Método: As variáveis de ambiente são definidas para especificar detalhes como nome do job, data, nome do banco de dados, servidor, diretório de arquivos, e tabelas envolvidas. As credenciais de acesso são lidas de um arquivo de configuração.

2.  **Conexão com o Banco de Dados** Objetivo: Estabelecer uma conexão com o banco de dados MySQL. Detalhes da Conexão: Utiliza as credenciais lidas do arquivo de configuração para conectar-se ao banco de dados especificado.

3.  **Manipulação de Dados** Truncamento de Tabela: A tabela principal é truncada para remover dados antigos antes de inserir novos dados. Inserção de Dados: Dados são inseridos na tabela a partir de uma seleção de outras tabelas, aplicando transformações como `SUBSTRING` e `TRIM` em campos específicos. Um arquivo externo é processado para remover espaços em branco antes de ser importado para o banco de dados. Importação de Dados: Dados de um arquivo externo são importados para a tabela principal usando o comando `LOAD DATA LOCAL INFILE`.

4.  **Atualizações e Correções** Atualização de Siglas: Siglas específicas são atualizadas com base em condições predefinidas. Correção de Rotinas e Siglas Específicas: Atualizações são feitas para corrigir siglas e rotinas específicas, otimizando a consistência dos dados. Atualização de Criticidade: A criticidade dos jobs é atualizada com base em condições específicas, incluindo a identificação de jobs muito urgentes.

5.  **Inserção e Atualização de Dados Adicionais** Inserção em Tabela de Resumo: Jobs específicos são inseridos em uma tabela de resumo, agrupados por `SERVICE` e atualizados com criticidade `Muito Urgente`. A criticidade de jobs identificados como muito urgentes é atualizada para refletir sua importância.

### Tabelas utilizadas

`TB_DEF_JOB_SERVICE_NOW`

`TB_DEF_VER_JOB`

`TB_DEF_VER_TABLES`

`TB_JOB_BIM_SUMMARY`

`TB_JOB_BIM_SUM_SERVICENOW`

`TB_DEF_JOB_SERVICE_NOW_D1`

`tb_siglas_full`

### Rotina batch: SJ7PA015
