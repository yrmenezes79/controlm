# README

## Documentação do Processo de Automação de Banco de Dados

### Objetivo

O objetivo deste processo é automatizar a manipulação de dados em um banco de dados MySQL, especificamente para a tabela `TB_JOB_BIM_SUMMARY`. O processo envolve a leitura de credenciais de configuração, execução de consultas SQL e atualização de dados de forma eficiente e segura.

### Etapas do Processo

1.  **Configuração Inicial** Arquivos de Configuração: O processo começa com a leitura de um arquivo de configuração que contém as credenciais de acesso ao banco de dados. Este arquivo é essencial para garantir que as informações de login não estejam expostas diretamente no código. Variáveis de Conexão: As credenciais lidas são armazenadas em variáveis que são usadas para estabelecer a conexão com o banco de dados.

2.  **Execução de Consultas SQL**

    **Função de Execução com Reconexão:** Uma função dedicada é utilizada para executar consultas SQL. Esta função é projetada para tentar reconectar ao banco de dados em caso de falhas, garantindo a resiliência do processo.
    **Comandos Executados:** `Truncate` da Tabela: A tabela `TB_JOB_BIM_SUMMARY` é truncada para remover todos os dados existentes antes de inserir novos dados. `Inserção de Dados`: Novos dados são inseridos na tabela a partir de outra tabela (`TB_JOB_BIM`), agrupados por `DATA_CENTER` e `JOBNAME`. `Atualização de Dados`: A tabela é atualizada com informações de tempo de execução máximo e se o job é crítico, utilizando dados de outras tabelas (`TB_EXECUTION` e `TB_DEF_VER_JOB`).

3.  **Manutenção e Limpeza** Limpeza de Dados: Após a execução das operações, a tabela é limpa para garantir que apenas os dados recentes e atualizados sejam mantidos. `Log de Atividades`: O processo imprime logs com timestamps para cada operação, permitindo o monitoramento e auditoria das atividades realizadas.

### Tabelas utilizadas

**TB_JOB_BIM_SUMMARY**
