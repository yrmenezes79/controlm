# Documentação dos Processos de Lote (Batch)

Este arquivo contém a documentação de vários programas de lote, extraída das imagens fornecidas.

---

### README

## Documentação do Processo de Manipulação de Dados e Limpeza de Arquivos

**PROGRAMA: analise_shout.py**

**Rotina batch: SJ7PA022**

### Introdução

Este documento descreve o processo de manipulação de dados e limpeza de arquivos. O objetivo principal deste programa é processar e atualizar informações em tabelas específicas dentro de um banco de dados MySQL, além de realizar a limpeza de arquivos temporários gerados durante o processo.

### Tabelas Envolvidas:

**TB_SHOUT_SUM**

**TB_SHOUT_SUM_TEMP**

### Descrição do Processo

1.  **Conexão com o Banco de Dados** O processo inicia com a leitura de um arquivo de configuração para obter as credenciais de acesso ao banco de dados. Em seguida, é estabelecida uma conexão com o banco de dados MySQL utilizando as credenciais obtidas.
    * 1.1. **Leitura do Arquivo de Configuração** O arquivo de configuração contém as credenciais de acesso ao banco de dados. As credenciais são lidas e armazenadas em variáveis para uso posterior na conexão com o banco de dados.
    * 1.2. **Conexão com o Banco de Dados** Uma conexão é estabelecida com o banco de dados "datalake" no servidor especificado. Esta conexão permite a execução de comandos SQL e a manipulação de dados de arquivos locais.
2.  **Manipulação de Dados** O processo envolve a execução de uma série de consultas SQL para manipular e atualizar dados nas tabelas especificadas.
    * 2.1. **Limpeza e Inserção de Dados na Tabela TB_SHOUT_SUM** A tabela **TB_SHOUT_SUM** é truncada para remover todos os dados existentes. Novos dados são inseridos na tabela a partir de uma junção de várias tabelas relacionadas a alertas e estatísticas de jobs.
    * 2.2. **Atualização de Dados na Tabela TB_SHOUT_SUM** A tabela **TB_SHOUT_SUM** é atualizada com informações adicionais de execução de jobs, utilizando dados de outra tabela relacionada.
    * 2.2.1. **Atualização de Contagem de Alertas** A tabela **TB_SHOUT_SUM_TEMP** é utilizada para armazenar temporariamente a contagem de alertas. A tabela **TB_SHOUT_SUM** é atualizada com a contagem de alertas a partir da tabela temporária.

---

### README

**PROGRAMA: rotinas-extras.py**

**Rotina batch: SJ7PA019**

### Introdução

Este documento descreve o processo de extração, transformação e carga de dados (ETL) para a tabela **TB_EXECUTION_ROTINA_EXTRA** no banco de dados MySQL. O processo envolve a geração de relatórios, processamento de arquivos e carregamento de dados em uma tabela específica.

### Tabelas Envolvidas

**TB_EXECUTION_ROTINA_EXTRA**

### Descrição do Processo

1.  **Leitura de Arquivo de Configuração** O processo inicia com a leitura de arquivos de configuração para obter as credenciais de acesso ao banco de dados e ao endpoint de geração de relatórios.
    * 1.1. **Leitura do Arquivo de Configuração** Os arquivos de configuração contêm as credenciais de acesso ao banco de dados e ao endpoint. As credenciais são lidas e armazenadas em variáveis para uso posterior.
2.  **Geração de Relatório** Um relatório é gerado a partir de um endpoint específico, utilizando as credenciais obtidas anteriormente.
    * 2.1. **Geração de Relatório** O relatório é gerado através de uma chamada à função `gerar_relatorio`, que se comunica com o endpoint de automação.
3.  **Processamento de Arquivo** O arquivo gerado é processado para remover vírgulas em datas, garantindo a formatação correta para carga no banco de dados.
    * 3.1. **Remoção de Vírgulas em Datas** As datas no arquivo são processadas para remover vírgulas, utilizando expressões regulares.
4.  **Carga de Dados no Banco de Dados** Os dados processados são carregados na tabela **TB_EXECUTION_ROTINA_EXTRA** no banco de dados MySQL.
    * 4.1. **Conexão com o Banco de Dados** Uma conexão é estabelecida com o banco de dados datalake no servidor especificado.
5.  **Remoção de Arquivos Temporários** Após a carga dos dados, os arquivos temporários são removidos para liberar espaço e manter a organização.
    * 5.1. **Remoção de Arquivos** A função `delete_files` é chamada para remover arquivos temporários relacionados ao processo.

**PROGRAMA: ultima_exe.py**

---

### README

**PROGRAMA: main.py**

**Rotina batch: SJ7PA017**

### Introdução

Este documento descreve o processo de atualização e inserção de dados em tabelas específicas no banco de dados MySQL. O objetivo é garantir que as informações de execução de jobs sejam atualizadas corretamente, utilizando dados provenientes de relatórios gerados e arquivos processados.

### Tabelas Envolvidas:

**TB_EXECUTION**

**TB_EXECUTION_30_OPEN**

**TB_EXECUTION_ULT**

**TB_DEF_JOB_SERVICE_NOW**

**TB_SHOUT**

### Descrição do Processo

1.  **Leitura do Arquivo de Configuração** O processo inicia com a leitura do arquivo de configuração para obter as credenciais de acesso ao banco de dados.
    * 1.1. **Função Utilizada** `ler_arquivo_configuracao`: Lê o arquivo de configuração e retorna o usuário e senha para conexão com o banco de dados.
2.  **Geração de Relatórios** Relatórios são gerados a partir de um endpoint de API para obter dados de execução de jobs.
3.  **Combinação e Processamento de Arquivos** Os arquivos gerados pelos relatórios são combinados e processados para remover vírgulas dentro das datas.
4.  **Inserção de Dados na Tabela TB_EXECUTION** Os dados processados são carregados na tabela **TB_EXECUTION**.
5.  **Atualização da Tabela TB_EXECUTION_ULT** A tabela **TB_EXECUTION_ULT** é atualizada com os valores mais recentes de `END_TIME`.
6.  **Atualização da Tabela TB_SHOUT** A tabela **TB_SHOUT** é atualizada com dados de notificações de jobs.

**PROGRAMA: analise_shout.py**

---

### README

* 2.3. **Remoção de Duplicatas** Duplicatas na tabela **TB_SHOUT_SUM** são removidas utilizando uma subconsulta que identifica registros duplicados com base em critérios específicos.
* 3. **Limpeza de Arquivos** Após a manipulação dos dados, o processo realiza a limpeza de arquivos temporários gerados durante a execução do job.
    * 3.1. **Remoção de Arquivos Temporários** A função `delete_files` é utilizada para remover arquivos temporários associados ao job "SJ7PA021". Esta etapa garante que o ambiente de arquivos permaneça limpo e organizado, evitando o acúmulo de arquivos desnecessários.

**PROGRAMA: shout.py**

**Rotina batch:**

### Introdução

Este documento descreve o processo de manipulação de dados e atualização de tabelas no banco de dados MySQL. O objetivo principal é realizar operações de limpeza, inserção, atualização e remoção de duplicatas em tabelas específicas, garantindo a consistência e a integridade dos dados.

### Tabelas Envolvidas:

**TB_SHOUT_SUM**

**TB_SHOUT**

**TB_CMR_STATIS**

**TB_DEF_JOB_SERVICE_EXEC**

**TB_SHOUT_SUM_TEMP**

### Descrição do Processo

1.  **Conexão com o Banco de Dados** O processo inicia com a leitura de um arquivo de configuração para obter as credenciais de acesso ao banco de dados. Em seguida, é estabelecida uma conexão com o banco de dados MySQL.
    * 1.1. **Leitura do Arquivo de Configuração** O arquivo de configuração contém as credenciais de acesso ao banco de dados. As credenciais são lidas e armazenadas em variáveis para uso posterior na conexão.
    * 1.2. **Conexão com o Banco de Dados** Uma conexão é estabelecida com o banco de dados `datalake` no servidor especificado.
    * 3. **Atualização de Dados**
    * 3.1. **Atualização de Campos na Tabela TB_SHOUT_SUM** O campo `END_TIME` na tabela **TB_SHOUT_SUM** é atualizado com base nos dados da tabela **TB_DEF_JOB_SERVICE_EXEC**.

**PROGRAMA: rotinas-extras.py**

---

### README

**PROGRAMA: analise_shout.py**

**Rotina batch: SJ7PA022**

### Introdução

Este documento descreve o processo de atualização e inserção de dados em tabelas específicas no banco de dados MySQL. O objetivo é garantir que as informações de execução de jobs sejam atualizadas corretamente, utilizando dados provenientes de relatórios gerados e arquivos processados.

### Tabelas Envolvidas

**TB_SHOUT_SUM**

**TB_SHOUT_SUM_TEMP**

**TB_DEF_VER_SHOUT**

**TB_DEF_VER_JOB**

**TB_DEF_VER_TABLES**

**TB_CMR_STATIS**

**TB_DEF_JOB_SERVICE_EXEC**

**TB_ALARM**

### Descrição do Processo

1.  **Leitura do Arquivo de Configuração** O processo inicia com a leitura do arquivo de configuração para obter as credenciais de acesso ao banco de dados.
    * 1.1. **Função Utilizada** `ler_arquivo_configuracao`: Lê o arquivo de configuração e retorna o usuário e senha para conexão com o banco de dados.
2.  **Conexão com o Banco de Dados** Estabelece uma conexão com o banco de dados MySQL utilizando as credenciais obtidas.
3.  **Inserção de Dados na Tabela TB_SHOUT_SUM** Os dados são inseridos na tabela **TB_SHOUT_SUM** a partir de várias consultas que combinam informações de diferentes tabelas de origem.
4.  **Atualização da Tabela TB_SHOUT_SUM** A tabela **TB_SHOUT_SUM** é atualizada com os tempos de término mais recentes dos jobs.
5.  **Contagem de Alertas** A tabela **TB_SHOUT_SUM_TEMP** é utilizada para armazenar a contagem de alertas dos últimos 30 dias.
6.  **Atualização de Contagem de Alertas** A tabela **TB_SHOUT_SUM** é atualizada com a contagem de alertas dos últimos 30 dias.

---

### README

**PROGRAMA: ultima_exe.py**

**Rotina batch: SJ7PA021**

### Introdução

Este documento descreve o processo de atualização e inserção de dados em tabelas específicas no banco de dados MySQL. O objetivo é garantir que as informações de execução de jobs sejam atualizadas corretamente, utilizando dados provenientes de outras tabelas.

### Tabelas Envolvidas

**TB_DEF_JOB_SERVICE_EXEC**

**TB_EXECUTION**

**TB_DEF_JOB_SERVICE_NOW**

**TB_DEF_JOB_SERVICE_EXEC**

### Descrição do Processo

1.  **Leitura do Arquivo de Configuração** O processo inicia com a leitura do arquivo de configuração para obter as credenciais de acesso ao banco de dados.
    * 1.1. **Função Utilizada** `ler_arquivo_configuracao`: Lê o arquivo de configuração e retorna o usuário e senha para conexão com o banco de dados.
2.  **Conexão com o Banco de Dados** Uma conexão é estabelecida com o banco de dados `datalake` no servidor especificado, utilizando as credenciais obtidas.
3.  **Inserção de Dados na Tabela TB_DEF_JOB_SERVICE_EXEC** Os dados da tabela **TB_DEF_JOB_SERVICE_NOW** são inseridos na tabela **TB_DEF_JOB_SERVICE_EXEC**, com algumas manipulações de formato.
4.  **Atualização da Tabela TB_DEF_JOB_SERVICE_EXEC** A tabela **TB_DEF_JOB_SERVICE_EXEC** é atualizada com os valores mais recentes de `END_TIME` provenientes da tabela **TB_EXECUTION**.
    * 4.1. **Geração de Lista de Jobs** Uma lista de jobs é gerada a partir da tabela **TB_DEF_JOB_SERVICE_NOW**, contendo os campos `DATA_CENTER` e `JOB_NAME`.
    * 4.2. **Atualização de END_TIME** Para cada job na lista, a tabela **TB_DEF_JOB_SERVICE_EXEC** é atualizada com o valor mais recente de `END_TIME` da tabela **TB_EXECUTION**.
    * 4.3. **Condição de Agrupamento** Os dados são agrupados por `DATA_CENTER` e `JOB_NAME` para obter o valor mais recente de `END_TIME`.

**PROGRAMA: main.py**

---

### README

* ...dados.
* 8. **Finalização do Processo** Após a execução de todas as operações, a conexão com o banco de dados é encerrada e os arquivos temporários são removidos.

**PROGRAMA: estatistica.py**

**Rotina batch: SJ7PA020**

### Objetivo

Este processo automatizado tem como objetivo realizar a coleta, processamento e carregamento de dados estatísticos de execução de jobs em um banco de dados MySQL. Ele é utilizado para consolidar informações de performance e execução de jobs, garantindo que os dados estejam organizados e disponíveis para análise.

### Etapas do Processo

1.  **Configuração Inicial** Arquivos de Configuração: O processo utiliza dois arquivos de configuração (`pf_datalake` e `cp068dbt`) para obter credenciais de acesso ao banco de dados e ao endpoint de geração de relatórios.
2.  **Geração de Relatório** Função Utilizada: `gerar_relatorio` Descrição: Um relatório é gerado a partir de um endpoint de automação, utilizando as credenciais obtidas dos arquivos de configuração. O relatório contém informações sobre a execução de jobs e é salvo em um arquivo de saída (ARQUIVO2).
3.  **Processamento de Dados** Remoção de Duplicatas: O arquivo gerado (ARQUIVO2) é processado para remover linhas duplicadas. As linhas únicas são gravadas em um novo arquivo (ARQUIVO), garantindo que os dados carregados no banco de dados sejam consistentes e livres de redundâncias.
4.  **Carregamento no Banco de Dados** Conexão com o Banco: O script estabelece uma conexão com o banco de dados MySQL utilizando as credenciais obtidas. Carga de Dados: Os dados do arquivo processado (ARQUIVO) são carregados na tabela **TB_CMR_STATIS** utilizando o comando `LOAD DATA LOCAL INFILE`. Transformações Aplicadas: Campos como `DATACENTER` e `JOBNAME` são ajustados para remover espaços em branco. O campo `MEDIA_PROC` é convertido de segundos para o formato de tempo (HH:mm:ss).
5.  **Limpeza de Arquivos Temporários** Função Utilizada: `delete_files` Descrição: Após o carregamento dos dados, os arquivos temporários (criados durante o processo) são excluídos para liberar espaço e evitar acúmulo de arquivos desnecessários.

### Tabelas Utilizadas

**TB_CMR_STATIS**

**Versão do python: Python 3.11.11**
