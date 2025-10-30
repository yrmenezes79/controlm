# README

## Documentação do Programa

### Descrição Geral:

Este programa é responsável por ler arquivos de configuração para obter credenciais de banco de dados, listar arquivos SQL no diretório atual, executar consultas SQL em um banco de dados Oracle, exportar os resultados para arquivos de texto e, em seguida, importar esses dados para tabelas em um banco de dados MySQL. O programa também lida com erros de leitura de arquivos e conexão com bancos de dados, garantindo que as operações sejam registradas com timestamps.

### Componentes Principais:

**Processo de Listagem de Arquivos:**

Gera um arquivo de lista (ARQUIVO) contendo nomes de arquivos SQL no diretório atual, excluindo aqueles que contêm "CREATE".

**Exportação de Dados do Oracle:**

Para cada tabela listada, o programa conecta-se a um banco de dados Oracle usando credenciais lidas de um arquivo de configuração. Executa consultas SQL a partir de arquivos correspondentes e grava os resultados em arquivos de texto.

**Importação de Dados para o MySQL:**

Conecta-se a um banco de dados MySQL usando credenciais lidas de outro arquivo de configuração. Trunca a tabela de destino e carrega dados do arquivo de texto gerado anteriormente. Exclui o arquivo de texto após a importação bem-sucedida.

### Rotina batch: SJ7PA016

### Versão do python: Python 3.12.8
