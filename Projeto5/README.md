# README

## Introdução

Este programa automatiza o processo de geração, processamento e carga de relatórios em um banco de dados MySQL. Ele utiliza uma API para gerar e baixar relatórios, processa os dados recebidos e os insere em uma tabela específica no banco de dados. Além disso, o programa realiza a limpeza de arquivos temporários após a execução.

## Objetivo

Autenticar-se em uma API para obter um token de acesso. Gerar um relatório específico e verificar seu status até a conclusão. Baixar o relatório gerado e processar os dados. Carregar os dados processados em uma tabela MySQL. Atualizar os dados na tabela com base em uma tabela de referência. Remover arquivos temporários após o processamento.

## Fluxo do Processo

1.  **Leitura de Configuração** O programa lê as credenciais de acesso ao banco de dados e à API a partir de arquivos de configuração. Esses arquivos devem conter as informações de usuário e senha.
2.  **Geração de Token** O programa realiza a autenticação na API para obter um token de acesso. Este token é necessário para realizar as operações subsequentes, como a geração e o download do relatório.
3.  **Geração e Verificação do Relatório** O programa solicita a geração de um relatório específico na API. Após a solicitação, verifica periodicamente o status do relatório até que ele seja concluído com sucesso.
4.  **Download e Processamento do Relatório** O relatório gerado é baixado no formato CSV. Os dados do relatório são processados para remover duplicatas e aspas desnecessárias.
5.  **Carga de Dados no Banco de Dados** Os dados processados são carregados em uma tabela específica no banco de dados MySQL. Após a carga, os dados são atualizados com base em uma tabela de referência (`tb_lobs_desc`), incluindo informações adicionais.
6.  **Remoção de Arquivos Temporários** Após a conclusão do processo, os arquivos temporários gerados durante a execução são removidos para manter o ambiente limpo.

## Resumo das Tabelas Utilizadas

**tb_bim_servicenow**

**tb_lobs_desc**

## Rotina batch: SJ7PA012
