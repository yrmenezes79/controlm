Tabelas e Controle de Dados
Introdução

Este documento descreve um processo automatizado que manipula dados em tabelas de um banco de dados MySQL.
O processo é utilizado para atualizar, sincronizar e gerar informações relacionadas a jobs e siglas, além de implementar uma trava de segurança para evitar sobrecarga de dados.

Objetivo

O objetivo deste processo é:

Truncar e atualizar a tabela TB_DEF_JOB_SERVICE_NOW_INC com dados novos e removidos.
Sincronizar a tabela TB_DEF_JOB_SERVICE_NOW_D1 com os dados atuais.
Gerar um resumo consolidado na tabela TB_DEF_JOB_SERVICE_NOW_SUM.
Atualizar a tabela TB_DEF_SIGLA_SUM com informações agrupadas por sigla e data center.
Implementar uma trava de segurança para evitar que a tabela TB_DEF_JOB_SERVICE_NOW_INC ultrapasse um limite de 10.000 registros.

Fluxo do Processo

Leitura de Configuração
O processo começa com a leitura das credenciais de acesso ao banco de dados a partir de um arquivo de configuração.
O arquivo deve conter as informações de usuário e senha.

Truncar Tabela TB_DEF_JOB_SERVICE_NOW_INC
A tabela TB_DEF_JOB_SERVICE_NOW_INC é truncada para remover todos os dados existentes antes de inserir novos registros.

Inserção de Dados na Tabela TB_DEF_JOB_SERVICE_NOW_INC
Os dados são inseridos na tabela TB_DEF_JOB_SERVICE_NOW_INC com base em comparações entre as tabelas TB_DEF_JOB_SERVICE_NOW e TB_DEF_JOB_SERVICE_NOW_D1.
Os registros são marcados como "I" (inclusão) ou "D" (exclusão).

Sincronização da Tabela TB_DEF_JOB_SERVICE_NOW_D1
A tabela TB_DEF_JOB_SERVICE_NOW_D1 é truncada e atualizada com os dados da tabela TB_DEF_JOB_SERVICE_NOW.

Atualização da Tabela TB_DEF_JOB_SERVICE_NOW_SUM
Os dados da tabela TB_DEF_JOB_SERVICE_NOW_INC são consolidados na tabela TB_DEF_JOB_SERVICE_NOW_SUM.

Atualização da Tabela TB_DEF_SIGLA_SUM
A tabela TB_DEF_SIGLA_SUM é atualizada com informações agrupadas por sigla e data center.

Trava de Segurança
Uma trava de segurança é implementada para verificar a quantidade de registros na tabela TB_DEF_JOB_SERVICE_NOW_INC.
Caso o número de registros ultrapasse 10.000, a tabela é truncada.

Resumo das Tabelas Utilizadas

TB_DEF_JOB_SERVICE_NOW

TB_DEF_JOB_SERVICE_NOW_D1

TB_DEF_JOB_SERVICE_NOW_INC

TB_DEF_JOB_SERVICE_NOW_SUM

TB_DEF_SIGLA_SUM

Versão do Python:

Python 3.11.11
