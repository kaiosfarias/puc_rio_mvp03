# puc_rio_mvp03
MVP ENGENHARIA DE DADOS - Kaio Farias
O notebook consolida um Projeto de Engenharia de Dados (MVP) que transformou uma base de dados de vendas em um Data Warehouse (DW) modelado em esquema estrela e realizou análises de negócios sobre os resultados.

Visão Geral do Projeto
O projeto utilizou o Dataset Global Superstore (dados de venda entre 2011 e 2014) para construir um pipeline de ETL/ELT na plataforma Databricks Free Edition. O objetivo principal é fornecer uma estrutura de dados modelada para que os times de vendas e finanças possam analisar o desempenho e identificar oportunidades para otimização e incremento de lucros. O notebook inclui os códigos SQL e visuais de execução do pipeline, enquanto o repositório também tem em anexo o extract do código sql inputado no databricks dentro dos pipelines, e revidências visuais da execução do ETL dentro do databricks.

Seções Principais e Conteúdo
O notebook está dividido em quatro partes principais:

1. Contexto e Objetivo: 
Fonte de Dados
Objetivo
Perguntas-chave (Foco das Análises)

2. Engenharia e Construção de DW
Modelo de Dados: A base original foi refatorada para um modelo dimensional (esquema estrela).

Domínios (Tabelas) Criados: O notebook descreve a criação das tabelas dimensionais (dim_customers, dim_priority, dim_products, dim_shipping, dim_stores, dim_timetable) e das tabelas fato (fact_order_items, fact_orders) a partir dos dados originais, detalhando as premissas e transformações aplicadas em cada domínio.

Implementação: Demonstração dos códigos SQL utilizados para a criação das Materialized Views (tabelas) e o registro da linhagem dos dados no Databricks.

3. Análise de Qualidade
A seção realiza uma inspeção nos dados das novas tabelas para garantir que os valores estão em conformidade com as regras e formatos esperados, concluindo que os dados transformados estão em compliance com as regras de data quality estabelecidas.

4. Análise de Dados e Resolução do Problema
A etapa final apresenta as análises de negócio solicitadas.
