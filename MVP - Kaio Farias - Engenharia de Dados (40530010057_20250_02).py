# Databricks notebook source
# MAGIC %md
# MAGIC # MVP ENGENHARIA DE DADOS
# MAGIC
# MAGIC **Nome:** Kaio Santos de Farias
# MAGIC
# MAGIC **Matrícula:** 4052025000452
# MAGIC
# MAGIC **Dataset:** [Global Superstore](https://www.kaggle.com/datasets/anandaramg/global-superstore?resource=download)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # AVISOS
# MAGIC
# MAGIC - O Notebook a seguir consolida todos as informações do fluxo executado no Databricks para a execução do MVP. 
# MAGIC - Parti da inserção da base original retirada do Kaggle no workspace Free Edition do Databricks, construí um pipeline com um conjunto de ações para tratamento dos dados necessários, resultando na construção de um DW em modelo estrela. 
# MAGIC - NO decorrer do notebook, trago todos os descrititvos, análises e racionais utilizados, além dos códigos executados no pipeline, dos registros visuais da exeução do pipeline em Databricks, e das evidências visuais de todos os gráficos construídos. 
# MAGIC - Os mesmos códigos SQL também estarão disponíveis no diertório do github, em formato .sql

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import io
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

# MAGIC %md
# MAGIC # PARTE 1 - Contexto e objetivo

# COMMAND ----------

# MAGIC %md
# MAGIC ## A fonte de dados
# MAGIC Para executar o projeto a seguir, usaremos o dataset Global Superstore, disponível na plataforma Kaggle. 
# MAGIC
# MAGIC Segundo as informações apresentadas, o dataset é inspirado no contexto do Walmart, como uma empresa consolidada e fornecedora de diversos tipos de produtos, o que fornece a complexidade e variedade de dados suficientes para alimentar diversos casos de uso e estudo. 
# MAGIC
# MAGIC A base de dados resultante inclui uma única tabela, com dados de venda entre os anos de 2011 e 2014. Apesar do aspecto datado dos dados presentes, optamos pela base devido ao seu volume, consistência e variabilidade das informações. 
# MAGIC
# MAGIC O dataset escolhido contém uma única tabela, que consolida dados relativos a múltiplos domínios de dados. Em uma implementação real como um Data Warehouse, diversos ajustes em sua estrutura poderiam ajudar na otimização, buscando uma estrutura que permitisse a consulta e análise de maneira simples, rápida e eficiente. 
# MAGIC
# MAGIC Portanto, esse será o direcional para a execução do trabalho a seguir. 
# MAGIC
# MAGIC ## Objetivo do projeto 
# MAGIC ## 
# MAGIC O objetivo do projeto é modelar um DW que auxilie o time de vendas e finanças da Superstore a analisar os resultados de vendas da operação e identificar oportunidades de otimização e incremento dos lucros.  
# MAGIC **Perguntas-chave**
# MAGIC - Quais categorias têm a margem de lucro (Lucro / Vendas) mais alta ou mais baixa?
# MAGIC - Quem são os clientes que fizeram o maior volume de compras ou que geraram o maior valor de vendas/lucro?
# MAGIC - Existe oportunidade de melhoria do resultado de lucro no portfolio de produtos?
# MAGIC - Qual é o desempenho de vendas e lucro por mercado?
# MAGIC - Quais as combinações de produto/região que têm gerado a maior margem de lucro? 
# MAGIC - Quais clientes e segmentos representam a maior fatia de receita e lucro? Quais os que devem ser invetigados porque possuem 
# MAGIC - Como o desconto aplicado afeta o lucro e a quantidade de produtos vendidos?
# MAGIC - Qual é o segmento de cliente que contribui com a maior parte das vendas e do lucro?
# MAGIC - Qual é o crescimento anual de vendas e lucro ao longo do tempo? Qual é o desempenho de vendas por número da semana ?
# MAGIC - Quais categorias estão mais propensas a ter pedidos com prioridade alta, e qual é o impacto disso nos custos de envio?
# MAGIC - Qual é o tempo médio de trânsito e como ele varia por modo de envio ou região ?
# MAGIC
# MAGIC Para a execução prática das análises, focaremos nas 5 primeiras perguntas. 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # PARTE 2 - Engenharia e construção de DW

# COMMAND ----------

# MAGIC %md
# MAGIC ## Base original (Masterdata)
# MAGIC A base recebida continha os seguintes campos: 
# MAGIC
# MAGIC - category: A categoria de produtos vendidos na superloja.
# MAGIC - city: A cidade onde o pedido foi feito.
# MAGIC - country: O país onde a superloja está localizada.
# MAGIC - customer_id: Um identificador único para cada cliente.
# MAGIC - customer_name: O nome do cliente que fez o pedido.
# MAGIC - discount: O desconto aplicado ao pedido.
# MAGIC - market: O mercado ou região onde a superloja opera.
# MAGIC - 记录数 number_of_records(originalmente em chinês): Uma coluna desconhecida ou não especificada.
# MAGIC - order_date: A data em que o pedido foi feito.
# MAGIC - order_id: Um identificador único para cada pedido.
# MAGIC - order_priority: O nível de prioridade do pedido.
# MAGIC - product_id: Um identificador único para cada produto.
# MAGIC - product_name: O nome do produto.
# MAGIC - profit: O lucro gerado com o pedido.
# MAGIC - quantity: A quantidade de produtos pedidos.
# MAGIC - region: A região onde o pedido foi feito.
# MAGIC - row_id: Um identificador único para cada linha no conjunto de dados.
# MAGIC - sales: O valor total de vendas para o pedido.
# MAGIC - segment: O segmento de cliente (por exemplo, consumidor, corporativo ou escritório doméstico).
# MAGIC - ship_date: A data em que o pedido foi enviado.
# MAGIC - ship_mode: O modo de envio usado para o pedido.
# MAGIC - shipping_cost: O custo de envio para o pedido.
# MAGIC - state: O estado ou região dentro do país.
# MAGIC - sub_category: A subcategoria de produtos dentro da categoria principal.
# MAGIC - year: O ano em que o pedido foi feito.
# MAGIC - market2: Outra coluna relacionada a informações de mercado.
# MAGIC - weeknum: O número da semana em que o pedido foi feito.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Esquema destino
# MAGIC A base de dados original foi construída em um único arquivo, o que gera alta redundância, replicando múltiplas entidades ao longo das linhas, o que, em uma implementação real, prejudica a performance das plataformas usadas. Para a implementação do DW, precisamos refatorar a base recebida para um modelo dimensional. 
# MAGIC A partir dos campos presentes na base, propomos o seguinte modelo de schema para a base: 
# MAGIC ![](path)
# MAGIC ![Screenshot 2025-11-30 at 13.45.13.png](./Screenshot 2025-11-30 at 13.45.13.png "Screenshot 2025-11-30 at 13.45.13.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Domínios de dados, descrições e premissas 
# MAGIC
# MAGIC - Stores
# MAGIC   - Corresponde às unidades de lojas consideradas. Cada registro nesse domínio/tabela corresponde a uma unidade. 
# MAGIC   - Dado que não possuímos metadados sobre lojas, atuamos com a premissa de que cada país é unicamente unicamente representado no dataset possui uma unidade. 
# MAGIC   - Essa decisão foi tomada de forma a reduzir a complexidade dos dados analisados, removendo um pouco da granularidade de cidades e estados, mas permitindo uma dimensão de loja a ser considerada. 
# MAGIC   - Para identificar cada unidade, criaremos uma chave única que será utilizada para a conexão com as demais tabelas. 
# MAGIC - Products
# MAGIC   - Corresponde a lista de produtos oferecidos nas lojas. Cada produto deve possuir um identificador único, e pertercer a um branch de categoria e subcategoria da árvore de produtos da super store. 
# MAGIC   - Não é permitido que um mesmo produto esteja presente presente em mais de uma subcategoria, nem que uma mesma sub categoria seja listada em 2 categorias diferentes
# MAGIC - Orders
# MAGIC   - Corresponde a lista de pedidos executados, e suas informações adjacentes. 
# MAGIC   - Cada pedido pode ser atrelado a múltiplos itens, mas apenas um único consumidor, e uma única loja
# MAGIC   - Para o contexto do projeto atual, assumimos que todos os items de um mesmo pedido são enviados no mesmo momento. 
# MAGIC - Order Items
# MAGIC   - Corresponde aos itens únicos dos pedidos realizados e suas informações adjacentes. Cada registro nesse domínio possui um identificador único, e pode estar conectado a apenas um pedido e apenas um produto no domínio de produto, e consequentemente, relacionado a apenas um cliente e apenas uma loja. 
# MAGIC   - Sendo assim, é a menor granularidade possível presente no esquema a ser estruturado. 
# MAGIC - Customers
# MAGIC   - Corresponde aos clientes da loja, e suas informações adjacentes. 
# MAGIC   - Cada cliente pode estar relacionado a múltiplos pedidos no domínio de pedidos, e por consequência, podem também ter comprado de múltiplas lojas. 
# MAGIC - Shipping
# MAGIC   - Corresponde aos tipos de envios existentes na operação da super store. 
# MAGIC - Priority
# MAGIC   - Corresponde a tabela que permite a classificação de prioridade dos pedidos realizados. 
# MAGIC - Time
# MAGIC   - Adicionalmente, prevemos uma tabela de tempo a ser utilizada como suporte para análises mais detalhadas. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registros de tratamento e engenharia
# MAGIC
# MAGIC Segundo a recomendação do desenvolvimento do MVP, toda a execução do projeto foi realizada utilizando a plataforma Databrick - free edition. 
# MAGIC A base retirada do Kaggle foi manualmente ingerida no workspace como um arquivo .txt, e em seguida, tratada e refatorada em múltiplas tabelas em um modelo de star schema, para construir o output desenhado conforme o schema demonstrado no ERD acima. A construção das tabelas foi feita por meio do desenvolvimento de um pipeline, com múltiplos códigos em SQL, para a geração de cada uma das tabelas. 
# MAGIC
# MAGIC Todas as tabelas e colunas resultantes tiveram seus metadados e descritivos cadastrados no catálogo do próprio Databricks, seguindo a recomendação das melhores práticas descritas no curso, contendo descrição dos valores, valores esperados, origem e transformações realizadas. 
# MAGIC
# MAGIC Insiro abaixo todas as evidêndias da construção e execução do pipeline, assim como os códigos utilizados. 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Código SQL utilizado na implementação dos pipelines
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Código SQL utilizado na implementação dos pipelines
# MAGIC
# MAGIC -- dim_customers
# MAGIC CREATE MATERIALIZED VIEW `workspace`.`default`.`dim_customers`
# MAGIC   (
# MAGIC
# MAGIC     customer_id STRING
# MAGIC     COMMENT 'Descrição:  Um identificador único para cada cliente, Origem:  Master data, Formato: integer, Transformação:  Extraído da tabela original, sem ajustes, Valores esperados:  Número inteiro, positivo e maior que zero, Nullable:  Não',
# MAGIC     customer_name STRING
# MAGIC     COMMENT 'Descrição:  O nome do cliente que fez o pedido, Origem:  Master data, Formato: string, Transformação:  Extraído da tabela original, sem ajustes, Número inteiro, positivo e maior que zero String de caracteres com múltiplas palavras, Nullable:  Não',
# MAGIC     segment STRING
# MAGIC     COMMENT 'Descrição:  O segmento de cliente (por exemplo, consumidor, corporativo ou escritório doméstico), Origem:  Master data, Formato: string, Transformação:  Extraído da tabela original, sem ajustes, String de caracteres com múltiplas palavras Pode conter or valores Consumer, Corporate ou Home Office, Nullable:  Não'
# MAGIC   )
# MAGIC   COMMENT 'The table contains information about customers, including their identifiers, names, and segments. This data can be used for customer segmentation analysis, targeted marketing efforts, and understanding customer demographics.' AS
# MAGIC SELECT DISTINCT
# MAGIC   `Customer ID` as customer_id,
# MAGIC   `Customer Name` as customer_name,
# MAGIC   `Segment` as segment
# MAGIC FROM
# MAGIC   workspace.default.superstore_masterdata
# MAGIC
# MAGIC -- dim priority
# MAGIC CREATE MATERIALIZED VIEW `workspace`.`default`.`dim_priority`
# MAGIC   (
# MAGIC
# MAGIC     priority STRING
# MAGIC     COMMENT 'Descrição:  O nível de prioridade do pedido, Origem:  Master data, Formato: string, Transformação:  Extraído da tabela original, sem ajustes, Pode conter or valores Consumer, Corporate ou Home Office String de caracteres com as opções Critical, HIgh, Low, e Medium , Nullable:  Não',
# MAGIC     priority_level INT
# MAGIC     COMMENT 'Descrição:  Chave que identifica a prioridade do pedido, Origem:  Gerado sistematicamente, Formato: integer, Transformação:  Criado no processo de etl, para permitir conexões , String de caracteres com as opções Critical, HIgh, Low, e Medium  Numério decimal, positivo de 1 a 4, Nullable:  Não'
# MAGIC   ) AS
# MAGIC SELECT DISTINCT
# MAGIC   `Order Priority` AS priority,
# MAGIC   CASE
# MAGIC     WHEN `Order Priority` = 'Critical' THEN 4
# MAGIC     WHEN `Order Priority` = 'High' THEN 3
# MAGIC     WHEN `Order Priority` = 'Medium' THEN 2
# MAGIC     WHEN `Order Priority` = 'Low' THEN 1
# MAGIC     ELSE 0 -- Caso apareça algum valor estranho/nulo, ele recebe 0
# MAGIC   END AS priority_level
# MAGIC FROM
# MAGIC   workspace.default.superstore_masterdata
# MAGIC ORDER BY
# MAGIC   priority_level DESC
# MAGIC -- dim_products
# MAGIC CREATE MATERIALIZED VIEW `workspace`.`default`.`dim_products`
# MAGIC   (
# MAGIC
# MAGIC     product_id STRING
# MAGIC     COMMENT 'Descrição:  Um identificador único para cada produto, Origem:  Master data, Formato: string, Transformação:  Extraído da tabela original, sem ajustes, Nome do país, Múltiplas possibilidades String de 16 caracteres, alfanumérico, Nullable:  Não',
# MAGIC     product_name STRING
# MAGIC     COMMENT 'Descrição:  O nome do produto, Origem:  Master data, Formato: string, Transformação:  Extraído da tabela original, sem ajustes, String de 16 caracteres, alfanumérico String de caracteres com múltiplas palavras, Nullable:  Não',
# MAGIC     category STRING
# MAGIC     COMMENT 'Descrição:  A categoria de produtos vendidos na superloja, Origem:  Master data, Formato: string, Transformação:  Extraído da tabela original, sem ajustes, Numério decimal, positivo. Não pode ser zero.  String de caracteres com as opções Furniture, Office supplies e Technology, Nullable:  Não',
# MAGIC     subcategory STRING
# MAGIC     COMMENT 'Descrição:  A subcategoria de produtos dentro da categoria principal, Origem:  Master data, Formato: string, Transformação:  Extraído da tabela original, sem ajustes, String de caracteres com as opções Furniture, Office supplies e Technology String de caracteres com 17 valores possívies dentro do universo de produtos para escritórios, Nullable:  Não',
# MAGIC     product_price DOUBLE
# MAGIC     COMMENT 'Descrição:  Valor unitário dos items a venda, Origem:  Calculado, Formato: float, Transformação:  Calculado a partir da divisão do total de vendas de cada produto dividido pela quantidade de unidades vendids, String de caracteres com múltiplas palavras Numério decimal, positivo. Não pode ser zero. , Nullable:  Não'
# MAGIC   ) AS
# MAGIC SELECT
# MAGIC   `Product ID` as product_id,
# MAGIC   `Product Name` as product_name,
# MAGIC   `Category` as category,
# MAGIC   `Sub-Category` as subcategory,
# MAGIC   ROUND(SUM(Sales) / SUM(Quantity), 2) AS product_price
# MAGIC FROM
# MAGIC   workspace.default.superstore_masterdata
# MAGIC GROUP BY
# MAGIC   `Product ID`,
# MAGIC   `Product Name`,
# MAGIC   `Category`,
# MAGIC   `Sub-Category`
# MAGIC -- dim_shipping
# MAGIC CREATE MATERIALIZED VIEW `workspace`.`default`.`dim_shipping`
# MAGIC   (
# MAGIC
# MAGIC     ship_mode STRING
# MAGIC     COMMENT 'Descrição:  O modo de envio usado para o pedido, Origem:  Master data, Formato: string, Transformação:  Extraído da tabela original, sem ajustes, String de caracteres com as opções First Class, Same Day, Second Class, e Standart Class String de caracteres com as opções First Class, Same Day, Second Class, e Standart Class, Nullable:  Não',
# MAGIC     ship_code INT
# MAGIC     COMMENT 'Descrição:  Identificador único do modo de envio, Origem:  Gerado sistematicamente, Formato: integer, Transformação:  Criado no processo de etl, para permitir conexões , String de caracteres com as opções First Class, Same Day, Second Class, e Standart Class Numéro inteiro, maior que 0., Nullable:  Não'
# MAGIC   ) AS
# MAGIC SELECT
# MAGIC   `Ship Mode` AS ship_mode,
# MAGIC   ROW_NUMBER() OVER (ORDER BY `Ship Mode`) AS ship_code
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT DISTINCT
# MAGIC       `Ship Mode`
# MAGIC     FROM
# MAGIC       workspace.default.superstore_masterdata
# MAGIC   ) AS subquery
# MAGIC -- dim_stores
# MAGIC CREATE MATERIALIZED VIEW `workspace`.`default`.`dim_stores`
# MAGIC   (
# MAGIC
# MAGIC     country STRING
# MAGIC     COMMENT 'Descrição:  O país onde a superloja está localizada, Origem:  Master data, Formato: string, Transformação:  Extraído da tabela original, sem ajustes, Nome da região, nomes possíveis são: Africa, Canada, Caribbean, Central, Central Asia, East, EMEA, North, North Asia, Oceania, South, Southeast Asia, West Nome do país, Múltiplas possibilidades, Nullable:  Não',
# MAGIC     store_code STRING
# MAGIC     COMMENT 'Descrição:  Identificador único da loja, Origem:  Gerado sistematicamente, Formato: integer, Transformação:  Criado no processo de etl, para permitir conexões , Numério decimal, positivo de 1 a 4 Criado a partir da primeira palavra do nome do país, somado a um número de 1 a 147 , Nullable:  Não',
# MAGIC     market STRING
# MAGIC     COMMENT 'Descrição:  O mercado ou região onde a superloja opera, Origem:  Master data, Formato: string, Transformação:  Extraído da tabela original, sem ajustes, Criado a partir da primeira palavra do nome do país, somado a um número de 1 a 147  Mercado onde a loja opera, valores possívies: Africa, APAC, Canada, EMEA, EU, LATAM, US, Nullable:  Não',
# MAGIC     market2 STRING
# MAGIC     COMMENT 'Descrição:  Outra coluna relacionada a informações de mercado, Origem:  Master data, Formato: string, Transformação:  Extraído da tabela original, sem ajustes, Mercado onde a loja opera, valores possívies: Africa, APAC, Canada, EMEA, EU, LATAM, US Mercado onde a loja opera, valores possívies: Africa, APAC, EMEA, EU, LATAM, North America, Nullable:  Não',
# MAGIC     region STRING
# MAGIC     COMMENT 'Descrição:  A região onde o pedido foi feito, Origem:  Master data, Formato: string, Transformação:  Extraído da tabela original, sem ajustes, Mercado onde a loja opera, valores possívies: Africa, APAC, EMEA, EU, LATAM, North America Nome da região, nomes possíveis são: Africa, Canada, Caribbean, Central, Central Asia, East, EMEA, North, North Asia, Oceania, South, Southeast Asia, West, Nullable:  Não'
# MAGIC   ) AS
# MAGIC SELECT
# MAGIC   country,
# MAGIC   LOWER(CONCAT(
# MAGIC     REPLACE(country, ' ', '_'), 
# MAGIC     ROW_NUMBER() OVER (ORDER BY country)
# MAGIC   )) AS store_code
# MAGIC , Market
# MAGIC FROM (
# MAGIC   SELECT DISTINCT country
# MAGIC   , Market
# MAGIC   FROM workspace.default.superstore_masterdata
# MAGIC ) AS subquery
# MAGIC WHERE 
# MAGIC   NOT (country = 'Mongolia' AND Market = 'EMEA')
# MAGIC   AND NOT (country = 'Austria' AND Market = 'EU')
# MAGIC -- dim_timetable
# MAGIC CREATE MATERIALIZED VIEW `workspace`.`default`.`dim_timetable`
# MAGIC   (
# MAGIC
# MAGIC     date DATE
# MAGIC     COMMENT 'Descrição:  Data específica a ser conectada para análises históricas, Origem:  Gerado sistematicamente, Formato: date, Transformação:  Criado no processo de etl, para permitir conexões , Número decimal, representando a porcentagem de desconto aplicado ao produto. É possível que seja zero, uma vez que é possível vender um produto sem desconto. No entanto, não deveria ser maior que 1, uma vez que não é possível obter mais de 100% de desconto Data em formato de 10 dígitos, separada por - na ordem YYYY-MM-DD, Nullable:  Não',
# MAGIC     year INT
# MAGIC     COMMENT 'Descrição:  Ano da data, Origem:  Gerado sistematicamente, Formato: integer, Transformação:  Extraído da data na tabela , Data em formato de 10 dígitos, separada por - na ordem YYYY-MM-DD Numéro inteiro, maior que 0, entre 2011 e 2014, Nullable:  Não',
# MAGIC     month INT
# MAGIC     COMMENT 'Descrição:  Mês da data , Origem:  Gerado sistematicamente, Formato: integer, Transformação:  Extraído da data na tabela , Numéro inteiro, maior que 0, entre 2011 e 2014 Numéro inteiro, maior que 0, entre 1 e 12, Nullable:  Não',
# MAGIC     day INT
# MAGIC     COMMENT 'Descrição:  Dia da data, Origem:  Gerado sistematicamente, Formato: integer, Transformação:  Extraído da data na tabela , Numéro inteiro, maior que 0, entre 1 e 12 Numéro inteiro, maior que 0, entre 1 e 31, Nullable:  Não',
# MAGIC     quarter INT
# MAGIC     COMMENT 'Descrição:  Trimestre da data , Origem:  Gerado sistematicamente, Formato: integer, Transformação:  Extraído da data na tabela , Numéro inteiro, maior que 0, entre 1 e 53 Numéro inteiro, maior que 0, entre 1 e 4, Nullable:  Não',
# MAGIC     semester INT
# MAGIC     COMMENT 'Descrição:  Semestre da data , Origem:  Gerado sistematicamente, Formato: integer, Transformação:  Extraído da data na tabela , Numéro inteiro, maior que 0, entre 1 e 4 Numéro inteiro, maior que 0, entre 1 e 2, Nullable:  Não',
# MAGIC     year_month STRING
# MAGIC     COMMENT 'Descrição:  Mês e ano da data , Origem:  Gerado sistematicamente, Formato: string, Transformação:  Extraído da data na tabela , Numéro inteiro, maior que 0, entre 1 e 31 String com a concatenação de ano e mês com um hífem, seguindo o padrão YYYY-MM , Nullable:  Não',
# MAGIC     week_num INT
# MAGIC     COMMENT 'Descrição:  Semestre da data , Origem:  Gerado sistematicamente, Formato: integer, Transformação:  Extraído da data na tabela , Numéro inteiro, maior que 0, entre 1 e 4 Numéro inteiro, maior que 0, entre 1 e 2, Nullable:  Não'
# MAGIC   ) AS
# MAGIC SELECT
# MAGIC   CAST(data_gerada AS DATE) AS `date`,
# MAGIC   YEAR(data_gerada) AS `year`,
# MAGIC   MONTH(data_gerada) AS `month`,
# MAGIC   DAY(data_gerada) AS `day`,
# MAGIC   QUARTER(data_gerada) AS `quarter`,
# MAGIC   CASE
# MAGIC     WHEN MONTH(data_gerada) <= 6 THEN 1
# MAGIC     ELSE 2
# MAGIC   END AS semester,
# MAGIC   -- Formata a data para texto 'YYYY-MM'
# MAGIC   DATE_FORMAT(data_gerada, 'yyyy-MM') AS year_month,
# MAGIC   WEEKOFYEAR(data_gerada) AS week_num
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       explode(sequence(DATE '2011-01-01', DATE '2014-12-31', INTERVAL 1 DAY)) AS data_gerada
# MAGIC   )
# MAGIC -- fact_order_items
# MAGIC CREATE MATERIALIZED VIEW `workspace`.`default`.`fact_order_items`
# MAGIC   (
# MAGIC
# MAGIC     order_item_id STRING
# MAGIC     COMMENT 'Descrição:  Um identificador único para cada linha no conjunto de dados, Origem:  Master data, Formato: string, Transformação:  Extraído da tabela original, sem ajustes, Numéro inteiro, maior que 0. String de caracteres com múltiplos valores possíveis e mínimo de 7, Nullable:  Não',
# MAGIC     order_id STRING
# MAGIC     COMMENT 'Descrição:  Um identificador único para cada pedido, Origem:  Master data, Formato: string, Transformação:  Extraído da tabela original, sem ajustes, String de caracteres com múltiplos valores possíveis e mínimo de 7 String de caracteres com múltiplos valores possíveis e mínimo de 9 caracteres, Nullable:  Não',
# MAGIC     product_id STRING
# MAGIC     COMMENT 'Descrição:  Um identificador único para cada produto, Origem:  Master data, Formato: string, Transformação:  Extraído da tabela original, sem ajustes, String de caracteres com múltiplos valores possíveis e mínimo de 9 caracteres String de 16 caracteres, alfanumérico, Nullable:  Não',
# MAGIC     item_quantity STRING
# MAGIC     COMMENT 'Descrição:  A quantidade de produtos pedidos, Origem:  Master data, Formato: integer, Transformação:  Extraído da tabela original, sem ajustes, String de 16 caracteres, alfanumérico Numéro inteiro, maior que 0. Por natureza não há máximo para uma compra, mas números muito acima de um range plausível podem ser identificados como erros ou outliers. , Nullable:  Não',
# MAGIC     item_sale DECIMAL(18, 2)
# MAGIC     COMMENT 'Descrição:  O valor total de vendas para o item no pedido, Origem:  Master data, Formato: float, Transformação:  Convertido de int para float, pois é mais adeqaudo para valores financeiros. Soma todos os valores sales da tabela order items, Número decimal. Poder ser negativo ou positivo, uma vez que é possível vender um produto obtendo prejuízo com a venda Número decimal, positivo. Não deve ser zero, visto que um pedido não deveria existir sem nenhum tipo de produto vendido. , Nullable:  Não',
# MAGIC     item_profit DOUBLE
# MAGIC     COMMENT 'Descrição:  O lucro gerado com o produto dentro deste pedido , Origem:  Master data, Formato: float, Transformação:  Extraído da tabela original, sem ajustes, Numéro inteiro, maior que 0. Por natureza não há máximo para uma compra, mas números muito acima de um range plausível podem ser identificados como erros ou outliers.  Número decimal. Poder ser negativo ou positivo, uma vez que é possível vender um produto obtendo prejuízo com a venda, Nullable:  Não',
# MAGIC     item_discount DECIMAL(18, 2)
# MAGIC     COMMENT 'Descrição:  O desconto aplicado ao pedido, Origem:  Master data, Formato: float, Transformação:  Extraído da tabela original, sem ajustes, Número decimal. Poder ser negativo ou positivo, uma vez que é possível vender um produto obtendo prejuízo com a venda Número decimal, representando a porcentagem de desconto aplicado ao produto. É possível que seja zero, uma vez que é possível vender um produto sem desconto. No entanto, não deveria ser maior que 1, uma vez que não é possível obter mais de 100% de desconto, Nullable:  Sim'
# MAGIC   ) AS
# MAGIC SELECT
# MAGIC   vendas.`Row ID` AS order_item_id,
# MAGIC   vendas.`Order ID` AS order_id,
# MAGIC   vendas.`Product ID` AS product_id,
# MAGIC   vendas.`Quantity` AS item_quantity,
# MAGIC   CAST(vendas.`Sales` AS DECIMAL(18, 2)) AS item_sale,
# MAGIC   ROUND(vendas.`Profit`, 2) AS item_profit,
# MAGIC   TRY_CAST(vendas.`Discount` AS DECIMAL(18, 2)) AS item_discount
# MAGIC FROM
# MAGIC   workspace.default.superstore_masterdata AS vendas
# MAGIC -- fact_orders
# MAGIC CREATE MATERIALIZED VIEW `workspace`.`default`.`fact_orders`
# MAGIC   (
# MAGIC
# MAGIC     order_id STRING
# MAGIC     COMMENT 'Descrição:  Um identificador único para cada pedido, Origem:  Master data, Formato: string, Transformação:  Extraído da tabela original, sem ajustes, Data em formato de 10 dígitos, separada por - na ordem YYYY-MM-DD String de caracteres com múltiplos valores possíveis e mínimo de 9 caracteres, Nullable:  Não',
# MAGIC     ship_date STRING
# MAGIC     COMMENT 'Descrição:  A data em que o pedido foi enviado, Origem:  Master data, Formato: date, Transformação:  Convertido de date time para date, uma vez que a granularidade de hora não é necessária, Número decimal, positivo. Não deve ser zero, visto que um pedido não deveria existir sem nenhum tipo de produto vendido.  Data em formato de 10 dígitos, separada por - na ordem YYYY-MM-DD, Nullable:  Não',
# MAGIC     store_code STRING
# MAGIC     COMMENT 'Descrição:  Identificador único da loja, Origem:  Gerado sistematicamente, Formato: string, Transformação:  Criado no processo de etl, para permitir conexões , Numério decimal, positivo. Pode ser zero, caso não haja custo para envio.  Criado a partir da primeira palavra do nome do país, somado a um número de 1 a 147 , Nullable:  Não',
# MAGIC     priority_level INT
# MAGIC     COMMENT 'Descrição:  Chave que identifica a prioridade do pedido, Origem:  Gerado sistematicamente, Formato: integer, Transformação:  Criado no processo de etl, para permitir conexões , String de caracteres com múltiplos valores possíveis e mínimo de 9 caracteres Numério decimal, positivo de 1 a 4, Nullable:  Não',
# MAGIC     order_date STRING
# MAGIC     COMMENT 'Descrição:  A data em que o pedido foi feito, Origem:  Master data, Formato: date, Transformação:  Convertido de date time para date, uma vez que a granularidade de hora não é necessária, String de caracteres com 17 valores possívies dentro do universo de produtos para escritórios Data em formato de 10 dígitos, separada por - na ordem YYYY-MM-DD, Nullable:  Não',
# MAGIC     ship_code INT
# MAGIC     COMMENT 'Descrição:  O modo de envio usado para o pedido, Origem:  Master data, Formato: string, Transformação:  Extraído da tabela original, sem ajustes, Criado a partir da primeira palavra do nome do país, somado a um número de 1 a 147  String de caracteres com as opções First Class, Same Day, Second Class, e Standart Class, Nullable:  Não',
# MAGIC     order_sale DECIMAL(18, 2)
# MAGIC     COMMENT 'Descrição:  O valor total de vendas para o pedido, Origem:  Master data, Formato: float, Transformação:  Convertido de int para float, pois é mais adeqaudo para valores financeiros. Soma todos os valores sales da tabela order items, Número decimal. Poder ser negativo ou positivo, uma vez que é possível vender um produto obtendo prejuízo com a venda Número decimal, positivo. Não deve ser zero, visto que um pedido não deveria existir sem nenhum tipo de produto vendido. , Nullable:  Não',
# MAGIC     order_profit DOUBLE
# MAGIC     COMMENT 'Descrição:  O lucro gerado com o pedido, Origem:  Master data, Formato: float, Transformação:  Soma o valor profit de todos os items presentes no pedido, a partir da tabela order_items, Numério decimal, positivo de 1 a 4 Número decimal. Poder ser negativo ou positivo, uma vez que é possível vender um produto obtendo prejuízo com a venda, Nullable:  Não',
# MAGIC     ship_cost DOUBLE
# MAGIC     COMMENT 'Descrição:  O custo de envio para o pedido, Origem:  Master data, Formato: float, Transformação:  Extraído da tabela original, sem ajustes, Data em formato de 10 dígitos, separada por - na ordem YYYY-MM-DD Numério decimal, positivo. Pode ser zero, caso não haja custo para envio. , Nullable:  Não'
# MAGIC   ) AS
# MAGIC CREATE MATERIALIZED VIEW fact_orders AS
# MAGIC SELECT DISTINCT
# MAGIC   vendas.`Order ID` as order_id,
# MAGIC   vendas.`Customer ID` as customer_id
# MAGIC   , DATE_FORMAT(vendas.`Ship Date`, 'yyyy-MM-dd') AS ship_date
# MAGIC   , stores.store_code
# MAGIC   , priority.priority_level
# MAGIC   , DATE_FORMAT(vendas.`Order Date`, 'yyyy-MM-dd') AS order_date
# MAGIC   , envio.ship_code
# MAGIC   , CAST(SUM(vendas.Sales) AS DECIMAL(18, 2)) AS order_sale
# MAGIC   , ROUND(sum(vendas.Profit), 2) as order_profit
# MAGIC   , ROUND(sum(vendas.`Shipping Cost`), 2) as ship_cost
# MAGIC FROM workspace.default.superstore_masterdata AS vendas
# MAGIC LEFT JOIN workspace.default.dim_shipping AS envio
# MAGIC   ON vendas.`Ship Mode` = envio.ship_mode
# MAGIC LEFT JOIN workspace.default.dim_priority AS priority
# MAGIC   ON vendas.`Order Priority` = priority.priority
# MAGIC LEFT JOIN workspace.default.dim_stores AS stores
# MAGIC   ON vendas.`Country` = stores.country
# MAGIC GROUP BY 1,2,3,4,5,6,7;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Registros da execução dos pipelines
# MAGIC ![Screenshot 2025-11-30 at 13.54.13.png](./Screenshot 2025-11-30 at 13.54.13.png "Screenshot 2025-11-30 at 13.54.13.png")
# MAGIC ![Screenshot 2025-11-30 at 13.54.30.png](./Screenshot 2025-11-30 at 13.54.30.png "Screenshot 2025-11-30 at 13.54.30.png")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Registro de linhagem dos dados
# MAGIC
# MAGIC No tocante a linhagem dos dados, o cenário executado tem uma particularidade: todos as tabelas criadas no esquema final são originários da mesma tabela (base original do kaggle) seja com os campos orginais ou com algum tratamento (conforme registrado no dicionário dos dados, utilizando o campo de origem), com a exceção da tabela dim_timetable, que é criada manualmente. 
# MAGIC
# MAGIC O ponto de atenção aqui é que, num cenário real de implementação essa provavemente não seria a realidade. com diferentes domínios sendo alimentados por sistemas diferentes. Dados de pedidos e faturas seriam originados em um ERP, enquanto informações de clienets viriam de um CRM, informações de shipping viriam de plataformas logísticas, entre outros. 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #PARTE 3 - Análise de Qualidade
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Para a execução dos descritivos a seguir, comparamos os dados de cada coluna e tabela com os valores esperados, conforme descrito nos campos no catálogo de dados. 
# MAGIC
# MAGIC Em linhas gerais, o dataset não apresenta gaps em qualidade de dados, mas isso é consequência da base já ter sido tratada antes da publicação no Kaggle, o que é um cenário diferente da realidade de projetos de negócio. 

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Tabela Customers
# MAGIC

# COMMAND ----------

customers = spark.sql("SELECT * FROM workspace.default.dim_customers")
customers = customers.toPandas()
customers.describe()

# COMMAND ----------

customers.info()

# COMMAND ----------

customers['segment'].unique()

# COMMAND ----------

# MAGIC %md
# MAGIC **Valores esperados:** 
# MAGIC * customer_id - Número inteiro, positivo e maior que zero
# MAGIC * customer_name - String de caracteres com múltiplas palavras
# MAGIC * segment - Pode conter or valores Consumer, Corporate ou Home Office
# MAGIC
# MAGIC **Conclusão:**
# MAGIC Os valores coletados estão em compliance com as regras de dataquality estabelecidas
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Tabela Priority
# MAGIC

# COMMAND ----------

priority = spark.sql("SELECT * FROM workspace.default.dim_priority")
priority = priority.toPandas()
priority.describe()

# COMMAND ----------

priority.info()

# COMMAND ----------

priority['priority_level'].unique()

# COMMAND ----------

priority['priority'].unique()

# COMMAND ----------

# MAGIC %md
# MAGIC **Valores Esperados:**
# MAGIC * priority -	String de caracteres com as opções Critical, HIgh, Low, e Medium 
# MAGIC * priority_level -	Numério `inteiro`, positivo de 1 a 4
# MAGIC
# MAGIC **Conclusão:**
# MAGIC Os valores coletados estão em compliance com as regras de dataquality estabelecidas
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Tabela Product
# MAGIC

# COMMAND ----------

products = spark.sql("SELECT * FROM workspace.default.dim_products")
products = products.toPandas()
products.describe()

# COMMAND ----------

products.info()

# COMMAND ----------

products['category'].unique()


# COMMAND ----------

products['subcategory'].unique()

# COMMAND ----------

# MAGIC %md
# MAGIC **Valores Esperados:**
# MAGIC * product_id -	String de 16 caracteres, alfanumérico
# MAGIC * product_name -	String de caracteres com múltiplas palavras
# MAGIC * product _price -	Numério decimal, positivo. Não pode ser zero. 
# MAGIC * category	String de caracteres com as opções Furniture, Office supplies e Technology
# MAGIC * sub_category -	String de caracteres com 17 valores possívies dentro do universo de produtos para escritórios
# MAGIC
# MAGIC **Conclusão:**
# MAGIC Os valores coletados estão em compliance com as regras de dataquality estabelecidas
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Tabela Shipping
# MAGIC

# COMMAND ----------

ship = spark.sql("SELECT * FROM workspace.default.dim_shipping")
ship = ship.toPandas()
ship.describe()

# COMMAND ----------

ship.info()

# COMMAND ----------

ship['ship_mode'].unique()

# COMMAND ----------

ship['ship_code'].unique()

# COMMAND ----------

# MAGIC %md
# MAGIC **Valores Esperados:**
# MAGIC * ship_mode - 	String de caracteres com as opções First Class, Same Day, Second Class, e Standart Class
# MAGIC * ship_code -	Numéro inteiro, maior que 0.
# MAGIC
# MAGIC **Conclusão:**
# MAGIC Os valores coletados estão em compliance com as regras de dataquality estabelecidas
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Tabela Stores
# MAGIC
# MAGIC

# COMMAND ----------

stores = spark.sql("SELECT * FROM workspace.default.dim_stores")
stores = stores.toPandas()
stores.describe()

# COMMAND ----------

stores['country'].unique()


# COMMAND ----------

stores['store_code'].unique()


# COMMAND ----------

stores['Market'].unique()

# COMMAND ----------

stores.info()

# COMMAND ----------

# MAGIC %md
# MAGIC **Valores Esperados:**
# MAGIC * store_code - 	Criado a partir da primeira palavra do nome do país, somado a um número de 1 a 147 
# MAGIC * market - 	Mercado onde a loja opera, valores possívies: Africa, APAC, Canada, EMEA, EU, LATAM, US
# MAGIC * country - 	Nome do país, Múltiplas possibilidades
# MAGIC
# MAGIC **Conclusão:**
# MAGIC Os valores coletados estão em compliance com as regras de dataquality estabelecidas
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 6. Tabela Time Table
# MAGIC

# COMMAND ----------

time = spark.sql("SELECT * FROM workspace.default.dim_timetable")
time = time.toPandas()
time.describe()

# COMMAND ----------

time.info()

# COMMAND ----------

# MAGIC %md
# MAGIC **Valores Esperados:**
# MAGIC * year -	Numéro inteiro, maior que 0, entre 2011 e 2014
# MAGIC * month	- Numéro inteiro, maior que 0, entre 1 e 12
# MAGIC * day	- Numéro inteiro, maior que 0, entre 1 e 31
# MAGIC * year_month -	String com a concatenação de ano e mês com um hífem, seguindo o padrão YYYY-MM 
# MAGIC * week_num -	Numéro inteiro, maior que 0, entre 1 e 53
# MAGIC * quarter -	Numéro inteiro, maior que 0, entre 1 e 4
# MAGIC * semester - 	Numéro inteiro, maior que 0, entre 1 e 2
# MAGIC
# MAGIC **Conclusão:**
# MAGIC Os valores coletados estão em compliance com as regras de dataquality estabelecidas
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 7. Tabela Order Items
# MAGIC

# COMMAND ----------

items = spark.sql("SELECT * FROM workspace.default.fact_order_items")
items = items.toPandas()
items.describe()

# COMMAND ----------

items.info()

# COMMAND ----------

# MAGIC %md
# MAGIC **Valores Esperados:**
# MAGIC * order_item_id -	String de caracteres com múltiplos valores possíveis e * mínimo de 7
# MAGIC order_id -	String de caracteres com múltiplos valores possíveis e mínimo de 9 caracteres
# MAGIC * product_id -	String de 16 caracteres, alfanumérico
# MAGIC * item_quantity - 	Numéro inteiro, maior que 0. Por natureza não há máximo para uma compra, mas números muito acima de um range plausível podem ser identificados como erros ou outliers. 
# MAGIC * item_sale	- Número decimal, positivo. Não deve ser zero, visto que um pedido não deveria existir sem nenhum tipo de produto vendido. 
# MAGIC * item_profit	- Número decimal. Poder ser negativo ou positivo, uma vez que é possível vender um produto obtendo prejuízo com a venda
# MAGIC * item_discount	- Número decimal, representando a porcentagem de desconto aplicado ao produto. É possível que seja zero, uma vez que é possível vender um produto sem desconto. No entanto, não deveria ser maior que 1, uma vez que não é possível obter mais de 100% de desconto
# MAGIC
# MAGIC **Conclusão:**
# MAGIC Os valores coletados estão em compliance com as regras de dataquality estabelecidas
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 8. Tabela Order 

# COMMAND ----------

orders = spark.sql("SELECT * FROM workspace.default.fact_orders")
orders = orders.toPandas()
orders.describe()

# COMMAND ----------

orders.info()

# COMMAND ----------

# MAGIC %md
# MAGIC **Valores Esperados:**
# MAGIC * order_id -	String de caracteres com múltiplos valores possíveis e mínimo de 9 caracteres
# MAGIC * ship_date -	Data em formato de 10 dígitos, separada por - na ordem YYYY-MM-DD
# MAGIC * store_code -	Criado a partir da primeira palavra do nome do país, somado a um número de 1 a 147 
# MAGIC * priority_level -	Numério decimal, positivo de 1 a 4
# MAGIC * order_date -	Data em formato de 10 dígitos, separada por - na ordem YYYY-MM-DD
# MAGIC * ship_code	- String de caracteres com as opções First Class, Same Day, Second Class, e Standart Class
# MAGIC * order_sale - 	Número decimal, positivo. Não deve ser zero, visto que um pedido não deveria existir sem nenhum tipo de produto vendido. 
# MAGIC * order_profit - 	Número decimal. Poder ser negativo ou positivo, uma vez que é possível vender um produto obtendo prejuízo com a venda
# MAGIC * ship_cost	- Numério decimal, positivo. Pode ser zero, caso não haja custo para envio. 
# MAGIC
# MAGIC **Conclusão:**
# MAGIC Os valores coletados estão em compliance com as regras de dataquality estabelecidas
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #PARTE 4 - Análise de dados e resolução do problema

# COMMAND ----------

# MAGIC %md
# MAGIC Para a análise dos dados gerados, nos propomos a responder as seguintes perguntas: 
# MAGIC
# MAGIC - Quais categorias têm a margem de lucro (Lucro / Vendas) mais alta ou mais baixa?
# MAGIC - Quem são os clientes que fizeram o maior volume de compras ou que geraram o maior valor de vendas/lucro?
# MAGIC - Existe oportunidade de melhoria do resultado de lucro no portfolio de produtos?
# MAGIC - Qual é o desempenho de vendas e lucro por mercado?
# MAGIC - Quais as combinações de produto/região que têm gerado a maior margem de lucro?

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1 - Quais categorias têm a margem de lucro (Lucro / Vendas) mais alta ou mais baixa?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t1.category, sum(t2.item_sale) as sales, ROUND(sum(t2.item_profit),2) as profit, ROUND(profit / sales, 2) as margin
# MAGIC FROM workspace.default.dim_products as t1
# MAGIC LEFT JOIN workspace.default.fact_order_items as t2
# MAGIC ON t1.product_id = t2.product_id
# MAGIC GROUP BY 1

# COMMAND ----------

pdf = _sqldf.toPandas()

fig, ax = plt.subplots(figsize=(10, 6))

categories = pdf['category']
sales = pdf['sales']
profit = pdf['profit']
margin = pdf['margin']

bar_width = 0.35
x = np.arange(len(categories))

bars1 = ax.bar(
    x - bar_width / 2,
    sales,
    bar_width,
    label='Vendas',
    color='skyblue'
)
bars2 = ax.bar(
    x + bar_width / 2,
    profit,
    bar_width,
    label='Lucro',
    color='lightgreen'
)

for bar in bars1 + bars2:
    height = bar.get_height()
    ax.annotate(
        f'{height:.2f}',
        xy=(bar.get_x() + bar.get_width() / 2, height),
        xytext=(0, 3),
        textcoords="offset points",
        ha='center',
        va='bottom',
        fontsize=10
    )

def get_margin_color(m, margins):
    max_margin = max(margins)
    min_margin = min(margins)
    if m == max_margin:
        return 'green'
    elif m == min_margin:
        return 'red'
    else:
        return 'yellow'

y_box = -float(max(float(sales.max()), float(profit.max()))) * 0.1  # Position below x-axis
for i, m in enumerate(margin):
    color = get_margin_color(m, margin)
    ax.text(
        x[i],
        y_box,
        f'Margin: {float(m):.2f}',
        ha='center',
        va='top',
        fontsize=10,
        bbox=dict(boxstyle='round,pad=0.3', facecolor=color, edgecolor='white', alpha=0.7)
    )

ax.set_xlabel('Categoria')
ax.set_ylabel('Valor (Vendas e Lucro)')
ax.set_xticks(x)
ax.set_xticklabels(categories)
ax.legend(loc='upper left')

plt.title('Vendas, Lucro e Margem, por Categoria')
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC **1 - Quais categorias têm a margem de lucro (Lucro / Vendas) mais alta ou mais baixa?**
# MAGIC
# MAGIC Todas as 3 categorias demonstram um alto volume de vendas, variando entre 4.1 e 5.1 mi de dólares, com a categoria de Tecnologia liderando o ranking. No entando, no ângulo de margem de lucro, temos Móveis (Furniture) com o pior resultado de margem de Lucro (7%). A recomendação aqui seria de investigar os itens e lojas dentro dessa categoria para identificar principais ofensores e traçar planos de ação. 

# COMMAND ----------

# MAGIC %md
# MAGIC ![Screenshot 2025-12-05 at 14.30.21.png](./Screenshot 2025-12-05 at 14.30.21.png "Screenshot 2025-12-05 at 14.30.21.png")
# MAGIC
# MAGIC Registro do gráfico gerado

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2 - Qual segmeto faz o maior volume de compras ou que geraram o maior valor de vendas/lucro?

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH total AS (
# MAGIC   SELECT sum(order_sale) AS sales
# MAGIC   FROM workspace.default.fact_orders
# MAGIC ),
# MAGIC totalp AS (
# MAGIC   SELECT sum(order_profit) AS profit
# MAGIC   FROM workspace.default.fact_orders
# MAGIC )
# MAGIC SELECT
# MAGIC   t1.segment,
# MAGIC   sum(t2.order_sale) AS sales,
# MAGIC   ROUND((sum(t2.order_sale) / (SELECT sales FROM total))*100, 2) AS sales_percentage,
# MAGIC   ROUND(sum(t2.order_profit), 2) AS profit,
# MAGIC   ROUND((sum(t2.order_profit) / (SELECT profit FROM totalp))*100, 2) AS profit_percentage,
# MAGIC   ROUND(profit/ sales, 4) AS profit_margin
# MAGIC FROM workspace.default.dim_customers AS t1
# MAGIC LEFT JOIN workspace.default.fact_orders AS t2
# MAGIC   ON t1.customer_id = t2.customer_id
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

pdf = _sqldf.toPandas()

segments = pdf['segment']
sales_percentage = pdf['sales_percentage'].astype(float)
profit_percentage = pdf['profit_percentage'].astype(float)
profit_margin = pdf['profit_margin'].astype(float)

bar_width = 0.35
x = np.arange(len(segments))

fig, ax = plt.subplots(figsize=(10, 6))

bars1 = ax.bar(
    x - bar_width / 2,
    sales_percentage,
    bar_width,
    label='Share das Vendas',
    color='skyblue'
)
bars2 = ax.bar(
    x + bar_width / 2,
    profit_percentage,
    bar_width,
    label='Share de Lucro',
    color='lightgreen'
)

for bar in bars1 + bars2:
    height = bar.get_height()
    ax.annotate(
        f'{height:.2f}%',
        xy=(bar.get_x() + bar.get_width() / 2, height),
        xytext=(0, 3),
        textcoords="offset points",
        ha='center',
        va='bottom',
        fontsize=10
    )

y_box = -max(sales_percentage.max(), profit_percentage.max()) * 0.15
for i, m in enumerate(profit_margin):
    ax.text(
        x[i],
        y_box,
        f'Margem de Lucro: {m:.4f}',
        ha='center',
        va='top',
        fontsize=10,
        bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', edgecolor='black', alpha=0.8)
    )

ax.set_xlabel('Segemento')
ax.set_ylabel('Percentage (%)')
ax.set_xticks(x)
ax.set_xticklabels(segments)
ax.legend(loc='upper right')

plt.title('Concentração de Vendas e Lucro por Segmento de cliente')
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **2 - Qual segmeto faz o maior volume de compras ou que geraram o maior valor de vendas/lucro?**
# MAGIC A partir do resultado encontrado, vemos que o segmento Consumer é responsável pela maior parte dos volume de receita e também lucro, seguido de Corporate e Home office, respectivamente. 
# MAGIC Um ponto interessante é que a concentração de vendas e lucro é muito semelhante em cada segmento, fato confirmado por uma margem de luvro muito próxima, na casa dos ~12%, indicando que ainda que os segmentos sejam diferentes em volume, são quase igualmente "lucrativos", com o segmento Home office tendo um elev destaque, com o valor marginalmente superior de ~11,99%
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC ![Screenshot 2025-12-05 at 14.30.53.png](./Screenshot 2025-12-05 at 14.30.53.png "Screenshot 2025-12-05 at 14.30.53.png")
# MAGIC Registro do gráfico gerado

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3 - Existe oportunidade de melhoria do resultado de lucro no portfolio de produtos?

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH total AS (
# MAGIC   SELECT sum(item_sale) AS sales
# MAGIC   FROM workspace.default.fact_order_items
# MAGIC ),
# MAGIC totalp AS (
# MAGIC   SELECT sum(item_profit) AS profit
# MAGIC   FROM workspace.default.fact_order_items
# MAGIC )
# MAGIC SELECT
# MAGIC   t1.product_id
# MAGIC   , t1.product_name
# MAGIC   , t1.category,
# MAGIC   sum(t2.item_sale) AS sales,
# MAGIC   ROUND((sum(t2.item_sale) / (SELECT sales FROM total))*100, 2) AS sales_percentage,
# MAGIC   ROUND(sum(t2.item_profit), 2) AS profit,
# MAGIC   ROUND((sum(t2.item_profit) / (SELECT profit FROM totalp))*100, 2) AS profit_percentage
# MAGIC FROM workspace.default.dim_products AS t1
# MAGIC LEFT JOIN workspace.default.fact_order_items AS t2
# MAGIC   ON t1.product_id = t2.product_id
# MAGIC GROUP BY 1,2,3
# MAGIC ORDER BY 6 DESC

# COMMAND ----------

pdf = _sqldf.toPandas()

fig = px.scatter(
    pdf,
    x='sales',
    y='profit',
    color='category',
    hover_name='product_name',
    labels={'sales': 'Vendas', 'profit': 'Lucro', 'category': 'Categoria'},
    title='Venda e Lucro por Produto e categoria',
    size_max=80
)

mean_sales = pdf['sales'].mean()
mean_profit = pdf['profit'].mean()

fig.add_vline(
    x=mean_sales,
    line_dash="dash",
    line_color="grey",
    line_width=1,
    annotation_text="Média de vendas",
    annotation_position="top right"
)
fig.add_hline(
    y=mean_profit,
    line_dash="dash",
    line_color="grey",
    line_width=1,
    annotation_text="Média de lucro",
    annotation_position="top right"
)

fig.add_vline(x=0, line_color="grey", line_width=1)
fig.add_hline(y=0, line_color="grey", line_width=1)

fig.update_layout(
    legend_title_text='Categoria',
    legend=dict(x=0.99, y=0.01, xanchor='right', yanchor='bottom'),
    width=900,
    height=600,
    plot_bgcolor='white',
    paper_bgcolor='white'
)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **3 - Existe oportunidade de melhoria do resultado de lucro no portfolio de produtos?**
# MAGIC
# MAGIC A partir do gráfico interativo constrído, conseguimos ter uma visão interessante sobre o estado de vendas e lucro. Em resumo, todos os items na área negativa do eixo Y correpondem a items vendidos, porém com margem de lucro negativa. Items como a Cubify X 3D Printer e Bevis conference Table apresentam um volume de vendas superior a 10k, porém com um prejuízo alto acumulado. 
# MAGIC
# MAGIC Com o uso do gráfico interativo é possível montar frentes de investigação que analisem em detalhes os produtos de margem negativa e identifiquem planos de ação para a correção de rota. 

# COMMAND ----------

# MAGIC %md
# MAGIC Registro do gráfico gerado
# MAGIC ![Screenshot 2025-12-05 at 14.31.41.png](./Screenshot 2025-12-05 at 14.31.41.png "Screenshot 2025-12-05 at 14.31.41.png")
# MAGIC Recortes interativos, executados para explorar os resultados
# MAGIC ![Screenshot 2025-12-04 at 09.35.23.png](./Screenshot 2025-12-04 at 09.35.23.png "Screenshot 2025-12-04 at 09.35.23.png")![Screenshot 2025-12-04 at 09.35.31.png](./Screenshot 2025-12-04 at 09.35.31.png "Screenshot 2025-12-04 at 09.35.31.png")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4 - Qual é o desempenho de vendas e lucro por mercado?

# COMMAND ----------

# MAGIC %%sql
# MAGIC WITH total AS (
# MAGIC   SELECT sum(order_sale) AS sales
# MAGIC   FROM workspace.default.fact_orders
# MAGIC ),
# MAGIC totalp AS (
# MAGIC   SELECT sum(order_profit) AS profit
# MAGIC   FROM workspace.default.fact_orders
# MAGIC )
# MAGIC SELECT
# MAGIC   t1.market,
# MAGIC   sum(t2.order_sale) AS sales,
# MAGIC   ROUND((sum(t2.order_sale) / (SELECT sales FROM total))*100, 1) AS sales_percentage,
# MAGIC   ROUND(sum(t2.order_profit), 2) AS profit,
# MAGIC   ROUND((sum(t2.order_profit) / (SELECT profit FROM totalp))*100, 1) AS profit_percentage
# MAGIC FROM workspace.default.dim_stores AS t1
# MAGIC LEFT JOIN workspace.default.fact_orders AS t2
# MAGIC   ON t1.store_code = t2.store_code
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

pdf = _sqldf.toPandas()

markets = pdf['market']
sales_percentage = pdf['sales_percentage'].astype(float)
profit_percentage = pdf['profit_percentage'].astype(float)

bar_width = 0.6
x = np.arange(len(markets))

fig, ax = plt.subplots(figsize=(10, 6))
bars = ax.bar(
    x,
    sales_percentage,
    bar_width,
    color='skyblue',
    label='Sales %'
)

for i, bar in enumerate(bars):
    height = bar.get_height()
    ax.annotate(
        f'{height:.1f}%',
        xy=(bar.get_x() + bar.get_width() / 2, height),
        xytext=(0, 3),
        textcoords="offset points",
        ha='center',
        va='bottom',
        fontsize=10
    )

def get_color(pct):
    if pct >= profit_percentage.max() * 0.8:
        return 'green'
    elif pct >= profit_percentage.max() * 0.5:
        return 'yellow'
    else:
        return 'red'

y_box = -max(sales_percentage) * 0.15
for i, pct in enumerate(profit_percentage):
    ax.text(
        x[i],
        y_box,
        f'Margem%: {pct:.1f}',
        ha='center',
        va='top',
        fontsize=10,
        bbox=dict(boxstyle='round,pad=0.3', facecolor=get_color(pct), edgecolor='black', alpha=0.8)
    )

ax.set_xlabel('Market')
ax.set_ylabel('Share de Vendas(%)')
ax.set_xticks(x)
ax.set_xticklabels(markets)
ax.legend(loc='upper right')

plt.title('Concentração de Vendas e Margem de lucro por Mercado')
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **4 - Qual é o desempenho de vendas e lucro por mercado?** 
# MAGIC
# MAGIC Analisando a distribuição de vendas e e as respectivas margem de lucro por região enxergamos um cenário interessante e positivo: com cerca de 50% das vendas concentradas em mercados onde a margem de lucro é mais alta. Como ação imediata, recomendamos focar as investigações em relação  a possíveis caminhos de melhoria de margem em mercados como US e LATAM, visto que pelo o volume de vendas concentrado nessas áreas, o impacto na melhoria de 1% de margem seria alto. 

# COMMAND ----------

# MAGIC %md
# MAGIC ![Screenshot 2025-12-05 at 14.33.08.png](./Screenshot 2025-12-05 at 14.33.08.png "Screenshot 2025-12-05 at 14.33.08.png")
# MAGIC Registro do gráfico construído
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5 - Quais as combinações de produto/mercado que têm gerado a maior margem de lucro?

# COMMAND ----------

# MAGIC %%sql
# MAGIC
# MAGIC WITH total AS (
# MAGIC   SELECT sum(item_sale) AS sales
# MAGIC   FROM workspace.default.fact_order_items
# MAGIC ),
# MAGIC totalp AS (
# MAGIC   SELECT sum(item_profit) AS profit
# MAGIC   FROM workspace.default.fact_order_items
# MAGIC )
# MAGIC SELECT
# MAGIC  t4.market
# MAGIC   , t1.product_id
# MAGIC   , t1.product_name
# MAGIC   ,sum(t2.item_sale) AS sales,
# MAGIC   ROUND((sum(t2.item_sale) / (SELECT sales FROM total))*100, 2) AS sales_percentage,
# MAGIC   ROUND(sum(t2.item_profit), 2) AS profit,
# MAGIC   ROUND((sum(t2.item_profit) / (SELECT profit FROM totalp))*100, 2) AS profit_percentage
# MAGIC FROM workspace.default.dim_products AS t1
# MAGIC LEFT JOIN workspace.default.fact_order_items AS t2
# MAGIC   ON t1.product_id = t2.product_id
# MAGIC LEFT JOIN workspace.default.fact_orders AS t3
# MAGIC   ON t2.order_id = t3.order_id
# MAGIC LEFT JOIN workspace.default.dim_stores AS t4
# MAGIC   ON t3.store_code = t4.store_code
# MAGIC GROUP BY 1,2,3
# MAGIC ORDER BY 4 DESC

# COMMAND ----------

pdf = _sqldf.toPandas()

fig = px.scatter(
    pdf,
    x='sales',
    y='profit',
    color='market',
    hover_name='product_name',
    labels={'sales': 'Vendas', 'profit': 'Lucro', 'market': 'Mercado'},
    title='Venda e Lucro por Produto e Mercado',
    size_max=80
)

median_sales = pdf['sales'].mean()
median_profit = pdf['profit'].mean()

fig.add_vline(
    x=median_sales,
    line_dash="dash",
    line_color="black",
    line_width=1.5,
    annotation_text="Média de vendas",
    annotation_position="top right"
)
fig.add_hline(
    y=median_profit,
    line_dash="dash",
    line_color="gray",
    line_width=1.5,
    annotation_text="Média de lucro",
    annotation_position="top right"
)

fig.add_vline(x=0, line_color="grey", line_width=1)
fig.add_hline(y=0, line_color="grey", line_width=1)

fig.update_layout(
    legend_title_text='Mercado',
    legend=dict(
        orientation="h",
        x=0.5,
        y=1.08,
        xanchor='center',
        yanchor='top'
    ),
    width=900,
    height=600,
    plot_bgcolor='white',
    paper_bgcolor='white'
)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **5 - Quais as combinações de produto/mercado que têm gerado a maior margem de lucro?** 
# MAGIC
# MAGIC De maneira análoga ao gráfico de dispersão da análise 3, o gráfico interativo acima permite a identificação das combinações de produto/mercado com pior performance de lucro, a partit da seleção dos produtos abaixo da linha 0 do eixo y. 
# MAGIC No caso, os produtos Cubefy Cube X 3D printer no US, Motorola Smartphone, Cordless na EMEA e o Hover Stove no EU apresentaram grande resultados de venda, mas com um prejuízo acumulado. É importante investigar os produtos em questão e traçar planos de ação para corrigir a performance. 

# COMMAND ----------

# MAGIC %md
# MAGIC Registro do gráfico gerado
# MAGIC ![Screenshot 2025-12-05 at 14.34.27.png](./Screenshot 2025-12-05 at 14.34.27.png "Screenshot 2025-12-05 at 14.34.27.png")
# MAGIC
# MAGIC Recortes interativos realizados para gerar as conclusões. 
# MAGIC ![Screenshot 2025-12-04 at 14.35.20.png](./Screenshot 2025-12-04 at 14.35.20.png "Screenshot 2025-12-04 at 14.35.20.png")![Screenshot 2025-12-04 at 14.35.28.png](./Screenshot 2025-12-04 at 14.35.28.png "Screenshot 2025-12-04 at 14.35.28.png")![Screenshot 2025-12-04 at 14.36.26.png](./Screenshot 2025-12-04 at 14.36.26.png "Screenshot 2025-12-04 at 14.36.26.png")

# COMMAND ----------

# MAGIC %md
# MAGIC #PARTE 5 - Fechamento e Auto avaliação
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Conclusões gerais 
# MAGIC Com a solução desenhada, foi possível aplicar os principais conceitos e habilidades adquitidos na discplina, executando na prática a constrção de um Data Warehouse e utilização como fonte de dados para a execução de análises e extração do resultado. 
# MAGIC
# MAGIC #### Auto-avaliação 
# MAGIC
# MAGIC - Visão geral da solução
# MAGIC   - Em geral, acredito que a solução construída tem bases sólidas na teoria aplicada, e oferece soluções com performance adequada para o desafio proposto. 
# MAGIC   - Em um cenário real, a infraestrutura de tabelas construídas seria satisfatória para a geração da lista das análises relacionadas as perguntas listadas, e forneceria uma boa fonte de informação para a excução dessas e outras análises no dia a dia do negócio.
# MAGIC
# MAGIC - Diferenças entre cenário construído e realidade
# MAGIC   - NO entanto, é importante destacar alguns gaps no projeto construído. Em linhas gerais, a maior diferença do cenáio trabalhado para um setup de dia a dia vem da falta de normalização e da descentralização das fontes de dados no dia a dia de um negócio. 
# MAGIC   - No cenário trabalhado, todas as informações usada vinham de uma única tabela já normalizada e tratada, que foi quebrada através de múltiplos processos de ETL, de forma a resultar em um modelo estrela otimizado para os processos analíticos. 
# MAGIC   - Ainda que esse cenário tenha permitido a prática do proceso de ETL, ele não traz o legado que normalmente encontramos ao encarar esses desafio: dados vindos de múltiplos sistemas diferentes, em formatos diferentes, tipos de arquivo diferentes, granularidades diferentes entre outros elementos que tornam o processo de construção, alimentação e manutenção de um DW desafiadores. 
# MAGIC
# MAGIC - Oportunidades de aprofundamento e melhoria
# MAGIC   - Além disso, para fins de simplificação de escopo, algumas decisões foram tomadas e premissas assumidas. por exemplo, a opção do resumo das vendas de Store no nível de País. Ao passo que isso reduziu a complexidade dos dados e ainda permitiu o trabalho, no dia a dia a realidade iria exigir o trabalho dos dados em sua completa granularidade, explorando dods os dados num nível cidade, e exigindo também dados sobre as unidades de venda, centros de abastecimentos, e outros elementos que, na base de dados atual, não estão disponíveis. 
# MAGIC
# MAGIC   - Além disso, do ponto de vista de tempo, ainda seria possível introduzir mais detalhes como dia da semana, número do dia no ano, entre outros. 
# MAGIC
# MAGIC - Uso de recursos interativos e de visualização
# MAGIC   - Como elemento positivo, vale reforçar o uso do os gráficos interativos como ferramenta exploratória para a resolução das perguntas, a partir dos daods analisados. 
# MAGIC
# MAGIC   - Essa ecolha foi realizada não só pela facilidade e personalização introduzidas, mas também porque replicam um uso cada vez mais comum das infraestrutura construída: a conexão de dados de um DW como fonte de entrada em ferramentas de self service BI (como Tableau, Power BI, Looker Studio) para permitir que usuários finais, não familiares com o código e a engenharia necessária para a conexão com os sistemas raiz, façam uso de dados de qualidade, tratados e validados, permitindo que toda a organização se baseie nos dados disponíveis para exploração e tomada de decisão. 