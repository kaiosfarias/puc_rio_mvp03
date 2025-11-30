-- Please edit the sample below

CREATE MATERIALIZED VIEW fact_orders AS
SELECT DISTINCT
  vendas.`Order ID` as order_id
  , DATE_FORMAT(vendas.`Ship Date`, 'yyyy-MM-dd') AS ship_date
  , stores.store_code
  , priority.priority_level
  , DATE_FORMAT(vendas.`Order Date`, 'yyyy-MM-dd') AS order_date
  , envio.ship_code
  , CAST(SUM(vendas.Sales) AS DECIMAL(18, 2)) AS order_sale
  , ROUND(sum(vendas.Profit), 2) as order_profit
  , ROUND(sum(vendas.`Shipping Cost`), 2) as ship_cost
FROM workspace.default.superstore_masterdata AS vendas
LEFT JOIN workspace.default.dim_shipping AS envio
  ON vendas.`Ship Mode` = envio.ship_mode
LEFT JOIN workspace.default.dim_priority AS priority
  ON vendas.`Order Priority` = priority.priority
LEFT JOIN workspace.default.dim_stores AS stores
  ON vendas.`Country` = stores.country
GROUP BY 1,2,3,4,5,6;