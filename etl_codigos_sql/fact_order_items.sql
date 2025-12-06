-- Please edit the sample below

CREATE MATERIALIZED VIEW fact_order_items AS
SELECT vendas.`Row ID` AS order_item_id,
       vendas.`Order ID` AS order_id,
       vendas.`Product ID` AS product_id,
       vendas.`Quantity` AS item_quantity,
       CAST(vendas.`Sales` AS DECIMAL(18,2)) AS item_sale,
       ROUND(vendas.`Profit`, 2) AS item_profit,
       TRY_CAST(vendas.`Discount` AS DECIMAL(18,2)) AS item_discount
FROM workspace.default.superstore_masterdata AS vendas;