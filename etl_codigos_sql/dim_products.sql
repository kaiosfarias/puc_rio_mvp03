-- Please edit the sample below

CREATE MATERIALIZED VIEW dim_products AS
SELECT 
  `Product ID` as product_id,
  `Product Name` as product_name,
  `Category` as category,
  `Sub-Category` as subcategory,
  ROUND(SUM(Sales) / SUM(Quantity), 2) AS product_price
FROM workspace.default.superstore_masterdata
GROUP BY 
  `Product ID`, 
  `Product Name`, 
  `Category`, 
  `Sub-Category`;