-- Please edit the sample below

CREATE MATERIALIZED VIEW dim_priority AS
SELECT DISTINCT 
  `Order Priority` AS priority,
  CASE 
    WHEN `Order Priority` = 'Critical' THEN 4
    WHEN `Order Priority` = 'High'     THEN 3
    WHEN `Order Priority` = 'Medium'   THEN 2
    WHEN `Order Priority` = 'Low'      THEN 1
    ELSE 0 -- Caso apareça algum valor estranho/nulo, ele recebe 0
  END AS priority_level
FROM workspace.default.superstore_masterdata
ORDER BY priority_level DESC;