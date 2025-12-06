-- Please edit the sample below

CREATE MATERIALIZED VIEW dim_stores AS
SELECT
  country,
  LOWER(CONCAT(
    REPLACE(country, ' ', '_'), 
    ROW_NUMBER() OVER (ORDER BY country)
  )) AS store_code
, Market
FROM (
  SELECT DISTINCT country
  , Market
  FROM workspace.default.superstore_masterdata
) AS subquery
WHERE 
  NOT (country = 'Mongolia' AND Market = 'EMEA')
  AND NOT (country = 'Austria' AND Market = 'EU')