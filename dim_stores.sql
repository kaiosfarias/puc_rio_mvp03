-- Please edit the sample below

CREATE MATERIALIZED VIEW dim_stores AS
SELECT
  country,
  LOWER(CONCAT(
    REPLACE(country, ' ', '_'), 
    ROW_NUMBER() OVER (ORDER BY country)
  )) AS store_code
, Market as market
, Market2 as market2
, Region as `region`
FROM (
  SELECT DISTINCT country
  , Market
, Market2
, Region
  FROM workspace.default.superstore_masterdata
) AS subquery