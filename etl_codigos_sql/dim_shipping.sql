CREATE MATERIALIZED VIEW dim_shipping AS
SELECT
  `Ship Mode` AS ship_mode,
  ROW_NUMBER() OVER (ORDER BY `Ship Mode`) AS ship_code
FROM (
  SELECT DISTINCT `Ship Mode`
  FROM workspace.default.superstore_masterdata
) AS subquery