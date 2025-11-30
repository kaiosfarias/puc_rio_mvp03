-- Please edit the sample below

CREATE MATERIALIZED VIEW dim_customers AS
SELECT DISTINCT
  `Customer ID` as customer_id,
  `Customer Name` as customer_name,
  `Segment` as segment
FROM workspace.default.superstore_masterdata