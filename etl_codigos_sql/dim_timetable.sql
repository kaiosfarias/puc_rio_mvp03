-- Please edit the sample below
CREATE MATERIALIZED VIEW dim_timetable AS
SELECT 
  CAST(data_gerada AS DATE) AS `date`,
  YEAR(data_gerada) AS `year`,
  MONTH(data_gerada) AS `month`,
  DAY(data_gerada) AS `day`,
  QUARTER(data_gerada) AS `quarter`,
  CASE 
    WHEN MONTH(data_gerada) <= 6 THEN 1 
    ELSE 2 
  END AS semester,
  -- Formata a data para texto 'YYYY-MM'
  DATE_FORMAT(data_gerada, 'yyyy-MM') AS year_month,
  WEEKOFYEAR(data_gerada) AS week_num
FROM (
  SELECT 
    explode(
      sequence(DATE '2011-01-01', DATE '2014-12-31', INTERVAL 1 DAY)
    ) AS data_gerada
);