-- Databricks notebook source

use datamart_geekcoders;

-- COMMAND ----------

-- SELECT *
-- FROM cleansed_geekcoders.airlines;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dim_airlines (
  iata_code STRING,
  icao_code STRING,
  airlines_name STRING,
  Date_Part DATE
) USING DELTA 

LOCATION "/mnt/datamart_datalake/dim_airlines"

-- COMMAND ----------

INSERT OVERWRITE dim_airlines
Select
  iata_code,
  icao_code,
  airlines_name,
  Date_Part
from cleansed_geekcoders.airlines;

-- COMMAND ----------

SELECT * FROM datamart_geekcoders.dim_airlines;
