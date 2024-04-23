-- Databricks notebook source

use datamart_geekcoders;

-- COMMAND ----------

SELECT *
FROM cleansed_geekcoders.airport;

-- COMMAND ----------

SELECT count(code),
       count(DISTINCT code)
FROM cleansed_geekcoders.airport;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dim_airport (
  code STRING,
  city STRING,
  state STRING,
  airport_name STRING,
  Date_Part DATE
) USING DELTA 

LOCATION "/mnt/datamart_datalake/dim_airport"

-- COMMAND ----------

INSERT OVERWRITE dim_airport
Select
  code,
  city,
  state,
  airport,
  Date_Part
from cleansed_geekcoders.airport;

-- COMMAND ----------

SELECT * FROM datamart_geekcoders.dim_airport;
