-- Databricks notebook source

use datamart_geekcoders;

-- COMMAND ----------

SELECT *
FROM cleansed_geekcoders.cancellation;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dim_cancellation (
  code STRING,
  description STRING,
  Date_Part DATE
) USING DELTA 

LOCATION "/mnt/datamart_datalake/dim_cancellation"

-- COMMAND ----------

INSERT OVERWRITE dim_cancellation
Select
  code,
  description,
  Date_Part
from cleansed_geekcoders.cancellation;

-- COMMAND ----------

SELECT * FROM datamart_geekcoders.dim_cancellation;
