-- Databricks notebook source

use datamart_geekcoders;

-- COMMAND ----------

-- SELECT *
-- FROM cleansed_geekcoders.unique_carriers;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dim_unique_carriers (
  code STRING,
  description STRING,
  Date_Part DATE
) USING DELTA 

LOCATION "/mnt/datamart_datalake/dim_unique_carriers"

-- COMMAND ----------

INSERT OVERWRITE dim_unique_carriers
Select
  code,
  description,
  Date_Part
from cleansed_geekcoders.unique_carriers;

-- COMMAND ----------

SELECT * FROM datamart_geekcoders.dim_unique_carriers;
