-- Databricks notebook source

use datamart_geekcoders;

-- COMMAND ----------

SELECT count(tailid),
       count(DISTINCT tailid)
FROM cleansed_geekcoders.plane;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dim_plane (
  tailid STRING,
  type STRING,
  manufacturer STRING,
  issue_date DATE,
  model STRING,
  status STRING,
  aircraft_type STRING,
  engine_type STRING,
  year INT,
  Date_Part DATE
) USING DELTA LOCATION "/mnt/datamart_datalake/dim_plane"

-- COMMAND ----------

INSERT
  OVERWRITE dim_plane
Select
  tailid,
  type,
  manufacturer,
  issue_date,
  model,
  status,
  aircraft_type,
  engine_type,
  year,
  Date_Part
from
  cleansed_geekcoders.plane;

-- COMMAND ----------

SELECT * FROM datamart_geekcoders.dim_plane;
