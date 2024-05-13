# Databricks notebook source
# MAGIC %sql
# MAGIC use datamart_geekcoders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(tailid),
# MAGIC        count(DISTINCT tailid)
# MAGIC FROM cleansed_geekcoders.plane;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_plane (
# MAGIC   tailid STRING,
# MAGIC   type STRING,
# MAGIC   manufacturer STRING,
# MAGIC   issue_date DATE,
# MAGIC   model STRING,
# MAGIC   status STRING,
# MAGIC   aircraft_type STRING,
# MAGIC   engine_type STRING,
# MAGIC   year INT,
# MAGIC   Date_Part DATE
# MAGIC ) USING DELTA LOCATION "/mnt/datamart_datalake/dim_plane"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT
# MAGIC   OVERWRITE dim_plane
# MAGIC Select
# MAGIC   tailid,
# MAGIC   type,
# MAGIC   manufacturer,
# MAGIC   issue_date,
# MAGIC   model,
# MAGIC   status,
# MAGIC   aircraft_type,
# MAGIC   engine_type,
# MAGIC   year,
# MAGIC   Date_Part
# MAGIC from
# MAGIC   cleansed_geekcoders.plane;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM datamart_geekcoders.dim_plane;
