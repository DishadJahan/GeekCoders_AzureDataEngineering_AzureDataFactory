# Databricks notebook source
# MAGIC %sql
# MAGIC use datamart_geekcoders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM cleansed_geekcoders.airlines;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_airlines (
# MAGIC   iata_code STRING,
# MAGIC   icao_code STRING,
# MAGIC   airlines_name STRING,
# MAGIC   Date_Part DATE
# MAGIC ) USING DELTA 
# MAGIC
# MAGIC LOCATION "/mnt/datamart_datalake/dim_airlines"

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE dim_airlines
# MAGIC Select
# MAGIC   iata_code,
# MAGIC   icao_code,
# MAGIC   airlines_name,
# MAGIC   Date_Part
# MAGIC from cleansed_geekcoders.airlines;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM datamart_geekcoders.dim_airlines;
