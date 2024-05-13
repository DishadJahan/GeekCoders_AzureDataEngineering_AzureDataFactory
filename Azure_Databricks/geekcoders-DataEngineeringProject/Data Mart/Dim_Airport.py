# Databricks notebook source
# MAGIC %sql
# MAGIC use datamart_geekcoders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM cleansed_geekcoders.airport;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_airport (
# MAGIC   code STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   airport_name STRING,
# MAGIC   Date_Part DATE
# MAGIC ) USING DELTA 
# MAGIC
# MAGIC LOCATION "/mnt/datamart_datalake/dim_airport"

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE dim_airport
# MAGIC Select
# MAGIC   code,
# MAGIC   city,
# MAGIC   state,
# MAGIC   airport,
# MAGIC   Date_Part
# MAGIC from cleansed_geekcoders.airport;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM datamart_geekcoders.dim_airport;
