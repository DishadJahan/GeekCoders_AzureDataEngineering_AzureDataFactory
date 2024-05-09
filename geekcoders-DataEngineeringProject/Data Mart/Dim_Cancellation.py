# Databricks notebook source
# MAGIC %sql
# MAGIC use datamart_geekcoders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM cleansed_geekcoders.cancellation;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_cancellation (
# MAGIC   code STRING,
# MAGIC   description STRING,
# MAGIC   Date_Part DATE
# MAGIC ) USING DELTA 
# MAGIC
# MAGIC LOCATION "/mnt/datamart_datalake/dim_cancellation"

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE dim_cancellation
# MAGIC Select
# MAGIC   code,
# MAGIC   description,
# MAGIC   Date_Part
# MAGIC from cleansed_geekcoders.cancellation;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM datamart_geekcoders.dim_cancellation;
