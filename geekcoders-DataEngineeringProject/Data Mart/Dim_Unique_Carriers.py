# Databricks notebook source
# MAGIC %sql
# MAGIC use datamart_geekcoders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM cleansed_geekcoders.unique_carriers;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_unique_carriers (
# MAGIC   code STRING,
# MAGIC   description STRING,
# MAGIC   Date_Part DATE
# MAGIC ) USING DELTA 
# MAGIC
# MAGIC LOCATION "/mnt/datamart_datalake/dim_unique_carriers"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE dim_unique_carriers
# MAGIC Select
# MAGIC   code,
# MAGIC   description,
# MAGIC   Date_Part
# MAGIC from cleansed_geekcoders.unique_carriers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM datamart_geekcoders.dim_unique_carriers;
