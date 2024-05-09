# Databricks notebook source
# DBTITLE 1,Creating one DataFrame for all four "PLANE" csv datasets:
PLANE_path = "/mnt/raw_datalake/PLANE/"
df_base = spark.read.format("csv").options(header = True).load(PLANE_path)

df_base.display()

# COMMAND ----------

# DBTITLE 1,Transformation - Changing the data types of certain columns
df_base = df_base.selectExpr(
    "tailnum as tailid", 
    "type", 
    "manufacturer", 
    "to_date(issue_date) as issue_date", 
    "model",
    "status", 
    "aircraft_type", 
    "engine_type", 
    "cast(year as int) as year", 
    "to_date(Date_Part) as Date_Part"
)

# COMMAND ----------

# DBTITLE 1,Writing/Saving the transformed PLANE dataset to the cleansed container (ADLS)
df_base.write.format("delta").mode("overwrite").option("header", "True").save("/mnt/source_cleansed/plane")

# COMMAND ----------

# DBTITLE 1,Running the utilities notebook
# MAGIC %run /Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Cleansing/Utilities

# COMMAND ----------

# DBTITLE 1,Loading the "Plane" data to create a table:
df_plane = spark.read.format("delta").load("/mnt/source_cleansed/plane")
schema = pre_schema(df_plane)

# f_delta_cleansed_load(table_name, location, schema, database)

f_delta_cleansed_load('plane', "/mnt/source_cleansed/plane", schema, 'cleansed_geekcoders')
