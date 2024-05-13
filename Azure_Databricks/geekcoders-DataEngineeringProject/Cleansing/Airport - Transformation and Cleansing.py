# Databricks notebook source
# DBTITLE 1,Creating the DataFrame for "Airport" dataset
Airport_path = "/mnt/raw_datalake/Airport/"
df_base = spark.read.format("csv").options(header = True).load(Airport_path)

df_base.display()

# COMMAND ----------

# DBTITLE 1,Transformation - Changing the data types of certain columns
df_base = df_base.selectExpr(
    "Code as code", 
    "split(Description,',')[0] as city", 
    "split(split(Description,',')[1],':')[0] as state",
    "split(split(Description,',')[1],':')[1] as airport",  
    "to_date(Date_Part) as Date_Part"
)
df_base.display() 

# COMMAND ----------

# DBTITLE 1,Writing/Saving the transformed "Airport" dataset to the cleansed container (ADLS)
df_base.write.format("delta").mode("overwrite").option("header", "True").save("/mnt/source_cleansed/airport")

# COMMAND ----------

# DBTITLE 1,Running the utilities notebook
# MAGIC %run /Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Cleansing/Utilities

# COMMAND ----------

# DBTITLE 1,Loading the "Airport" data to create a table:
df_airport = spark.read.format("delta").load("/mnt/source_cleansed/airport")
schema = pre_schema(df_airport)

# f_delta_cleansed_load(table_name, location, schema, database)
f_delta_cleansed_load('airport', "/mnt/source_cleansed/airport", schema, 'cleansed_geekcoders')
