# Databricks notebook source
# DBTITLE 1,Creating the DataFrame for "Airlines" dataset
Airline_path = "/mnt/raw_datalake/airlines/"
df_base = spark.read.format("json").load(Airline_path)

df_base.display()

# COMMAND ----------

# DBTITLE 1,Transformation - Changing the data types of certain columns
df_base = df_base.selectExpr(
    "iata_code", 
    "icao_code", 
    "name as airlines_name",  
    "Date_Part"
)
df_base.display() 

# COMMAND ----------

# DBTITLE 1,Writing/Saving the transformed "Airlines" dataset to the cleansed container (ADLS)
df_base.write.format("delta").mode("overwrite").option("header", "True").save("/mnt/source_cleansed/airlines")

# COMMAND ----------

# MAGIC %run /Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Cleansing/Utilities

# COMMAND ----------

# DBTITLE 1,Loading the "Airlines" data to create a table:
df_airlines = spark.read.format("delta").load("/mnt/source_cleansed/airlines")
schema = pre_schema(df_airlines)

# f_delta_cleansed_load(table_name, location, schema, database)

f_delta_cleansed_load('airlines', "/mnt/source_cleansed/airlines", schema, 'cleansed_geekcoders')
