# Databricks notebook source
# DBTITLE 1,Creating the DataFrame for "Cancellation " dataset
Cancellation_path = "/mnt/raw_datalake/Cancellation/"
df_base = spark.read.format("parquet").options(header = True).load(Cancellation_path)

df_base.display()

# COMMAND ----------

# DBTITLE 1,Transformation - Changing the data types of certain columns
df_base = df_base.selectExpr(
    "replace(Code,'\"','') as code",
    "replace(Description,'\"','') as description",  
    "to_date(Date_Part) as Date_Part"
)
df_base.display()

# COMMAND ----------

# DBTITLE 1,Writing/Saving the transformed "Cancellation" dataset to the cleansed container (ADLS)
df_base.write.format("delta").mode("overwrite").option("header", "True").save("/mnt/source_cleansed/cancellation")

# COMMAND ----------

# MAGIC %run /Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Cleansing/Utilities

# COMMAND ----------

# DBTITLE 1,Loading the "Cancellation" data to create a table:
df_cancellation = spark.read.format("delta").load("/mnt/source_cleansed/cancellation")
schema = pre_schema(df_cancellation)

# f_delta_cleansed_load(table_name, location, schema, database)

f_delta_cleansed_load('cancellation', "/mnt/source_cleansed/cancellation", schema, 'cleansed_geekcoders')
