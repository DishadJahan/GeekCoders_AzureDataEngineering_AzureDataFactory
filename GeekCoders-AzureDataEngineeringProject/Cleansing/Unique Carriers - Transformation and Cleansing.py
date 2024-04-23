# Databricks notebook source
# DBTITLE 1,Creating the dataframe for "Unique Carriers" dataset
unique_carrier_path = "/mnt/raw_datalake/UNIQUE_CARRIERS/"
df_base = spark.read.format("parquet").options(header = True).load(unique_carrier_path)

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

# DBTITLE 1,Writing/Saving the transformed "Unique Carriers" dataset to the cleansed container (ADLS)
df_base.write.format("delta").mode("overwrite").option("header", "True").save("/mnt/source_cleansed/unique_carriers")

# COMMAND ----------

# DBTITLE 1,Running the utilities notebook
# MAGIC %run /Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Cleansing/Utilities

# COMMAND ----------

# DBTITLE 1,Loading the "Unique Carrier" data to create a table:
df_unique_carriers = spark.read.format("delta").load("/mnt/source_cleansed/unique_carriers")
schema = pre_schema(df_unique_carriers)

# f_delta_cleansed_load(table_name, location, schema, database)

f_delta_cleansed_load('unique_carriers', "/mnt/source_cleansed/unique_carriers", schema, 'cleansed_geekcoders')

# COMMAND ----------

# DBTITLE 1,Checking to see the created Table:
# MAGIC %sql
# MAGIC
# MAGIC select * 
# MAGIC from cleansed_geekcoders.unique_carriers;

# COMMAND ----------

# from pyspark.sql import SparkSession
# import os

# def check_directory_exists(path):
#     return os.path.exists(path)

# spark = SparkSession.builder.appName("DataWriter").getOrCreate()

# output_path = "/mnt/source_cleansed/plane"
# if not check_directory_exists(output_path):
#     df_plane.write.format("delta")\
#         .mode("overwrite")\
#         .option("header", "True")\
#         .save(output_path)
# else:
#     print("Output directory already exists. Skipping write operation.")

# COMMAND ----------

# df = spark.readStream.format("cloudFiles").option("cloudFiles.format", 'csv')\
#     .option("cloudFiles.schemaLocation", "/dbfs/FileStore/tables/schema/PLANE")\
#         .load('/mnt/raw_datalake/PLANE/')

# COMMAND ----------

# from pyspark.sql.functions import col
# from pyspark.sql.types import DateType

# df = df.withColumn("issue_date", col("issue_date").cast(DateType()))

# df.printSchema()

# COMMAND ----------

# list_files = [i.name for i in dbutils.fs.ls("/mnt/raw_datalake/")]

# print(list_files)
