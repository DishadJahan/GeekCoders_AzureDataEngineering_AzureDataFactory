# Databricks notebook source
# MAGIC %run /Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Cleansing/Utilities

# COMMAND ----------

list_table_info = [
    ("WRITE", "plane", 100),
    ("WRITE", "flight", 200),
    ("WRITE", "airport", 100),
    ("WRITE", "cancellation", 100),
    ("WRITE", "unique_carriers", 500),
    ("WRITE", "airlines", 10),
]

for i in list_table_info:
    f_count_check("cleansed_geekcoders", i[0], i[1], i[2])
