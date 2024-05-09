# Databricks notebook source
dbutils.widgets.text("Layer_Name","")
Layer_Name = dbutils.widgets.getArgument("Layer_Name")

# COMMAND ----------

Notebook_Path_Json = {
    "Database" : [
        "/Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Cleansing/Creating Database"
        ],
    "Cleansed" : [
        "/Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Cleansing/Airline - Transformation and Cleansing",
        "/Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Cleansing/Airport - Transformation and Cleansing",
        "/Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Cleansing/Cancellation - Transformation and Cleansing",
        "/Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Cleansing/PLANE - Transformation and Cleansing",
        "/Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Cleansing/Unique Carriers - Transformation and Cleansing",
        "/Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Cleansing/PLANE - Transformation and Cleansing",
    ],
    "Data_Quality_Cleansed" : [
        "/Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Data Quality Test/Data Quality Test Notebook"
    ],
    "Datamart" : [
        "/Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Data Mart/Dim_Airlines",
        "/Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Data Mart/Dim_Airport",
        "/Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Data Mart/Dim_Cancellation",
        "/Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Data Mart/Dim_Unique_Carriers",
        "/Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Data Mart/Dim_Plane",
        "/Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Data Mart/Reporting_Flight",
    ]
}

# COMMAND ----------

print(Notebook_Path_Json[Layer_Name])

# COMMAND ----------

for notebook_paths in Notebook_Path_Json[Layer_Name]:
    dbutils.notebook.run(notebook_paths,0)
