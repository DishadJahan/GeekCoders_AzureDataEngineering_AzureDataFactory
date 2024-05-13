# Databricks notebook source
insert_query = "select count(*) from datamart_geekcoders.dim_unique_carriers group by code having count(*) > 1"
insert_test_cases("datamart_geekcoders", 1, "Check if duplicate code is there in dim_unique_carriers or not", insert_query, 0)

# COMMAND ----------

insert_query = "select count(*) from datamart_geekcoders.dim_airport group by code having count(*) > 1"
insert_test_cases("datamart_geekcoders", 1, "Check if duplicate code is there in dim_airport or not", insert_query, 0)

# COMMAND ----------

execute_test_case("datamart_geekcoders")
