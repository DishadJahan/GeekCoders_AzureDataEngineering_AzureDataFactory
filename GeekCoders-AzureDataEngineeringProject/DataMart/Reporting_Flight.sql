-- Databricks notebook source
-- DBTITLE 1,Selecting the database:

use datamart_geekcoders;

-- COMMAND ----------

-- DBTITLE 1,Checking the data before creating the structure and inserting the values:
Select
  date,
  ArrDelay,
  DepDelay,
  Origin,
  Destination,
  Cancelled,
  CancellationCode,
  UniqueCarrier,
  FlightNum,
  TailNum,
  DepTime,
  ArrTime
from cleansed_geekcoders.flight;

-- COMMAND ----------

-- DBTITLE 1,Dropping the Table if it exists:
DROP TABLE IF EXISTS Reporting_flight;

-- COMMAND ----------

-- DBTITLE 1,Creating table Structure:
CREATE TABLE IF NOT EXISTS Reporting_flight (
  date DATE,
  ArrDelay INT,
  DepDelay INT,
  Origin STRING,
  Destination STRING,
  Cancelled INT,
  CancellationCode STRING,
  UniqueCarrier STRING,
  FlightNum INT,
  TailNum STRING,
  DepTime STRING,
  ArrTime STRING

) 
USING DELTA 

PARTITIONED BY (date_year INT)

LOCATION "/mnt/datamart_datalake/Reporting_flight"

-- COMMAND ----------

-- %py

-- from pyspark.sql.functions import ValuesView

-- max_year = spark.sql("""SELECT max(year(date)) FROM cleansed_geekcoders.flight""").collect()[0][0]
-- spark.sql(f"""
--         INSERT OVERWRITE Reporting_flight PARTITION (date_year = {max_year})
--         SELECT date,
--                ArrDelay,
--                DepDelay,
--                Origin,
--                Destination,
--                Cancelled,
--                CancellationCode,
--                UniqueCarrier,
--                FlightNum,
--                TailNum,
--                DepTime,
--                ArrTime
--         FROM cleansed_geekcoders.flight
--         WHERE year(date) = {max_year}
--     """)

-- COMMAND ----------

-- DBTITLE 1,Inserting the data into the created Table:
-- MAGIC %py
-- MAGIC
-- MAGIC all_years = spark.sql("""SELECT DISTINCT year(date) AS year FROM cleansed_geekcoders.flight""").collect()
-- MAGIC
-- MAGIC for i in all_years:
-- MAGIC     year = i['year']
-- MAGIC     spark.sql(f"""
-- MAGIC         INSERT OVERWRITE Reporting_flight PARTITION (date_year = {year})
-- MAGIC         SELECT date,
-- MAGIC                ArrDelay,
-- MAGIC                DepDelay,
-- MAGIC                Origin,
-- MAGIC                Destination,
-- MAGIC                Cancelled,
-- MAGIC                CancellationCode,
-- MAGIC                UniqueCarrier,
-- MAGIC                FlightNum,
-- MAGIC                TailNum,
-- MAGIC                DepTime,
-- MAGIC                ArrTime
-- MAGIC         FROM cleansed_geekcoders.flight
-- MAGIC         WHERE year(date) = {year}
-- MAGIC     """)

-- COMMAND ----------

SELECT DISTINCT year(date) AS year FROM datamart_geekcoders.Reporting_flight ORDER BY year;
