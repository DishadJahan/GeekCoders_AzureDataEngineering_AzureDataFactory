# Databricks notebook source
# MAGIC %sql
# MAGIC use datamart_geekcoders;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select
# MAGIC   date,
# MAGIC   ArrDelay,
# MAGIC   DepDelay,
# MAGIC   Origin,
# MAGIC   Destination,
# MAGIC   Cancelled,
# MAGIC   CancellationCode,
# MAGIC   UniqueCarrier,
# MAGIC   FlightNum,
# MAGIC   TailNum,
# MAGIC   DepTime,
# MAGIC   ArrTime
# MAGIC from cleansed_geekcoders.flight;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Reporting_flight;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS Reporting_flight (
# MAGIC   date DATE,
# MAGIC   ArrDelay INT,
# MAGIC   DepDelay INT,
# MAGIC   Origin STRING,
# MAGIC   Destination STRING,
# MAGIC   Cancelled INT,
# MAGIC   CancellationCode STRING,
# MAGIC   UniqueCarrier STRING,
# MAGIC   FlightNum INT,
# MAGIC   TailNum STRING,
# MAGIC   DepTime STRING,
# MAGIC   ArrTime STRING
# MAGIC ) USING DELTA PARTITIONED BY (date_year INT) LOCATION "/mnt/datamart_datalake/Reporting_flight"

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC from pyspark.sql.functions import ValuesView
# MAGIC
# MAGIC max_year = spark.sql(
# MAGIC     """SELECT max(year(date)) FROM cleansed_geekcoders.flight"""
# MAGIC ).collect()[0][0]
# MAGIC spark.sql(
# MAGIC     f"""
# MAGIC           INSERT OVERWRITE Reporting_flight PARTITION (date_year = {max_year})
# MAGIC           SELECT date,
# MAGIC                  ArrDelay,
# MAGIC                  DepDelay,
# MAGIC                  Origin,
# MAGIC                  Destination,
# MAGIC                  Cancelled,
# MAGIC                  CancellationCode,
# MAGIC                  UniqueCarrier,
# MAGIC                  FlightNum,
# MAGIC                  TailNum,
# MAGIC                  DepTime,
# MAGIC                  ArrTime
# MAGIC           FROM cleansed_geekcoders.flight
# MAGIC           WHERE year(date) = {max_year}
# MAGIC         """
# MAGIC )

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC all_years = spark.sql("""SELECT DISTINCT year(date) AS year FROM cleansed_geekcoders.flight""").collect()
# MAGIC
# MAGIC for i in all_years:
# MAGIC     year = i['year']
# MAGIC     spark.sql(f"""
# MAGIC         INSERT OVERWRITE Reporting_flight PARTITION (date_year = {year})
# MAGIC         SELECT date,
# MAGIC                ArrDelay,
# MAGIC                DepDelay,
# MAGIC                Origin,
# MAGIC                Destination,
# MAGIC                Cancelled,
# MAGIC                CancellationCode,
# MAGIC                UniqueCarrier,
# MAGIC                FlightNum,
# MAGIC                TailNum,
# MAGIC                DepTime,
# MAGIC                ArrTime
# MAGIC         FROM cleansed_geekcoders.flight
# MAGIC         WHERE year(date) = {year}
# MAGIC     """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DISTINCT year(date) AS year
# MAGIC FROM
# MAGIC   datamart_geekcoders.Reporting_flight
# MAGIC ORDER BY
# MAGIC   year;
