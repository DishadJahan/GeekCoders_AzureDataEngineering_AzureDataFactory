# Databricks notebook source
# DBTITLE 1,Creating one DataFrame for all four "Flight" csv datasets:
from functools import reduce
from pyspark.sql import DataFrame, SparkSession

# Set the Spark configuration options individually
spark = SparkSession.builder \
    .appName("FlightDataProcessing") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
    .getOrCreate()

# List of paths for all CSV files
paths = ["/mnt/raw_datalake/flight/"]

# Function to union all DataFrames
def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

# Read all CSV files into separate DataFrames
dfs = [spark.read.format("csv").options(header=True).load(path) for path in paths]

# Merge all DataFrames into one
df_base = unionAll(*dfs)

# Show the schema of the merged DataFrame
# df_base.printSchema()

# Show the first few rows of the merged DataFrame
df_base.display()

# COMMAND ----------

# DBTITLE 1,Transformation - Changing the data types of certain columns
df_base = df_base.selectExpr(
    "to_date(concat_ws('-', Year, Month, DayofMonth), 'yyyy-MM-dd') as date", 
    "from_unixtime(unix_timestamp(case when DepTime = 2400 then 0 else DepTime End, 'HHmm'), 'HH:mm') as DepTime",
    "from_unixtime(unix_timestamp(case when CRSDepTime = 2400 then 0 else CRSDepTime End, 'HHmm'), 'HH:mm') as CRSDepTime",
    "from_unixtime(unix_timestamp(case when ArrTime = 2400 then 0 else ArrTime End, 'HHmm'), 'HH:mm') as ArrTime",
    "from_unixtime(unix_timestamp(case when CRSArrTime = 2400 then 0 else CRSArrTime End, 'HHmm'), 'HH:mm') as CRSArrTime",
    "UniqueCarrier",
    "cast(FlightNum as int) as FlightNum",
    "TailNum",
    "cast(ActualElapsedTime as int) as ActualElapsedTime",
    "cast(CRSElapsedTime as int) as CRSElapsedTime",
    "cast(AirTime as int) as AirTime",
    "cast(ArrDelay as int) as ArrDelay",
    "cast(DepDelay as int) as DepDelay",
    "Origin",
    "Dest as Destination",
    "cast(Distance as int) as Distance",
    "cast(TaxiIn as int) as Taxi_In",
    "cast(TaxiOut as int) as Taxi_Out",
    "Cancelled",
    "CancellationCode",
    "cast(Diverted as int) as Diverted",
    "cast(CarrierDelay as int) as CarrierDelay",
    "cast(WeatherDelay as int) as WeatherDelay",
    "cast(NASDelay as int) as NASDelay",
    "cast(SecurityDelay as int) as SecurityDelay",
    "cast(LateAircraftDelay as int) as LateAircraftDelay",
    "to_date(Date_Part) as Date_Part"
)

# COMMAND ----------

# DBTITLE 1,Writing/Saving the transformed "Flight" dataset to the cleansed container (ADLS)
df_base.write.format("delta").mode("overwrite").option("header", "True").save("/mnt/source_cleansed/flight")

# COMMAND ----------

# DBTITLE 1,Running the utilities notebook
# MAGIC %run /Workspace/Users/dishadjahan.2802@outlook.com/geekcoders-DataEngineeringProject/Cleansing/Utilities

# COMMAND ----------

# DBTITLE 1,Loading the "Flight" data to create a table:
df_flight = spark.read.format("delta").load("/mnt/source_cleansed/flight")
schema = pre_schema(df_flight)

# f_delta_cleansed_load(table_name, location, schema, database)

f_delta_cleansed_load('flight', "/mnt/source_cleansed/flight", schema, 'cleansed_geekcoders')
