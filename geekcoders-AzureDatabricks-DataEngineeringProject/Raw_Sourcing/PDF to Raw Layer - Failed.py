# Databricks notebook source
pip install tabula-py

# COMMAND ----------

pip install jpype1

# COMMAND ----------

import tabula
from datetime import date

tabula.convert_into('/dbfs/mnt/source_loan', f'/dbfs/mnt/raw_datalake/LOAN/Date_Part={date.today()}/LOAN.csv',output_format='csv',pages='all')

# COMMAND ----------

import tabula
from datetime import date

tabula.convert_into('/dbfs/mnt/source_adls/', '/dbfs/mnt/raw_datalake/PLANE.csv',output_format='csv',pages='all')

# COMMAND ----------

import tabula
from datetime import date

today = str(date.today())
date_part = str(f'Date_Part={today}')
input_directory = '/dbfs/mnt/source-blob/PLANE.pdf'
base_directory = f'dbfs/mnt/raw_datalake/PLANE/{date_part}'
output_directory = f'dbfs/mnt/raw_datalake/PLANE/{date_part}/PLANE.csv'

dbutils.fs.mkdirs(base_directory)

tabula.convert_into(input_directory, output_directory, output_format='csv', pages='all')

# COMMAND ----------

import tabula
from datetime import date

print(date.today())
today = str(date.today())
input_directory = '/dbfs/mnt/source-blob/PLANE.pdf'
output_directory = f'dbfs/mnt/raw_datalake/PLANE/Date_Part={today}'

# dbutils.fs.mkdirs(base_directory)

tabula.convert_into(input_directory, f'output_directory'+"PLANE.csv", output_format='csv', pages='all')

# COMMAND ----------

from datetime import date
print(date.today())

# PLANE/Date_Part={date.today()}/

# COMMAND ----------

import tabula
from datetime import date
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
today = str(date.today())
output_directory = f'/mnt/raw_datalake/PLANE/Date_Part={today}'

dbutils.fs.mkdirs(output_directory)

pdf_path = '/mnt/source_adls/PLANE.pdf'
df = spark.read.format("pdf").option("multiline", "true").load(pdf_path)

df.write.mode("overwrite").option("header", "true").csv(output_directory)

csv_file = output_directory + "/PLANE.csv"
dbutils.fs.mv(csv_file, output_directory)

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import date

# Initialize SparkSession
spark = SparkSession.builder.appName("PDFtoCSV").getOrCreate()

# Get today's date
today = str(date.today())

# Define input and output paths
pdf_path = '/mnt/source-blob/'
output_directory = f'dbfs:/mnt/raw_datalake/PLANE/Date_Part={today}'

# Read PDF into DataFrame
df = spark.read.format('pdf').option("multiline", "true").load(pdf_path)

# Write DataFrame to CSV
csv_output_path = output_directory + "/PLANE.csv"
df.write.mode("overwrite").option("header", "true").csv(csv_output_path)

# Stop SparkSession
spark.stop()

# Move CSV file to a different location
dbutils.fs.mv(csv_output_path, output_directory)

# COMMAND ----------

dbutils.fs.ls("mnt/source-blob/")

# COMMAND ----------

from pyspark.sql import SparkSession
import tabula
from datetime import date

# Initialize SparkSession
spark = SparkSession.builder.appName("PDFtoCSV").getOrCreate()

# Get today's date
today = str(date.today())

# Define input and output paths
pdf_path = '/mnt/source-blob/PLANE.pdf'
output_directory = f'dbfs:/mnt/raw_datalake/PLANE/Date_Part={today}'

# Read PDF using tabula-py
tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True)

# Convert extracted tables to Spark DataFrame
dfs = []
for i, table in enumerate(tables):
    df = spark.createDataFrame(table)
    dfs.append(df)

# Concatenate DataFrames if there are multiple tables extracted
final_df = dfs[0] if len(dfs) == 1 else dfs[0].union(*dfs[1:])

# Write DataFrame to CSV
csv_output_path = output_directory + "/PLANE.csv"
final_df.write.mode("overwrite").csv(csv_output_path, header = True)

# Stop SparkSession
spark.stop()

# Move CSV file to a different location
dbutils.fs.mv(csv_output_path, output_directory + "/PLANE.csv")

# COMMAND ----------

data = tabula.read_pdf("/mnt/source_adls/PLANE.pdf", pages="all")
print(data)

# COMMAND ----------

dbutils.fs.ls("/mnt/source_loan/")

# COMMAND ----------

import tabula
from datetime import date
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
today = str(date.today())
output_directory = f'/mnt/raw_datalake/LOAN/Date_Part={today}'

dbutils.fs.mkdirs(output_directory)

pdf_path = '/mnt/source_loanpdf/loan_data.pdf'
df = spark.read.format("pdf").option("multiline", "true").load(pdf_path)

df.write.mode("overwrite").option("header", "true").csv(output_directory)

csv_file = output_directory + "/LOAN.csv"
dbutils.fs.mv(csv_file, output_directory)

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("PDFtoCSV").getOrCreate()

# Define PDF path
pdf_path = '/mnt/source_loanpdf/loan_data.pdf'

# Read PDF into DataFrame
df = spark.read.format("com.databricks:spark.pdf") \
    .option("multiline", "true") \
    .option("inferSchema", "true") \
    .load(pdf_path)

# Define output directory
output_directory = '/mnt/raw_datalake/LOAN'

# Write DataFrame to CSV
df.write.mode("overwrite").option("header", "true").csv(output_directory)

# Stop SparkSession
spark.stop()

# COMMAND ----------

# MAGIC %pip list
# MAGIC

# COMMAND ----------

pip install spark-pdf

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("PDFtoDF").getOrCreate()

# Read PDF into DataFrame
df = spark.read.format("pdf") \
    .option("multiline", "true") \
    .load("/mnt/source_loanpdf/loan_data.pdf")

# Show DataFrame schema and some sample data
df.printSchema()
df.show()

# COMMAND ----------

import tabula
from datetime import date

def f_source_pdf_datalake(source_path, sink_path, output_format, page, file_name):
    try:
        dbutils.fs.mkdirs(f'/{sink_path}{file_name.split('.')[0]}/Date_Part={date.today()}/')
        tabula.convert_into(f'{source_path}{file_name}', f'/dbfs/{sink_path}{file_name.split('.')[0]}/Date_Part={date.today()}/{file_name.split('.')[0]}.{output_format}',output_format = output_format, pages = page)
    except Exception as err:
        print("Error Occured", str(err))

# COMMAND ----------

list_files = [(i.name, i.name.split('.')[1]) for i in dbutils.fs.ls("/mnt/source_loan/") if (i.name.split('.')[1] == "pdf")]

for i in list_files:
    f_source_pdf_datalake('/dbfs/mnt/source_adls/', '/dbfs/mnt/raw_datalake/', 'csv', 'all', i[0])
