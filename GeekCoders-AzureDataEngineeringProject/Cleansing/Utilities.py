# Databricks notebook source
def pre_schema(df_base):
    try:
        schema = ""
        for i in df_base.dtypes:
            schema = schema + i[0] + " " + i[1] + ","
        return schema[0:-1]
    except Exception as err:
        print("Error Occured", str(err))

# COMMAND ----------

# MAGIC %py
# MAGIC def f_delta_cleansed_load(table_name, location, schema, database):
# MAGIC     try:
# MAGIC         spark.sql(f"""DROP TABLE IF EXISTS {database}.{table_name}""");                    
# MAGIC         spark.sql(f"""
# MAGIC                 CREATE TABLE IF NOT EXISTS {database}.{table_name}
# MAGIC                 ({schema})
# MAGIC                 using delta
# MAGIC                 location '{location}'          
# MAGIC                 """)
# MAGIC     except Exception as err:
# MAGIC         print("Error Occured", str(err))

# COMMAND ----------

# MAGIC %py
# MAGIC def f_count_check(database, operation_type, table_name, number_diff):
# MAGIC     spark.sql(f"""DESC HISTORY {database}.{table_name}""").createOrReplaceTempView("Table_Count")
# MAGIC     current_count = spark.sql("""
# MAGIC                             Select operationMetrics.numOutputRows
# MAGIC                             from Table_Count
# MAGIC                             where version = (select max(version) 
# MAGIC                                             from Table_Count 
# MAGIC                                             where trim(lower(operation)) = lower('{operation_type}'))
# MAGIC                             """)
# MAGIC     if (current_count.first() is None):
# MAGIC         final_current_count = 0
# MAGIC     else:
# MAGIC         final_current_count = int(current_count.first().numOutputRows)
# MAGIC
# MAGIC         ##############################################################################
# MAGIC
# MAGIC     previous_count = spark.sql("""
# MAGIC                             Select operationMetrics.numOutputRows
# MAGIC                             from Table_Count
# MAGIC                             where version < (select version 
# MAGIC                                             from Table_Count 
# MAGIC                                             where trim(lower(operation)) = lower('{operation_type}')
# MAGIC                                             order by version desc
# MAGIC                                             limit 1)
# MAGIC                             """)
# MAGIC     if (previous_count.first() is None):
# MAGIC         final_previous_count = 0
# MAGIC     else:
# MAGIC         final_previous_count = int(previous_count.first().numOutputRows)
# MAGIC
# MAGIC         ##############################################################################
# MAGIC
# MAGIC     if ((final_current_count - final_previous_count) > 100):
# MAGIC         # print("Difference is Huge in ", table_name)
# MAGIC         raise Exception ("Difference is Huge in ", table_name)
# MAGIC     else:
# MAGIC         pass
# MAGIC
# MAGIC # except Exception as err:
# MAGIC #     print("Error Occured", str(err))
