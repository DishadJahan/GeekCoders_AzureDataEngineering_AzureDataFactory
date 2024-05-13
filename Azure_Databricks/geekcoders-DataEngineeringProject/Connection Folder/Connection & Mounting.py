# Databricks notebook source
# DBTITLE 1,Mount & Connection from Databricks to "Raw" container (ADLS)
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}


dbutils.fs.mount(
  source = "abfss://raw@adlsgeekcodersdevop.dfs.core.windows.net/",
  mount_point = "/mnt/raw_datalake/",
  extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,Mount & Connection from Databricks to "Cleansed" container (ADLS)
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}


dbutils.fs.mount(
  source = "abfss://cleansed@adlsgeekcodersdevop.dfs.core.windows.net/",
  mount_point = "/mnt/source_cleansed/",
  extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,Mount & Connection from Databricks to "Datamart" container (ADLS)
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}


dbutils.fs.mount(
  source = "abfss://datamart@adlsgeekcodersdevop.dfs.core.windows.net/",
  mount_point = "/mnt/datamart_datalake/",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/raw_datalake/")

# COMMAND ----------

dbutils.fs.ls("/mnt/source_cleansed/")

# COMMAND ----------

dbutils.fs.ls("/mnt/datamart_datalake/")

# COMMAND ----------

# geekcoders-secret

# %scala
# val containerName = dbutils.secrets.get(scope="geekcoders-secret",key="containerName")
# val storageAccountName = dbutils.secrets.get(scope="geekcoders-secret",key="storageaccountname")
# val sas = dbutils.secrets.get(scope="geekcoders-secret",key="SAS")
# val config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows"

# dbutils.fs.mount(
#   source = dbutils.secrets.get(scope="geekcoders-secret",key="blob-mnt-path"),
#   mountPoint = "/mnt/source_blob/",
#   extraConfigs = Map(config -> sas))

# COMMAND ----------

# configs = {"fs.azure.account.auth.type": "OAuth",
#           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="geekcoders-secret", key="App-Client-ID"),
#           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="geekcoders-secret", key="App-Client-Secret"),
#           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/a4608493-78ff-488e-bc96-e0569291bf11/oauth2/token"}


# dbutils.fs.mount(
#   source = "abfss://raw@adlsgeekcodersdevop.dfs.core.windows.net/",
#   mount_point = "/mnt/raw_datalake/",
#   extra_configs = configs)
