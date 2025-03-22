# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using SAS tokens
# MAGIC 1. Set the spark config for SAS token. 
# MAGIC 2. List files from demo container. 
# MAGIC 3. Read data from circuits.csv file. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Skipped over this lesson since its not needed in  next part of the project
# MAGIC

# COMMAND ----------

formula1dl_sas_key=dbutils.secrets.get(scope="formula1-scope", key="formula1dl-sas-key")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dllwp.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dllwp.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dllwp.dfs.core.windows.net", formula1dl_sas_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dllwp.dfs.core.windows.net"))

# COMMAND ----------


display(spark.read.csv("abfss://demo@formula1dllwp.dfs.core.windows.net"))

# COMMAND ----------

