# Databricks notebook source
# MAGIC %md
# MAGIC <h3>Access Azure Data Lake using Cluster Scoped Credentials </h3>
# MAGIC 1. Set spark config with fs.azure.account.key
# MAGIC 2. List files from demo container 
# MAGIC 3. Read data from circuits.csv file
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dllwp.dfs.core.windows.net"))

# COMMAND ----------


display(spark.read.csv("abfss://demo@formula1dllwp.dfs.core.windows.net"))

# COMMAND ----------

