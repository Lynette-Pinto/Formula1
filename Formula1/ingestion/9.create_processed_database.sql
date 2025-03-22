-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "dbfs:/mnt/formula1dllwp/processed"

-- COMMAND ----------



-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/mnt/formula1dllwp"))
-- MAGIC

-- COMMAND ----------

