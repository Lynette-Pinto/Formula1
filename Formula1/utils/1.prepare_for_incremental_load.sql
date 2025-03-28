-- Databricks notebook source
-- MAGIC %md
-- MAGIC Drop all tables

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE


-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed  
LOCATION "/mnt/formula1dllwp/processed"

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE


-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation  
LOCATION "/mnt/formula1dllwp/presentation"

-- COMMAND ----------

-- MAGIC %fs mounts

-- COMMAND ----------

DESC DATABASE EXTENDED f1_processed

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

