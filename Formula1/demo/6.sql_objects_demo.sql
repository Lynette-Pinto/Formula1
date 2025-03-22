-- Databricks notebook source
-- MAGIC %md
-- MAGIC #####Learning Objectives 
-- MAGIC 1. Create managed table using Python
-- MAGIC 2. Create managed table using SQL
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe table 
-- MAGIC

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df= spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

SELECT * FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.race_results_sql AS 
SELECT * FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLEs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Learning Objectives
-- MAGIC 1. Create external table using Python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping an external table.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points INT,
  position INT,
  created_date TIMESTAMP
)
USING PARQUET
LOCATION "abfss://presentation@formula1lwpdl.dfs.core.windows.net/race_results_ext_sql  "

-- COMMAND ----------

SHOW tables in demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py where race_year=2020;

-- COMMAND ----------

select count(1) from demo.race_results_ext_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Learnig Objectives 
-- MAGIC 1. Create Table View
-- MAGIC 2. Create Global Temp View
-- MAGIC 3. Create Permanent View
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS 
SELECT * FROM demo.race_results_python 
WHERE race_year = 2020; 

-- COMMAND ----------

SELECT * from v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS 
SELECT * FROM demo.race_results_python 
WHERE race_year = 2012; 

-- COMMAND ----------

SELECT * from global_temp.gv_race_results

-- COMMAND ----------

SHOW TABLES in global_temp;


-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS 
SELECT * FROM demo.race_results_python 
WHERE race_year = 2000; 

-- COMMAND ----------

SHOW TABLES in demo

-- COMMAND ----------

