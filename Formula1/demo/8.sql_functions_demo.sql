-- Databricks notebook source
USE f1_processed

-- COMMAND ----------

SELECT * , CONCAT(driver_ref,'-',code) AS new_driver_ref
FROM drivers

-- COMMAND ----------

SELECT * , SPLIT(name,' ')[0] forename, SPLIT(name,' ')[1] lastname
FROM drivers

-- COMMAND ----------

SELECT *, current_timestamp 
from drivers

-- COMMAND ----------

SELECT *, date_format(dob, "dd-MM-yyyy") 
from drivers

-- COMMAND ----------

SELECT COUNT(*) FROM drivers

-- COMMAND ----------

SELECT MAX(dob) FROM drivers;

-- COMMAND ----------

SELECT * from drivers where dob = "2000-05-11"

-- COMMAND ----------

SELECT COUNT(*) from drivers where nationality="British"

-- COMMAND ----------

SELECT nationality, COUNT(*) 
FROM drivers  
GROUP BY nationality 
HAVING count(*) >1 
ORDER BY nationality

-- COMMAND ----------

SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob) AS age_rank FROM drivers ORDER BY nationality, age_rank

-- COMMAND ----------

