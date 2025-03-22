-- Databricks notebook source
SHOW DATABASES

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

USE f1_processed

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * FROM f1_processed.drivers 
LIMIT 10

-- COMMAND ----------

DESC drivers

-- COMMAND ----------

DESC EXTENDED drivers

-- COMMAND ----------

SELECT name, dob AS date_of_birth
FROM f1_processed.drivers 
WHERE nationality="British" 
AND dob > "1990-01-01"

-- COMMAND ----------

SELECT name, dob AS date_of_birth
FROM f1_processed.drivers 
WHERE nationality="British" 
AND dob > "1990-01-01"
ORDER BY date_of_birth DESC

-- COMMAND ----------

SELECT * from drivers
ORDER BY nationality
 ASC,
dob DESC;

-- COMMAND ----------

SELECT name,nationality, dob AS date_of_birth
FROM f1_processed.drivers 
WHERE (nationality="British" 
AND dob > "1990-01-01") OR (nationality="Indian")
ORDER BY date_of_birth DESC

-- COMMAND ----------

