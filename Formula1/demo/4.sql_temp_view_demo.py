# Databricks notebook source
# MAGIC %md
# MAGIC ##### Access dataframes using SQL 
# MAGIC
# MAGIC Objectives
# MAGIC 1. Create temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM v_race_results 
# MAGIC WHERE race_year=2020

# COMMAND ----------

race_results_2019_df=spark.sql("SELECT * FROM v_race_results WHERE race_year=2019")

# COMMAND ----------

display(race_results_2019_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC #####Global Temporary views
# MAGIC
# MAGIC 1. Create global temporary views on dataframes
# MAGIC 2. Access the view fro SQL
# MAGIC 3. Access the view from Python
# MAGIC 4. Access the view from another Notebook

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM global_temp.gv_race_results 
# MAGIC WHERE race_year=2020

# COMMAND ----------

race_results_2019_df=spark.sql("SELECT * FROM global_temp.gv_race_results WHERE race_year=2019")

# COMMAND ----------

