# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df= spark.read.parquet(f"{processed_folder_path}/races").filter("race_year=2019")\
    .withColumnRenamed("name","race_name")

# COMMAND ----------

circuits_df= spark.read.parquet(f"{processed_folder_path}/circuits")\
    .filter("circuit_id<70")\
    .withColumnRenamed("name","circuit_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Inner Join
# MAGIC

# COMMAND ----------

race_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"inner")\
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Outer Join
# MAGIC

# COMMAND ----------

race_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"left")\
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

race_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"right")\
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

race_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"full")\
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Semi Joins
# MAGIC

# COMMAND ----------

race_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"semi")\
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Anti Joins
# MAGIC
# MAGIC Negation of Semi Joins

# COMMAND ----------

race_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"anti")\
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Cross Join

# COMMAND ----------

race_circuits_df=races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuits_df.count())

# COMMAND ----------

