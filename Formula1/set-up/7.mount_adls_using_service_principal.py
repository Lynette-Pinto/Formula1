# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake using Service Principal
# MAGIC 1. Get client_id, client_secret and Tenent_id from key vault
# MAGIC 2. Set Spark Config with App/Client Id, Direct
# MAGIC 3. Call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to the mount (list all mounts unmount)

# COMMAND ----------

storage_account_name="formula1dllwp"
client_id=dbutils.secrets.get(scope="formula1-scope",key="formula1-app-clientid")
tenant_id=dbutils.secrets.get(scope="formula1-scope",key="formula1-tenantId")
client_secret=dbutils.secrets.get(scope="formula1-scope",key="formula1-app-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret":client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dllwp.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl/demo/"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

#dbutils.fs.unmount('/mnt/formula1dl/demo')