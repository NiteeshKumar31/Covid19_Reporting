# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount the following data lake storage gen2 containers
# MAGIC 1. raw
# MAGIC 2. processed
# MAGIC 3. lookup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set-up the configs
# MAGIC #### Please update the following 
# MAGIC - application-id
# MAGIC - service-credential
# MAGIC - directory-id
# MAGIC - secrets config

# COMMAND ----------

storage_acct_key = dbutils.secrets.get(scope = 'covid19-reporting-scope', key= 'covidreporting31dl-acct-key')
client_id = dbutils.secrets.get(scope = 'covid19-reporting-scope', key= 'covidreporting31dl-app-id')
tenant_id = dbutils.secrets.get(scope = 'covid19-reporting-scope', key= 'covidreporting31dl-tenent-id')
client_secret = dbutils.secrets.get(scope = 'covid19-reporting-scope', key= 'covidreporting31dl-client-secret')


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the raw container
# MAGIC #### Update the storage account name before executing

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@covidreporting31dl.dfs.core.windows.net/",
  mount_point = "/mnt/covidreporting31dl/raw",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the processed container
# MAGIC #### Update the storage account name before executing

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://processed@covidreporting31dl.dfs.core.windows.net/",
  mount_point = "/mnt/covidreporting31dl/processed",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the lookup container
# MAGIC #### Update the storage account name before executing

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://lookup@covidreporting31dl.dfs.core.windows.net/",
  mount_point = "/mnt/covidreporting31dl/lookup",
  extra_configs = configs)

# COMMAND ----------

# dbutils.fs.ls("/mnt/covidreporting31dl/raw")
# dbutils.fs.ls("/mnt/covidreporting31dl/processed")
dbutils.fs.ls("/mnt/covidreporting31dl/lookup")



# COMMAND ----------

