from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# Wrapper for Databricks dbutils function
def get_dbutils(spark):
    from pyspark.dbutils import DBUtils
    return DBUtils(spark)


# Databricks notebook source
storage_account_name = "formula1spark"
client_id            = get_dbutils.secrets.get(scope="formula1-scope", key="databricks-app-client-id")
tenant_id            = get_dbutils.secrets.get(scope="formula1-scope", key="databricks-app-tenant-id")
client_secret        = get_dbutils.secrets.get(scope="formula1-scope", key="databricks-app-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


def mount_adls(container_name):
  get_dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

mount_adls("raw")
mount_adls("processed")
get_dbutils.fs.mounts()
get_dbutils.fs.ls("/mnt/formula1spark/raw")
