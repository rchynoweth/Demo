# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC AutoLoader Example Notebook - Azure
# MAGIC 
# MAGIC Links:
# MAGIC - [Azure Docs](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/auto-loader)
# MAGIC 
# MAGIC 
# MAGIC High Level:
# MAGIC - Different files are all stored in the same directory so we will need to load them with wild card characters
# MAGIC - Using Foreach batch to write the files out 
# MAGIC - Two types of loading data from Azure i.e. notifications and file listing
# MAGIC   - Demonstrates the differences 
# MAGIC - Writes into delta tables
# MAGIC 
# MAGIC 
# MAGIC Key Features:
# MAGIC - Allow source files to be overwritten i.e. `cloudFiles.allowOverwrites`

# COMMAND ----------

dbutils.fs.ls("/Users/ryan.chynoweth@databricks.com")

# COMMAND ----------

databricks secrets put --scope admin --key storage_account_key

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, deltaTablePath)


(deltaTable.alias("target")
  .merge(microBatchDF.alias("source"), "")
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute()
)

# COMMAND ----------

"target.datetime = source.datetime and target.duration = source.duration and target.mrbts = source.mrbts and target.lnbts = source.mrbts and target.lncel = source.lncel"
