// Databricks notebook source
// MAGIC %md
// MAGIC # Reading Copy Book Files 
// MAGIC 
// MAGIC [Python Copybook](https://github.com/zalmane/copybook)

// COMMAND ----------

dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container", "")

// COMMAND ----------

val storage_account = dbutils.widgets.get("storage_account")
val container = dbutils.widgets.get("container")
val data_directory = "abfss://"+container+"@"+storage_account+".dfs.core.windows.net/"
println(data_directory)

// COMMAND ----------

dbutils.fs.ls(data_directory)

// COMMAND ----------

val cobolDataframe = spark
  .read
  .format("cobol")
  .option("copybook", "abfss://albert@racadlsgen2.dfs.core.windows.net/copyBookfile.pvt")
  .load(data_directory)

// COMMAND ----------

val cobolDataframe = spark
  .read
  .format("cobol")
  .option("copybook", "dbfs:/FileStore/shared_uploads/ryan.chynoweth@databricks.com/copyBookfile.pvt")
  .load("dbfs:/FileStore/shared_uploads/ryan.chynoweth@databricks.com/")

// COMMAND ----------

display(cobolDataframe)

// COMMAND ----------


