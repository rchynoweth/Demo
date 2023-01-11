// Databricks notebook source
// MAGIC %md
// MAGIC Widgets

// COMMAND ----------

dbutils.widgets.text("my_widget", "this is a default value field")

// COMMAND ----------

val my_value = dbutils.widgets.get("my_widget")

// COMMAND ----------

println(my_value)

// COMMAND ----------

// MAGIC %md
// MAGIC Environment variables
// MAGIC 
// MAGIC Notice that there is a single environment variable set on my cluster. 
// MAGIC 
// MAGIC <img src="https://racadlsgen2.blob.core.windows.net/public/ENV_VARS.png" />

// COMMAND ----------

val env_var = sys.env("my_test_variable")

// COMMAND ----------

println(env_var)

// COMMAND ----------

// DBTITLE 1,Using Python
// MAGIC %python
// MAGIC import os
// MAGIC 
// MAGIC print(os.environ.get("my_test_variable"))
// MAGIC 
// MAGIC 
// MAGIC source_db = dbutils.widgets.get("my_widget")
// MAGIC 
// MAGIC source_db = os.environ.get("source_db") # always "prod"
// MAGIC sink_db = os.environ.get("sink_db") # either "sandbox" or "prod"

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.read.table(f"{source_db}.table")
// MAGIC 
// MAGIC df.write.saveAsTable(f"{sink_db}.table")
