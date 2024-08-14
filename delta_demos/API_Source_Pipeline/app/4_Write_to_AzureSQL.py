# Databricks notebook source
# MAGIC %md
# MAGIC # Azure SQL Sink  
# MAGIC
# MAGIC In this example we use the [Spark provided JDBC](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html) class to read and write data to Azure SQL. This class can be used to write to a number of different databases. 
# MAGIC
# MAGIC Engineers may also use the [Apache Spark connector: SQL Server & Azure SQL](https://docs.microsoft.com/en-us/sql/connect/spark/connector)

# COMMAND ----------

import time

# COMMAND ----------

dbutils.widgets.text("schema_name", "") ### Note - this can be a widget or an environment variable  
schema_name = dbutils.widgets.get("schema_name")
dbutils.widgets.text("catalog_name", "") ### Note - this can be a widget or an environment variable  
catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# spark.sql("CREATE CATALOG IF NOT EXISTS {}".format(catalog_name))
spark.sql(f"use catalog {catalog_name}")
spark.sql("CREATE SCHEMA IF NOT EXISTS {}".format(schema_name))

# COMMAND ----------

spark.sql("USE {}".format(schema_name))

# COMMAND ----------

jdbcUsername = dbutils.secrets.get(scope = "rac_scope", key = "azuresqluser")
jdbcPassword = dbutils.secrets.get(scope = "rac_scope", key = "azuresqlpassword")
jdbcHostname = dbutils.secrets.get(scope = "rac_scope", key = "azuresqlserver")
jdbcDatabase = dbutils.secrets.get(scope = "rac_scope", key = "azuresqldatabase")
jdbcUrl = "jdbc:sqlserver://{}.database.windows.net:1433;database={};user={};password={};".format(jdbcHostname, jdbcDatabase, jdbcUsername, jdbcPassword)

# COMMAND ----------

df = spark.read.table("batch_gold_table")
display(df)

# COMMAND ----------

# DBTITLE 1,Write to Azure SQL
(df.write
 .format("jdbc")
 .mode("overwrite")
 .option("url", jdbcUrl)
 .option("dbtable", "batch_gold_table")
 .save())

# COMMAND ----------

# DBTITLE 1,Read from Azure SQL - show that it works
sql_df = (spark.read
 .format("jdbc")
 .option("url", jdbcUrl)
 .option("dbtable", "batch_gold_table")
 .load())

display(sql_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream Data from Delta to Azure SQL

# COMMAND ----------

df = spark.readStream.format('delta').table("batch_gold_table")

# COMMAND ----------

## 
## For each batch function to use in the streaming 
## 
def stream_to_azsql(microBatchDF, batchId):
  (microBatchDF.write
   .format("jdbc")
   .mode("overwrite")
   .option("url", jdbcUrl)
   .option("dbtable", "silver_batch_gold_table")
   .save()
  )


# COMMAND ----------

user_name = "ryan.chynoweth@databricks.com"
azsql_ckpt = "/Users/{}/api_weather_demo/ckpt/azsql_ckpt".format(user_name)
dbutils.fs.rm(azsql_ckpt, True)

(df.writeStream
    .format("delta")
    .option("checkpointLocation", azsql_ckpt)
    .trigger(once=True) # set to False if you want this continuous 
    .foreachBatch(stream_to_azsql)
    .start()
)


# COMMAND ----------


