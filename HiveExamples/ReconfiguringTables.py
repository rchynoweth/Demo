# Databricks notebook source
# MAGIC %md
# MAGIC # Reconfiguring your Delta Tables
# MAGIC 
# MAGIC Scenarios: 
# MAGIC 1. Turning an unmanaged table into a managed table 
# MAGIC 1. Turning a managed table into a unmanaged table 
# MAGIC 1. I stored all my tables in the dbfs root () and need to move them to my own storage account. 
# MAGIC 1. I have created a table using a mounted location but would like to move it from mount to direct access 

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.widgets.text("DatabaseName", "rac_demo_db")
# MAGIC database_name = dbutils.widgets.get("DatabaseName")
# MAGIC dbutils.widgets.text("UserName", "ryan.chynoweth@databricks.com")
# MAGIC user_name = dbutils.widgets.get("UserName")
# MAGIC table_path = "/users/{}/demo_delta_tables/".format(user_name)

# COMMAND ----------

dbutils.fs.rm(table_path, True)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))
spark.sql("USE {}".format(database_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Types of Tables 

# COMMAND ----------

## NOTE: 
#### This is the source DF for all the tables we create 
## 

from pyspark.sql.functions import col

df = (spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('/databricks-datasets/timeseries/Fires/Fire_Department_Calls_for_Service.csv'))
for c in df.columns:
  df = df.withColumnRenamed(c, c.replace(' ', '').replace('-', ''))

# COMMAND ----------

# DBTITLE 1,"Type 1" - see bullet list above


# COMMAND ----------

# DBTITLE 1,"Type 2" - see bullet list above


# COMMAND ----------

# DBTITLE 1,"Type 3" - see bullet list above


# COMMAND ----------

# MAGIC %md
# MAGIC ## Unmanaged to Managed 

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE managed_fires_table
AS 
SELECT * 
FROM delta.`/users/ryan.chynoweth@databricks.com/delta_tables/managed_fires_table`
""")

# COMMAND ----------

display(spark.sql("SELECT * FROM managed_fires_table"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_fires_table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta.`/users/ryan.chynoweth@databricks.com/delta_tables/managed_fires_table`

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE managed_fires_table

# COMMAND ----------

dbutils.fs.ls("/users/ryan.chynoweth@databricks.com/delta_tables/managed_fires_table")

# COMMAND ----------

try :
  display(spark.sql("select * from managed_fires_table"))
except:
  print("ERROR")

# COMMAND ----------


