# Databricks notebook source
dbutils.widgets.text("DatabaseName", "rac_demo_db")
database_name = dbutils.widgets.get("DatabaseName")

# COMMAND ----------

spark.sql("""
CREATE DATABASE IF NOT EXISTS {}
""".format(database_name))

# COMMAND ----------

spark.sql("USE {}".format(database_name))

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/nyctaxi")

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/nyctaxi/tables")

# COMMAND ----------

df = (spark
      .read
      .format("delta")
      .load("/databricks-datasets/nyctaxi/tables/nyctaxi_yellow")
)
display(df)

# COMMAND ----------

(
df.write
  .format("delta")
  .mode("overwrite")
  .saveAsTable("new_york_taxi_data_bronze")
)

# COMMAND ----------

spark.sql("""
OPTIMIZE new_york_taxi_data_bronze
ZORDER BY (pickup_datetime, dropoff_datetime)
""")

# COMMAND ----------


