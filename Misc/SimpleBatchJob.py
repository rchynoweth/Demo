# Databricks notebook source
import time 

# COMMAND ----------

df = spark.read.format('csv').option('header', 'true').option('delimiter', ';').load('/databricks-datasets/retail-org/products/*.csv')
display(df)


# COMMAND ----------

df.createOrReplaceTempView('test')

# COMMAND ----------

display(spark.sql('select * from test'))

# COMMAND ----------

time.sleep(500)

# COMMAND ----------


