# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

def fake_rest_api_call(string_val):
  return "[REDACTED]"

# COMMAND ----------

udf_executeRestApi = udf(fake_rest_api_call)

# COMMAND ----------

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF(columns)
display(df)

# COMMAND ----------

display(df.withColumn("new_col", udf_executeRestApi(col("language"))))

# COMMAND ----------

display(df.withColumn("new_col", udf_executeRestApi(col("language"))).drop('language', 'users_count'))

# COMMAND ----------


