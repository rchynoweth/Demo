# Databricks notebook source
import pyspark.sql.functions import udf

# COMMAND ----------

# DBTITLE 1,Import libs
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests 
import json

# COMMAND ----------

# DBTITLE 1,Define a function to call the API
def call_rest_api(string_column_value):
  assert type(string_column_value) == str
  # public REST API from PostMan: https://documenter.getpostman.com/view/8854915/Szf7znEe
  response = requests.get("https://cat-fact.herokuapp.com/facts/")
  return response.text

# COMMAND ----------

# DBTITLE 1,Create UDF from the function 
udf_call_rest_api = udf(call_rest_api)

# COMMAND ----------

# DBTITLE 1,Create/Read a DataFrame
columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF(columns)
display(df)

# COMMAND ----------

# DBTITLE 1,Call the API from UDF
df = df.withColumn("new_col", udf_call_rest_api(col("language")))
display(df)

# COMMAND ----------

# DBTITLE 1,Select and drop cols
df = (df.withColumn("new_col", udf_call_rest_api(col("language")))
       .drop('language', 'users_count')
     )
display(df)

# COMMAND ----------

# DBTITLE 1,Define JSON schema to work with the json data
js_sc = ArrayType(StructType([
    StructField("__v", StringType()),
    StructField("_id", StringType()),
    StructField("createdAt", StringType()),
    StructField("deleted", StringType()),
    StructField("source", StringType()),
    StructField("status", StructType([
      StructField("feedback", StringType()),
      StructField("sentCount", StringType()),
      StructField("verified", StringType())
    ]) ),
    StructField("text", StringType()),
    StructField("type", StringType()),
    StructField("updatedAt", StringType()),
    StructField("used", StringType()),
    StructField("user", StringType())
  ])
                 )

# COMMAND ----------

display(df.withColumn('exploded_col' , explode(from_json('new_col', js_sc)))
        .select(['exploded_col', 'exploded_col.createdAt', 'exploded_col.__v', 'exploded_col._id', 'exploded_col.deleted'])

                )

# COMMAND ----------


