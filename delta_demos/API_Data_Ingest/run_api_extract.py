# Databricks notebook source
from libs.api_extract import APIExtract
from pyspark.sql.functions import col

# COMMAND ----------

api_extract_client = APIExtract()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Single Threaded Option

# COMMAND ----------

data = api_extract_client.call_simple_rest_api()
data

# COMMAND ----------

df = spark.createDataFrame(data)
display(df)

# COMMAND ----------

df.write.saveAsTable('cat_data')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parallel API Option

# COMMAND ----------

# Create a list of dictionaries with the URL values
request_params = [
    {"url": "https://cat-fact.herokuapp.com/facts/"},
    {"url": "https://dog.ceo/api/breeds/list/all/"},
    {"url": "https://world.openpetfoodfacts.org/api/v0/product/20106836.json"},
    {"url": "https://world.openfoodfacts.org/api/v0/product/737628064502.json"},
    {"url": "https://openlibrary.org/api/books?bibkeys=ISBN:0201558025,LCCN:93005405&format=json"},
]

# Create DataFrame from the list of dictionaries
request_df = spark.createDataFrame(request_params)
request_df.show()

# COMMAND ----------

response_df = request_df.withColumn('response', api_extract_client.api_udf(col('url')))
response_df.show()

# COMMAND ----------


