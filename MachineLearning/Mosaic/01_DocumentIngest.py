# Databricks notebook source
# MAGIC %md
# MAGIC # Document Ingestion 
# MAGIC
# MAGIC In this notebook we will ingest document data (metric definitions and data schemas), apply embedding models, and save the embeddings as a merge into a vector database. Per customer request we will be using the [all-MiniLM-L6-v2](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2) to map text to a 384 dimensional vector space. Please note that the size of the vector space is important when creating the vector index. 
# MAGIC
# MAGIC As a sample we will generate and save data for metrics and schemas to the given volume provided via notebook widgets/parameters. 
# MAGIC
# MAGIC Please review the Python SDK [documentation](https://api-docs.databricks.com/python/vector-search/databricks.vector_search.html). 

# COMMAND ----------

# MAGIC %pip install -U sentence-transformers databricks-vectorsearch

# COMMAND ----------

dbutils.library.restartPython() 

# COMMAND ----------

# load required libraries
from pyspark.sql.types import * 
from pyspark.sql.functions import * 
from sentence_transformers import SentenceTransformer
from databricks.vector_search.client import VectorSearchClient

# COMMAND ----------

# set and get notebook parameters 
dbutils.widgets.text('catalog_name', '')
dbutils.widgets.text('schema_name', '')
dbutils.widgets.text('volume_name', '')

catalog_name = dbutils.widgets.get('catalog_name')
schema_name = dbutils.widgets.get('schema_name')
volume_name = dbutils.widgets.get('volume_name')
print(f"{catalog_name} | {schema_name} | {volume_name}")

# COMMAND ----------

# create and use UC objects
spark.sql(f"create catalog if not exists {catalog_name}")
spark.sql(f"use catalog {catalog_name}")
spark.sql(f"create schema if not exists {schema_name}")
spark.sql(f"use schema {schema_name}")
spark.sql(f"create volume if not exists {volume_name}")

# COMMAND ----------

# volume path to save raw files
volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
# Define the schema for reading the text files later on
text_file_schema = "data STRING"

# COMMAND ----------

delete_ckpts = True
if delete_ckpts:
  dbutils.fs.rm(f"{volume_path}/sql_checkpoint", True)
  dbutils.fs.rm(f"{volume_path}/metrics_checkpoint", True)
  dbutils.fs.rm(f"{volume_path}/sql_checkpoint2", True)
  dbutils.fs.rm(f"{volume_path}/metrics_checkpoint2", True)
  spark.sql("drop table if exists metrics_table")
  spark.sql("drop table if exists sql_table")
  spark.sql("drop table if exists metrics_sync_table")
  spark.sql("drop table if exists sql_sync_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Demo Data

# COMMAND ----------

# Define table creation SQL statements
table1_sql = f"""
CREATE TABLE IF NOT EXISTS {schema_name}.table1 (
    id INT,
    name STRING,
    age INT
)
"""

table2_sql = f"""
CREATE TABLE IF NOT EXISTS {schema_name}.table2 (
    product_id INT,
    product_name STRING,
    price DECIMAL
)
"""

table3_sql = f"""
CREATE TABLE IF NOT EXISTS {schema_name}.table3 (
    order_id INT,
    customer_id INT,
    order_date DATE
)
"""

# save the files as text files in the volume
dbutils.fs.put(f"{volume_path}/table1.sql", table1_sql, overwrite=True)
dbutils.fs.put(f"{volume_path}/table2.sql", table2_sql, overwrite=True)
dbutils.fs.put(f"{volume_path}/table3.sql", table3_sql, overwrite=True)
dbutils.fs.ls(volume_path)

# COMMAND ----------

# Define metrics for each table
metrics_table1 = """
Metric: Average Age
Description: Calculates the average age of individuals in table1.
SQL: SELECT AVG(age) AS average_age FROM table1
"""

metrics_table2 = """
Metric: Total Revenue
Description: Calculates the total revenue from all products in table2.
SQL: SELECT SUM(price) AS total_revenue FROM table2
"""

metrics_table3 = """
Metric: Order Count
Description: Counts the total number of orders in table3.
SQL: SELECT COUNT(order_id) AS order_count FROM table3
"""

# Save metrics definitions to text files in the volume
dbutils.fs.put(f"{volume_path}/metrics_table1.txt", metrics_table1, overwrite=True)
dbutils.fs.put(f"{volume_path}/metrics_table2.txt", metrics_table2, overwrite=True)
dbutils.fs.put(f"{volume_path}/metrics_table3.txt", metrics_table3, overwrite=True)
dbutils.fs.ls(volume_path)

# COMMAND ----------

# lets just make sure we can read the data as expected 

display(spark.read.format('text').option("wholeText", "true").load(f"{volume_path}/*.txt"))

display(spark.read.format('text').option("wholeText", "true").load(f"{volume_path}/*.sql"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data 
# MAGIC 1. Load as streaming dataframes
# MAGIC 1. Create tables with primary key ID columns
# MAGIC 1. Append dataframes to tables

# COMMAND ----------

# Use Auto Loader to read the metrics files
metrics_df = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "text")
              .option("wholeText", "true")
              .schema(text_file_schema)
              .load(f"{volume_path}/*.txt")
)

# COMMAND ----------

# Use Auto Loader to read the sql files
sql_df = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "text")
              .option("wholeText", "true")
              .schema(text_file_schema)
              .load(f"{volume_path}/*.sql")
)

# COMMAND ----------

# Load the pre-trained model
embedding_model_name = 'sentence-transformers/all-MiniLM-L6-v2'
model = SentenceTransformer(embedding_model_name)
# Broadcast the model to worker nodes
broadcasted_model = sc.broadcast(model)

# Define a Pandas UDF to get embeddings
@pandas_udf(ArrayType(FloatType()))
def get_embeddings_udf(text_series):
    model = broadcasted_model.value  # Retrieve the model from the broadcast variable
    embeddings = text_series.apply(lambda x: model.encode([x])[0])
    return embeddings

# COMMAND ----------

# Apply the Pandas UDF to the DataFrames
metrics_df_with_embeddings = metrics_df.withColumn("embeddings", get_embeddings_udf(col("data")))
sql_df_with_embeddings = sql_df.withColumn("embeddings", get_embeddings_udf(col("data")))

# COMMAND ----------

spark.sql("""
          create table if not exists metrics_table (
            id bigint generated always as identity,
            data string,
            embeddings ARRAY<FLOAT>
          )
          using delta
          -- tblproperties (delta.enableChangeDataFeed = true);
          """)


spark.sql("""
          create table if not exists metrics_sync_table (
            id bigint generated always as identity,
            data string,
            embeddings ARRAY<FLOAT>
          )
          using delta
          tblproperties (delta.enableChangeDataFeed = true);
          """)

spark.sql("""
          create table if not exists sql_table (
            id bigint generated always as identity,
            data string,
            embeddings ARRAY<FLOAT>
          )
          using delta
          -- tblproperties (delta.enableChangeDataFeed = true);
          """)

spark.sql("""
          create table if not exists sql_sync_table (
            id bigint generated always as identity,
            data string,
            embeddings ARRAY<FLOAT>
          )
          using delta
          tblproperties (delta.enableChangeDataFeed = true);
          """)

# COMMAND ----------

# Incremental Write the embedding dataframes to Databricks vector search
(metrics_df_with_embeddings
 .writeStream
 .trigger(availableNow=True) 
 .option("checkpointLocation", f"{volume_path}/metrics_checkpoint")
 .format("delta")
 .toTable("metrics_table")
)

(sql_df_with_embeddings
 .writeStream
 .trigger(availableNow=True)
 .option("checkpointLocation", f"{volume_path}/sql_checkpoint") 
 .format("delta")
 .toTable("sql_table")
 )

 # Incremental Write the embedding dataframes to Databricks vector search
(metrics_df
 .writeStream
 .trigger(availableNow=True) 
 .option("checkpointLocation", f"{volume_path}/metrics_checkpoint2")
 .format("delta")
 .toTable("metrics_sync_table")
)

(sql_df
 .writeStream
 .trigger(availableNow=True)
 .option("checkpointLocation", f"{volume_path}/sql_checkpoint2") 
 .format("delta")
 .toTable("sql_sync_table")
 )

# COMMAND ----------

for stream in spark.streams.active:
    stream.awaitTermination()

# COMMAND ----------

display(spark.read.table('metrics_table'))
display(spark.read.table('sql_table'))
display(spark.read.table('metrics_sync_table'))
display(spark.read.table('sql_sync_table'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Vector Search Endpoint and Indexes
# MAGIC
# MAGIC There are two types of Vector Search Indexes on 
# MAGIC Delta Sync Index, which automatically syncs with a source Delta Table, automatically and incrementally updating the index as the underlying data in the Delta Table changes.
# MAGIC Direct Vector Access Index, which supports direct read and write of vectors and metadata. The user is responsible for updating this table using the REST API or the Python SDK. This type of index is created using REST API or the SDK.
# MAGIC
# MAGIC There are two ways to enable vector search in Databricks. 
# MAGIC 1. Pre-compute embeddings and manually insert/update to a vector search endpoint. 
# MAGIC     - This option may not scale as well to many use cases, as the embeddings will always require a job to compute embeddings and write to delta table and manually update the vector search endpoint. 
# MAGIC     - However, this is an excellent choice for organizations with a fewer number of embedding models being needed. 
# MAGIC     - This is the `create_direct_access_index` function in the python sdk. 
# MAGIC 1. Allow Vector Search to Automatically handle embedding updates from a srouce delta table. 
# MAGIC     - This is the `create_delta_sync_index` function in the python sdk. 

# COMMAND ----------

client = VectorSearchClient()

# COMMAND ----------

endpoint_name = 'rac_vs_endpoint'
all_endpoints = [e.get('name') for e in client.list_endpoints().get('endpoints')]

if endpoint_name not in all_endpoints:
    client.create_endpoint_and_wait(
        name=endpoint_name,
        endpoint_type="STANDARD",
        verbose=True
    )
else : 
    print("Endpoint already exists.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Direct Index

# COMMAND ----------

if delete_ckpts:
  index_names = ['metrics_index', 'metrics_sync_index', 'sql_index', 'sql_sync_index']
  for i in index_names:
    try:
      client.delete_index(endpoint_name, f'{catalog_name}.{schema_name}.{i}')
      print(f"Deleted index {i}")
    except :
      print(f"Index {i} does not exist.")

# COMMAND ----------

index_dict = client.list_indexes(endpoint_name)
if index_dict.get('vector_indexes') is not None:
  all_indexes = [i.get('name') for i in index_dict.get('vector_indexes')]
else :
  all_indexes = []

# COMMAND ----------

if f'{catalog_name}.{schema_name}.metrics_index' not in all_indexes:
  print("Creating Index.")
  metrics_index = client.create_direct_access_index(
    endpoint_name=endpoint_name,
    index_name=f"{catalog_name}.{schema_name}.metrics_index",
    primary_key="id",
    embedding_dimension=384,# notice! This depends on the model. Please see cell #1 from this notebook
    embedding_vector_column="embeddings",
    schema={
      "id": "int",
      "data": "string",
      "embeddings": "array<float>"}
  )
else :
  print("Index already exists.")
  metrics_index = client.get_index(endpoint_name, f'{catalog_name}.{schema_name}.metrics_index')



if f'{catalog_name}.{schema_name}.sql_index' not in all_indexes:
  print("Creating Index.")
  sql_index = client.create_direct_access_index(
    endpoint_name=endpoint_name,
    index_name=f"{catalog_name}.{schema_name}.sql_index",
    primary_key="id",
    embedding_dimension=384,# notice! This depends on the model. Please see cell #1 from this notebook
    embedding_vector_column="embeddings",
    schema={
      "id": "int",
      "data": "string",
      "embeddings": "array<float>"}
  )
else :
  print("Index already exists.")
  sql_index = client.get_index(endpoint_name, f'{catalog_name}.{schema_name}.sql_index')

# COMMAND ----------

## Manually update the index
# Read the metrics_table into a Spark DataFrame
metrics_df = spark.table("metrics_table")

# Convert the Spark DataFrame to a list of dictionaries
metrics_list = [row.asDict() for row in metrics_df.collect()]
metrics_index.upsert(metrics_list)

# COMMAND ----------

## Manually update the index
# Read the sql_table into a Spark DataFrame
sql_df = spark.table("sql_table")

# Convert the Spark DataFrame to a list of dictionaries
sql_list = [row.asDict() for row in sql_df.collect()]
sql_index.upsert(sql_list)

# COMMAND ----------

# direct vectors query through embeddings instead of raw text
# test_embedding = sql_df.select('embeddings').collect()[0][0]

text_value = "What is the total revenue for each product?"
test_embedding = model.encode(text_value).tolist()

# # Delta Sync Index with embeddings computed by Databricks
results = metrics_index.similarity_search(
    query_vector=test_embedding,
    columns=["id", "data"],
    num_results=2
    )

display(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Vector Index Sync
# MAGIC
# MAGIC A Delta Sync Index requires an embedding model to be deployed as a model serving endpoint in Databricks to manage the auto-sync between table and index. 

# COMMAND ----------

model_serving_endpoint_name = "rac_embedding_model_endpoint"

# COMMAND ----------

# deploy model serving endpoint - this code doesn't exist
# then deploy syncs - this code looks good but needs testing
# then query the index - this code looks good but needs testing

# COMMAND ----------

index_dict = client.list_indexes(endpoint_name)
if index_dict.get('vector_indexes') is not None:
  all_indexes = [i.get('name') for i in index_dict.get('vector_indexes')]
else :
  all_indexes = []


# create metrics index
if f'{catalog_name}.{schema_name}.metrics_sync_index' not in all_indexes:
  print("Creating Index.")
  metrics_sync_index = client.create_delta_sync_index_and_wait(
    endpoint_name=endpoint_name,
    index_name=f"{catalog_name}.{schema_name}.metrics_sync_index",
    primary_key="id",
    # pipeline_type="TRIGGERED",
    pipeline_type="CONTINUOUS",
    embedding_dimension=384, 
    # embedding_vector_column="embeddings",
    source_table_name=f'{catalog_name}.{schema_name}.metrics_sync_table',
    embedding_source_column='data',
    embedding_model_endpoint_name=model_serving_endpoint_name,
    # sync_computed_embeddings=True
  )
else :
  print("Index already exists.")
  metrics_sync_index = client.get_index(endpoint_name, f'{catalog_name}.{schema_name}.metrics_sync_index')


# create a sql index table
if f'{catalog_name}.{schema_name}.sql_sync_index' not in all_indexes:
  print("Creating Index.")
  sql_sync_index = client.create_delta_sync_index_and_wait(
    endpoint_name=endpoint_name,
    index_name=f"{catalog_name}.{schema_name}.sql_sync_index",
    primary_key="id",
    # pipeline_type="TRIGGERED",
    pipeline_type="CONTINUOUS",
    embedding_dimension=384, 
    # embedding_vector_column="embeddings",
    source_table_name=f'{catalog_name}.{schema_name}.sql_sync_table',
    embedding_source_column='data',
    embedding_model_endpoint_name=model_serving_endpoint_name,
    # sync_computed_embeddings=True
  )
else :
  print("Index already exists.")
  sql_sync_index = client.get_index(endpoint_name, f'{catalog_name}.{schema_name}.sql_sync_index')

# COMMAND ----------

# MAGIC %md
# MAGIC Payload query. It seems that the index automatically sends some query to the endpoint and that request has a specific format. 
# MAGIC
# MAGIC There were 25 rows last query. 
# MAGIC
# MAGIC ```
# MAGIC SELECT * 
# MAGIC
# MAGIC FROM rac_embedding_model_payload
# MAGIC
# MAGIC order by timestamp_ms desc
# MAGIC ```

# COMMAND ----------

# direct vectors query through embeddings instead of raw text
# test_embedding = sql_df.select('embeddings').collect()[0][0]

text_value = "What is the total revenue for each product?"

# # Delta Sync Index with embeddings computed by Databricks
results = metrics_sync_index.similarity_search(
    query_text=text_value,
    columns=["id", "data"],
    num_results=2
    )

display(results)

# COMMAND ----------


