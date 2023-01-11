# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming ML Predictions
# MAGIC 
# MAGIC ### Inference on Streaming Data
# MAGIC 
# MAGIC Spark Streaming enables...<br><br>
# MAGIC 
# MAGIC * Scalable and fault-tolerant operations that continuously perform inference on incoming data
# MAGIC * Streaming applications can also incorporate ETL and other Spark features to trigger actions in real time
# MAGIC 
# MAGIC This lesson is meant as an introduction to streaming applications as they pertain to production machine learning jobs.  
# MAGIC 
# MAGIC Streaming poses a number of specific obstacles. These obstacles include:<br><br>
# MAGIC 
# MAGIC * *End-to-end reliability and correctness:* Applications must be resilient to failures of any element of the pipeline caused by network issues, traffic spikes, and/or hardware malfunctions.
# MAGIC * *Handle complex transformations:* Applications receive many data formats that often involve complex business logic.
# MAGIC * *Late and out-of-order data:* Network issues can result in data that arrives late and out of its intended order.
# MAGIC * *Integrate with other systems:* Applications must integrate with the rest of a data infrastructure.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Streaming data sources in Spark...<br><br>
# MAGIC 
# MAGIC * Offer the same DataFrames API for interacting with your data
# MAGIC * The crucial difference is that in structured streaming, the DataFrame is unbounded
# MAGIC * In other words, data arrives in an input stream and new records are appended to the input DataFrame
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-3/structured-streamining-model.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 
# MAGIC Spark is a good solution for...<br><br>
# MAGIC 
# MAGIC * Batch inference
# MAGIC * Incoming streams of data
# MAGIC 
# MAGIC For low-latency inference, however, Spark may or may not be the best solution depending on the latency demands of your task

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Connecting to the Stream
# MAGIC 
# MAGIC As data technology matures, the industry has been converging on a set of technologies.  Apache Kafka and cloud-specific managed alternatives like AWS Kinesis and Azure Event Hubs have become the ingestion engine at the heart of many pipelines.  
# MAGIC 
# MAGIC This technology brokers messages between producers, such as an IoT device writing data, and consumers, such as a Spark cluster reading data to perform real time analytics. There can be a many-to-many relationship between producers and consumers and the broker itself is scalable and fault tolerant.
# MAGIC 
# MAGIC We'll simulate a stream using the `maxFilesPerTrigger` option.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/>  There are a number of ways to stream data.  One other common design pattern is to stream from an an object store where any new files that appear will be read by the stream.

# COMMAND ----------

dbutils.widgets.text("DatabaseName", "rac_demo_db")
dbutils.widgets.text("UserName", "ryan.chynoweth@databricks.com")

# COMMAND ----------

database_name = dbutils.widgets.get('DatabaseName')
user_name = dbutils.widgets.get('UserName')
table_location = '/users/{}/databases/{}'.format(user_name, database_name) 
spark.sql("CREATE DATABASE IF NOT EXISTS {} LOCATION '{}'".format(dbutils.widgets.get('DatabaseName'), table_location))
spark.sql("USE {}".format(dbutils.widgets.get("DatabaseName")))

# COMMAND ----------

# MAGIC %md
# MAGIC Import the dataset in Spark.

# COMMAND ----------

from databricks import feature_store

# COMMAND ----------

fs = feature_store.FeatureStoreClient()
table_name = '{}.rac_clean_airbnb_ml_data'.format(database_name) 

# Displays most recent table
airbnbDF = fs.read_table(
  name=table_name
)
airbnbDF = airbnbDF.drop("index")
display(airbnbDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a schema for the data stream.  Data streams need a schema defined in advance.

# COMMAND ----------

from pyspark.sql.types import DoubleType, IntegerType, StructType


schema = (StructType()
.add("host_total_listings_count", DoubleType())
.add("neighbourhood_cleansed", IntegerType())
.add("zipcode", IntegerType())
.add("latitude", DoubleType())
.add("longitude", DoubleType())
.add("property_type", IntegerType())
.add("room_type", IntegerType())
.add("accommodates", DoubleType())
.add("bathrooms", DoubleType())
.add("bedrooms", DoubleType())
.add("beds", DoubleType())
.add("bed_type", IntegerType())
.add("minimum_nights", DoubleType())
.add("number_of_reviews", DoubleType())
.add("review_scores_rating", DoubleType())
.add("review_scores_accuracy", DoubleType())
.add("review_scores_cleanliness", DoubleType())
.add("review_scores_checkin", DoubleType())
.add("review_scores_communication", DoubleType())
.add("review_scores_location", DoubleType())
.add("review_scores_value", DoubleType())
.add("price", DoubleType())
)

# COMMAND ----------

# MAGIC %md
# MAGIC Check to make sure the schemas match.

# COMMAND ----------

schema == airbnbDF.schema

# COMMAND ----------

# MAGIC %md
# MAGIC Check the number of shuffle partitions.

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC Change this to 8.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC Create a data stream using `readStream` and `maxFilesPerTrigger`.

# COMMAND ----------

streamingData = (spark
                 .readStream
                 .schema(schema)
                 .option("maxFilesPerTrigger", 1)
                 .parquet("/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.parquet/")
                 .drop("price"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply an `sklearn` Model on the Stream
# MAGIC 
# MAGIC Using the DataFrame API, Spark allows us to interact with a stream of incoming data in much the same way that we did with a batch of data.  

# COMMAND ----------

# MAGIC %md
# MAGIC Import a `spark_udf`

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.ensemble import RandomForestRegressor

with mlflow.start_run(run_name="Final RF Model") as run: 
  df = pd.read_csv("/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv")
  X = df.drop(["price"], axis=1)
  y = df["price"]

  rf = RandomForestRegressor(n_estimators=100, max_depth=5)
  rf.fit(X, y)
  
  mlflow.sklearn.log_model(rf, "random-forest-model")
  
  runID = run.info.run_id
  experimentId = run.info.experiment_id
  URI = run.info.artifact_uri

# COMMAND ----------

# MAGIC %md
# MAGIC Create a UDF from the model you just trained in `sklearn` so that you can apply it in Spark.

# COMMAND ----------

import mlflow.pyfunc

pyfunc_udf = mlflow.pyfunc.spark_udf(spark, URI + "/random-forest-model")

# COMMAND ----------

# MAGIC %md
# MAGIC Before working with our stream, we need to establish a stream name so that we can have better control over it.

# COMMAND ----------

myStreamName = "lesson03_stream"

# COMMAND ----------

# MAGIC %md
# MAGIC Next create a utility method that blocks until the stream is actually "ready" for processing.

# COMMAND ----------

def until_stream_is_ready(name, progressions=3):
  import time
  
  # Get the query identified by "name"
  queries = list(filter(lambda query: query.name == name, spark.streams.active))

  # We need the query to exist, and progress to be >= "progressions"
  while (len(queries) == 0 or len(queries[0].recentProgress) < progressions):
    time.sleep(5) # Give it a couple of seconds
    queries = list(filter(lambda query: query.name == name, spark.streams.active))

  print("The stream {} is active and ready.".format(name))


# COMMAND ----------

# MAGIC %md
# MAGIC Now we can transform the stream with a prediction and preview it with the **`display()`** command.

# COMMAND ----------

predictionsDF = streamingData.withColumn("prediction", pyfunc_udf(*streamingData.columns))

display(predictionsDF, streamName=myStreamName)

# COMMAND ----------

until_stream_is_ready(myStreamName)

# COMMAND ----------

# When you are done previewing the results, stop the stream.
for stream in spark.streams.active:
  print(f"Stopping {stream.name}")
  stream.stop() # Stop the stream

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Write out a Stream of Predictions
# MAGIC 
# MAGIC You can perform writes to any target database.  In this case, write to a Delta file.  This file will always be up to date, another component of an application can query this endpoint at any time.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Delta is an open-source storage layer that brings ACID transactions to Spark and big data workloads.  It is built on the Parquet format.  <a href="https://databricks.com/product/databricks-delta" target="_blank">Find out more about Delta here.</a>

# COMMAND ----------

dbutils.fs.rm(f"/users/{user_name}/mlflow_demos/ml-deployment", True)
checkpointLocation = f"/users/{user_name}/mlflow_demos/ml-deployment/stream.checkpoint"
writePath = f"/users/{user_name}/mlflow_demos/ml-deployment/predictions"

(predictionsDF
  .writeStream                                           # Write the stream
  .queryName(myStreamName)                               # Name the query
  .format("delta")                                       # Use the delta format
  .partitionBy("zipcode")                                # Specify a feature to partition on
  .option("checkpointLocation", checkpointLocation)      # Specify where to log metadata
  .option("path", writePath)                             # Specify the output path
  .outputMode("append")                                  # Append new records to the output path
  .start()                                               # Start the operation
)

# COMMAND ----------

until_stream_is_ready(myStreamName)

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the underlying file.
# MAGIC 
# MAGIC Refresh this a few times to note the changes.

# COMMAND ----------

from pyspark.sql.utils import AnalysisException

total = spark.read.format("delta").load(writePath).count()
print(total)

# COMMAND ----------

# When you are done previewing the results, stop the stream.
for stream in spark.streams.active:
  print(f"Stopping {stream.name}")
  stream.stop() # Stop the stream

# COMMAND ----------

# MAGIC %md
# MAGIC Things to note:<br><br>
# MAGIC 
# MAGIC * For batch processing, you can trigger a stream every 24 hours to maintain state.
# MAGIC * You can easily combine historic and new data in the same stream.
