# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/databricks icon.png?raw=true" width=100/> 
# MAGIC 
# MAGIC ## Delta Lake and Structured Streaming
# MAGIC 
# MAGIC __NOTE:__ Use a cluster running Databricks 7.3 ML or higher.

# COMMAND ----------

dbutils.widgets.text("DatabaseName", "rac_demo_db")
dbutils.widgets.text("UserName", "ryan.chynoweth@databricks.com")
database_name = dbutils.widgets.get("DatabaseName")
user_name = dbutils.widgets.get("UserName")
spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))
spark.sql("USE {}".format(database_name))

# COMMAND ----------

setup_responses = dbutils.notebook.run("./includes/2_setup", 0, {"database_name": database_name, "user_name": user_name}).split()

local_data_path = setup_responses[0]
dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]

print(f"Path to be used for Local Files: {local_data_path}")
print(f"Path to be used for DBFS Files: {dbfs_data_path}")
print(f"Database Name: {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is a Stream? <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/spark-streaming-logo.png?raw=true" width=200/>
# MAGIC 
# MAGIC The "traditional" way to process data is in batches.  In a __batch__, a collection of data is complete and "at rest" *before* we access it.
# MAGIC 
# MAGIC In contrast, a data __stream__ is a __continuous flow of data__.  When we access a stream, we expect that new data will appear *after* we open the stream.  The data may arrive in regular or irregular intervals.  Common __examples__ of streams include:
# MAGIC 
# MAGIC - Telemetry from devices
# MAGIC - Log files from applications
# MAGIC - Transactions created by users
# MAGIC 
# MAGIC Common stream __technologies__ include:
# MAGIC 
# MAGIC - Sockets
# MAGIC - Java Message Service (JMS) applications, including RabbitMQ, ActiveMQ, IBM MQ, etc.
# MAGIC - Apache Kafka
# MAGIC 
# MAGIC As we'll see in this workshop, __Delta Lake__ is also a highly effective solution for streaming use cases.  
# MAGIC 
# MAGIC The diagram below outlines the approach Spark takes to deal with streams.  We're using the newer Spark streaming technology, which is known as __Structured Streaming__.
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/spark-streaming-basic-concept.png?raw=true" />
# MAGIC 
# MAGIC As the diagram above illustrates, Spark considers a stream to be an "unbounded" or "infinite" table.  This is a brilliant concept because it allows us to use SQL against a stream.  However, as we shall see, there are some limitations.  
# MAGIC 
# MAGIC With Databricks Delta we can complete inserts, merges, updates, and deletes as a stream!

# COMMAND ----------

# Here is a bit of Spark housekeeping.  When we aggregate our stream later in this demo, Spark will need to shuffle data.
# For our small demo data size, we'll get much better performance by reducing the number of shuffle partitions.
# Default is 200.  Most of them won't be used, but each will have to wait for an available core, then start up and shut down.
# Let's run this cell to reduce the number of shuffle partitions.

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a stream
# MAGIC 
# MAGIC A Delta Lake table can be a streaming source, sink, or both.
# MAGIC 
# MAGIC We're going to use a __Delta Lake table__, `readings_stream_source`, as our __stream source__.  It's just an ordinary Delta Lake table, but when we run "readStream" against it below, it will become a streaming source.
# MAGIC 
# MAGIC The table doesn't contain any data yet.  We'll initiate the stream now, and then later we'll generate data into the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS readings_stream_source;
# MAGIC 
# MAGIC CREATE TABLE readings_stream_source
# MAGIC   ( id INTEGER, 
# MAGIC     reading_time TIMESTAMP, 
# MAGIC     device_type STRING,
# MAGIC     device_id STRING,
# MAGIC     device_operational_status STRING,
# MAGIC     reading_1 DOUBLE,
# MAGIC     reading_2 DOUBLE,
# MAGIC     reading_3 DOUBLE )
# MAGIC   USING DELTA 

# COMMAND ----------

# Create a stream from the landing table
# To read data as a stream, we simply call readStream instead of read.
# Complete this call... use "delta" as the format, and read table "readings_stream_source"
readings_stream = (spark
                   .readStream
                   .format("delta")
                   .table("readings_stream_source"))

# Register the stream as a temporary view so we can run SQL on it
readings_stream.createOrReplaceTempView("readings_streaming")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Consume the stream in APPEND mode
# MAGIC 
# MAGIC First we'll read all incoming records at the individual record level as they come in on the stream.
# MAGIC 
# MAGIC Here we'll just read the records, but keep in mind that we could perform arbitrary processing on them, including writing them out to another table.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from readings_streaming

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consuming streams in aggregate: Initiate 3 stream listeners
# MAGIC 
# MAGIC Execute the three cells below to initiate three aggregate stream listeners.  The comments in each cell will explain what each listener does.
# MAGIC 
# MAGIC After we initiate these listeners, we'll be ready to begin generating data into the stream.
# MAGIC 
# MAGIC Above we started an example of detail-level append streaming.  Now we'll concentrate on ways to __aggregate__ streams into useful summaries.
# MAGIC 
# MAGIC Take special note of the difference between __full aggregation__ and __windowed aggregation__, which we use in the second and third listeners.  The diagram below shows how windowed aggregation works:
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/databricks-demo-delta-streaming/blob/master/images/windowed_aggregation.png?raw=true" />
# MAGIC 
# MAGIC As the diagram illustrates, windowed aggregation uses only a subset of the data.  This subset is typically chosen based on a timestamp range.  Note that the timestamp is from the data itself, not from the current time of our code in this notebook.  In the diagram above, we are using a timestamp column to select data within 10-minute ranges.
# MAGIC 
# MAGIC In addition, we can __slide__ the window.  In the diagram above, we are sliding the 10-minute window every 5 minutes.  

# COMMAND ----------

# MAGIC %sql
# MAGIC -- full aggregation
# MAGIC 
# MAGIC -- This is a streaming query that aggregates the entire stream "from the beginning of time."
# MAGIC 
# MAGIC -- Start this query now.  It will initialize, then remain inactive until we start generating data in a cell further down.
# MAGIC 
# MAGIC 
# MAGIC SELECT 
# MAGIC   device_id, device_type, avg(reading_1)
# MAGIC FROM readings_streaming
# MAGIC GROUP BY device_type, device_id 
# MAGIC ORDER BY device_type ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- windowed aggregation
# MAGIC 
# MAGIC -- This is also a streaming query that reads the readings_streaming stream.
# MAGIC -- It also calculates the average reading_1 for each device type, and also the COUNT for each device_type.
# MAGIC -- However, note that this average and count is NOT calculated for the entire stream, since "the beginning of time."
# MAGIC -- Instead, this query serves a "dashboard" use case, because it calculates the average reading over a 2-minute window of time.
# MAGIC -- Furthermore, this 2-minute window "slides" by a minute once per minute.
# MAGIC 
# MAGIC -- Start this query now.  It will initialize, then remain inactive until we start generating data in a cell further down.
# MAGIC 
# MAGIC SELECT 
# MAGIC   device_type
# MAGIC   , count(1)
# MAGIC   , avg(reading_1)
# MAGIC   , window(reading_time, "120 seconds", "60 seconds")
# MAGIC 
# MAGIC   
# MAGIC -- this is a reserved word that lets us define windows
# MAGIC FROM readings_streaming 
# MAGIC GROUP BY device_type, window(reading_time, "120 seconds", "60 seconds") -- window allows you to aggregate by column, window size, and sliding size
# MAGIC 
# MAGIC ORDER BY 
# MAGIC   window DESC, 
# MAGIC   device_type ASC 
# MAGIC LIMIT 4 -- this lets us see the last two window aggregations for device_type

# COMMAND ----------

# windowed aggregation WITH STREAMING OUTPUT

# This is the same aggregate query we ran above (except we remove the "LIMIT 4", but here we also WRITE the aggregated stream to a Delta Lake table.
# We could have done all this in the original query, of course, but we wanted to introduce the concepts one at a time.

# This is useful because now another independent application can read the Delta Lake pre-aggregated table at will to 
# easily produce a dashboard or other useful output.

# Start this query now.  It will initialize, then remain inactive until we start generating data in a cell further down.
checkpoint_location = f"/Users/{user_name}/ckpts/_streaming_ckpnt"
dbutils.fs.rm(checkpoint_location, True)

out_stream = spark.sql(""" SELECT 
        device_type
        , count(1) as device_cnt
        , avg(reading_1) as reading_1_avg
        , window(reading_time, "120 seconds", "60 seconds")

      FROM readings_streaming 
      GROUP BY device_type, window(reading_time, "120 seconds", "60 seconds") -- window allows you to aggregate by column, window size, and sliding size

      ORDER BY 
        window DESC
        , device_type ASC 
  """)

# Here we create a new output stream, which is an aggregated version of the data from our original stream.
# Note that we use .outputMode("complete")...
# Spark Structured Streaming has three output modes:
#  - append - this is used when we want to write detail-level records.  You cannot query an entire table of detail records, 
#             you can only see the latest data in the stream.
# - complete - however, if you query a stream with an aggregate query, Spark will retain and update the entire aggregate.
#              In this mode, we will continue to output new versions of the entire aggregation result set.
# - update - this is similar to complete, and is also used for aggregations.  The difference is that "update" will only
#            write the *changed* rows of an aggregate result set.


(
out_stream.writeStream
  .format("delta")
  .outputMode("complete")
  .option("checkpointLocation", checkpoint_location)
  .table("readings_agg")
)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Begin generating data
# MAGIC 
# MAGIC Now we're ready to start pumping data into our stream.
# MAGIC 
# MAGIC The cell below generates transaction records into the Delta Lake table.  Our stream queries above will then consume this data.

# COMMAND ----------

# Now let's simulate an application that streams data into our landing_point table

import time

next_row = 0

while(next_row < 14):
  
  time.sleep(1)

  next_row += 1
  
  spark.sql(f"""
    INSERT INTO readings_stream_source (
      SELECT * FROM current_readings_labeled
      WHERE id = {next_row} )
  """)
  
  


# COMMAND ----------

# MAGIC %md
# MAGIC ### Examine the 4 streaming queries we started above.  
# MAGIC 
# MAGIC Take a look at the data generated by our four streaming readers.  Notice the difference between detail-level, full aggregation, and windowed aggregation.
# MAGIC 
# MAGIC Also click <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/data_icon.png?raw=true" /> in the left sidebar and note that our stream writer has created a new table, readings_agg.

# COMMAND ----------

# Cancel all running streams

for s in spark.streams.active:
  s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Examine the new sink we created
# MAGIC 
# MAGIC Let's take a look at the new Delta Lake table we created above using writeStream().
# MAGIC 
# MAGIC This would be a great data source for a separate dashboard application.  The application could simply query the table at will, and format the results.  It wouldn't have to worry at all about streaming or aggregation, since the information has already been prepared and formatted.
# MAGIC 
# MAGIC __NOTE:__ You will NOT be able to run the cell below until you cancel any running streams above.  That's because you are running in the same notebook as the streams.  If the cell below were in a separate notebook (which is a more real-world scenario), you would be able to run it concurrently with the streams.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now imagine that this is our independent dashboard-building program.
# MAGIC -- It can read the pre-aggregated data on Delta Lake at will, and simply display the data.
# MAGIC 
# MAGIC select device_type, device_cnt, reading_1_avg, window.start as start_time, window.end as end_time
# MAGIC 
# MAGIC from readings_agg

# COMMAND ----------

# MAGIC %md 
# MAGIC Here I will split the device types into two different tables and use the trigger once functionality available in the streaming APIs. 

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

filter_checkpoint = f"/Users/{user_name}/ckpts/streaming_ckpnt_filter"

dbutils.fs.rm(filter_checkpoint, True)

# COMMAND ----------

def transform_stream(microBatchDF, batchId):
    # TRANSFORMER
    (microBatchDF.filter(col("device_type") == "TRANSFORMER")
     .write
     .format("delta")
     .mode("append")
     .saveAsTable("transformer_readings_agg") )
    
    # RECTIFIER
    (microBatchDF.filter(col("device_type") == "RECTIFIER")
     .write
     .format("delta")
     .mode("append")
     .saveAsTable("rectifier_readings_agg") )

# COMMAND ----------

# using the same stream as before
out_stream = spark.sql(""" SELECT 
        device_type
        , count(1) as device_cnt
        , avg(reading_1) as reading_1_avg
        , window(reading_time, "120 seconds", "60 seconds")

      FROM readings_streaming 
      GROUP BY device_type, window(reading_time, "120 seconds", "60 seconds") -- window allows you to aggregate by column, window size, and sliding size
  """)


# COMMAND ----------

(out_stream.writeStream
    .format("delta")
    .option("checkpointLocation", filter_checkpoint)
    .trigger(once=True)
    .foreachBatch(transform_stream)
    .outputMode("update")
    .start())

# COMMAND ----------

display(
  spark.sql("""
      select * from transformer_readings_agg
""")
)

# COMMAND ----------

display(
  spark.sql("""
      select * from rectifier_readings_agg
""")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### What just happened?
# MAGIC 
# MAGIC We use `readStream` to read streaming input from a variety of input sources and create a DataFrame.
# MAGIC 
# MAGIC We captured a detail-level stream, and we saw examples of stream aggregation.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#" target="_blank">Structured Streaming Programming Guide</a>
# MAGIC * <a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">A Deep Dive into Structured Streaming</a> by Tathagata Das. This is an excellent video describing how Structured Streaming works.

# COMMAND ----------


