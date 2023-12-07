# Databricks notebook source


# COMMAND ----------

from prophet_forecast import ProphetForecast

from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, date, timedelta
import random, string, uuid
import pandas as pd
import time

uuidUdf= udf(lambda : uuid.uuid4().hex,StringType())

# COMMAND ----------

# get creds 
kafka_bootstrap_servers_tls = dbutils.secrets.get("oetrta", "kafka-bootstrap-servers-tls"      )

# COMMAND ----------

# Set up topic 
# Full username
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# Short form of username, suitable for use as part of a topic name.
user = username.split("@")[0].replace(".","_")

# DBFS directory for this project, we will store the Kafka checkpoint in there
project_dir = f"/home/{username}/oetrta/kafka_test"

checkpoint_location = f"{project_dir}/kafka_checkpoint_output"

output_topic = f"{user}_oetrta_kafka_test-1-output"
input_topic = f"{user}_oetrta_kafka_test-1-input"

# COMMAND ----------

print( username )
print( user )
print( project_dir )
print( checkpoint_location )
print( input_topic )

# COMMAND ----------

# GET SCHEMA  

start_date = date(2022, 1, 1)
end_date = date(2022, 12, 31)
date_list = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

# Create a Pandas DataFrame with the dates
pandas_df = pd.DataFrame(date_list, columns=["ds"])

# Convert the Pandas DataFrame to a PySpark DataFrame
date_df = spark.createDataFrame(pandas_df)

# Generate a list of integers from 1 to 1500
sku_list = list(range(1, 1501))

# Create a list of Row objects
rows = [Row(sku=sku) for sku in sku_list]

# Create a PySpark DataFrame from the list of Row objects
sku_df = spark.createDataFrame(rows).withColumn("sku", col("sku").cast(StringType()))

input_df = sku_df.crossJoin(date_df).withColumn('y', monotonically_increasing_id()).withColumn("sendTime", lit(datetime.now().timestamp()).cast("timestamp"))


input_schema = input_df.schema



# COMMAND ----------

# DBTITLE 1,Read Data From Kafka
# Read Data
startingOffsets = "earliest"

# In contrast to the Kafka write in the previous cell, when we read from Kafka we use the unencrypted endpoints.
# Thus, we omit the kafka.security.protocol property
kafka = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls )
  .option("kafka.security.protocol", "SSL")  .option("subscribe", input_topic )
  .option("startingOffsets", startingOffsets )
  .load())

score_df = kafka.select(col("key").cast("string").alias("eventId"), from_json(col("value").cast("string"), input_schema).alias("json")).select('eventId', 'json.*')

score_df = score_df.repartition('sku')

# COMMAND ----------

# define object 
pf = ProphetForecast()

# COMMAND ----------

# define function to score data 
def generate_forecast_udf(history_pd):
    return pf.generate_forecast( history_pd )

# COMMAND ----------

# DBTITLE 1,Function to Score Data
def score_prophet_model(df, epoch_id):
  # score_df = df.select(col("key").cast("string").alias("eventId"), from_json(col("value").cast("string"), input_schema).alias("json")).select('eventId', 'json.*')

  # score input
  results = (
    df
      .groupBy('sku', 'sendTime')
      .applyInPandas(generate_forecast_udf, schema=pf.forecast_result_schema)
      .withColumn('scoreDatetime', current_timestamp() )
      .withColumn("eventId", uuidUdf())
      .select(col("eventId").alias("key"), to_json(struct(col('sku'), col('ds'), col('y'), col('yhat_upper'), col('yhat_lower'), col('yhat'), col('scoreDatetime') ,col('sendTime'))).alias("value"))
  )
  
  # write results out
  (results.write
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls )
  .option("kafka.security.protocol", "SSL")
  .option("topic", output_topic)
  .save()
  )


# COMMAND ----------

# DBTITLE 1,Score and Write Data to Kafka
# Write to Kafka 
# Clear checkpoint location
dbutils.fs.rm(checkpoint_location, True)

stream_out = (score_df.writeStream
    .foreachBatch(score_prophet_model)
    .start()
    )

# COMMAND ----------

# DBTITLE 1,Read Scored Data From Kafka and Display
# Read Data
startingOffsets = "earliest"

# In contrast to the Kafka write in the previous cell, when we read from Kafka we use the unencrypted endpoints.
# Thus, we omit the kafka.security.protocol property
first_out = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls )
  .option("kafka.security.protocol", "SSL").option("subscribe", output_topic )
  .option("startingOffsets", startingOffsets )
  .load())


out = first_out.select(col("key").cast("string").alias("eventId"), col("value").cast("string"))
display(out)

# COMMAND ----------


