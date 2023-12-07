# Databricks notebook source


# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date, timedelta
import random, string, uuid
from pyspark.sql import Row
import pandas as pd
import time 

uuidUdf= udf(lambda : uuid.uuid4().hex,StringType())

# COMMAND ----------

# get creds 
kafka_bootstrap_servers_tls = dbutils.secrets.get("oetrta", "kafka-bootstrap-servers-tls"      )

# COMMAND ----------

# Set up topic 
# Full username, e.g. "aaron.binns@databricks.com"
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# Short form of username, suitable for use as part of a topic name.
user = username.split("@")[0].replace(".","_")

# DBFS directory for this project, we will store the Kafka checkpoint in there
project_dir = f"/home/{username}/oetrta/kafka_test"

topic = f"{user}_oetrta_kafka_test-1-input"

# COMMAND ----------

print( username )
print( user )
print( project_dir )
print( topic )

# COMMAND ----------

# DBTITLE 1,Dates Dataframe for 2022
start_date = date(2022, 1, 1)
end_date = date(2022, 12, 31)
date_list = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

# Create a Pandas DataFrame with the dates
pandas_df = pd.DataFrame(date_list, columns=["ds"])

# Convert the Pandas DataFrame to a PySpark DataFrame
date_df = spark.createDataFrame(pandas_df)
display(date_df)

# COMMAND ----------

# DBTITLE 1,SKU DF
# Generate a list of integers from 1 to 1500
sku_list = list(range(1, 1501))

# Create a list of Row objects
rows = [Row(sku=sku) for sku in sku_list]

# Create a PySpark DataFrame from the list of Row objects
sku_df = spark.createDataFrame(rows).withColumn("sku", col("sku").cast(StringType()))

display(sku_df)

# COMMAND ----------

input_df = sku_df.crossJoin(date_df).withColumn('y', monotonically_increasing_id()).withColumn("sendTime", lit(datetime.now().timestamp()).cast("timestamp"))

# COMMAND ----------

display(input_df.orderBy(col('sku'), col('ds')))

# COMMAND ----------

iterations = 10000

# COMMAND ----------

for i in range(1,iterations):
  print(f"Sending Data: {datetime.utcnow()} | iteration: {i}")
  (input_df.withColumn("eventId", uuidUdf())
    .select(col("eventId").alias("key"), to_json(struct(col('sku'), col('ds'), col('y') ,col('sendTime'))).alias("value"))
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls )
    .option("kafka.security.protocol", "SSL")
    .option("topic", topic)
    .save()
  )
  print(f"Sleeping: {datetime.utcnow()}")
  time.sleep(5)

# COMMAND ----------


