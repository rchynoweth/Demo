# Databricks notebook source
# MAGIC %md
# MAGIC # This is an AWS Example! 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Load new data from S3 with Auto Loader into a Delta table

# COMMAND ----------

# NOTE - If using this function in DBR 8.0 you need to import the pyspark sql functions with the form:
#    import pyspark.sql.functions as fn
# If you just import with this form:
#    from pyspark.sql.functions import *
# Then the below use of the python filter function will bomb with an error - it will try to apply the pyspark filter function 
# instead of the python filter function.  The specific error it will return is:
# TypeError: Invalid argument, not a string or column: <function untilStreamIsReady.<locals>.<lambda> at 0x7f4e2e544280> of type <class 'function'>. For column literals, use 'lit', 'array', 'struct' or 'create_map' function.
def untilStreamIsReady(name):
  queries = list(filter(lambda query: query.name == name, spark.streams.active))

  if len(queries) == 0:
    print("The stream is not active.")

  else:
    while (queries[0].isActive and len(queries[0].recentProgress) == 0):
      pass # wait until there is any type of progress

    if queries[0].isActive:
      queries[0].awaitTermination(5)
      print("The stream is active and ready.")
    else:
      print("The stream is not active.")

None

# COMMAND ----------

from pyspark.sql.types import *

encountersSchema = StructType([StructField("Id", StringType(), True),
                 StructField("start", TimestampType(), True),
                 StructField("stop", TimestampType(), True),
                 StructField("patient", StringType(), True),
                 StructField("provider", StringType(), True),
                 StructField("encounterclass", StringType(), True),
                 StructField("code", IntegerType(), True),
                 StructField("description", StringType(), True),
                 StructField("cost", DoubleType(), True),
                 StructField("reasoncode", LongType(), True),
                 StructField("reasondescription", StringType(), True)
                ])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Documentation for the Auto Loader: https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html

# COMMAND ----------

# Read from S3 using the Auto-Loader
# There are two ways to use the Auto-Loader - Directory listing and Notifications.  Directory listing is the default, and only requires permissions on the S3 bucket that you want to read
# When a new stream is first set up using the useNotifications option, the Auto-Loader automatically spins up AWS SQS and SNS resources that get events from the input directory
encountersInputDf = (
  spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.region", "us-west-2")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("cloudFiles.useNotifications", "true")
    .option("cloudFiles.validateOptions", "true")
    # This option is required for when there are S3 partition directories present in the form of hr=17.  To ignore all partition directories set the value to ""
    # To add partition directory values as part of the dataset they must be specified in the schema above at the end
    # If there are partition directories present on S3 and this option is not specified, it will automatically detect and try to add partition directory values to the dataset and fail
    #.option("cloudFiles.partitionColumns", "client_id, hr") 
    .schema(encountersSchema)
    .load("/mnt/achutestdata/autoloader")
)


# COMMAND ----------

#.trigger(once=True) \

# Write to the raw Delta table
encountersInputDf.writeStream \
  .format("delta") \
  .option("checkpointLocation", "/mnt/achutestdata/demodeltadb/encounters_raw/_checkpoint") \
  .queryName("autoLoaderTest") \
  .start("/mnt/achutestdata/demodeltadb/encounters_raw")

untilStreamIsReady("autoLoaderTest")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table achu_demo.encounters_raw
# MAGIC using delta
# MAGIC location "/mnt/achutestdata/demodeltadb/encounters_raw"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- 249970
# MAGIC select * from achu_demo.encounters_raw limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add a new file to the Auto Loader location, see it load above and see the table count change

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC cp dbfs:/mnt/achutestdata/rawdata/encountersDf2011.csv/part-00000-tid-4115110809915429426-befa8fce-a640-4c1e-b2d7-7eba8bf4264b-9689-1-c000.csv dbfs:/mnt/achutestdata/autoloader/

# COMMAND ----------

# MAGIC %md
# MAGIC #### Manage Auto Loader resources

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.databricks.sql.CloudFilesAWSResourceManager
# MAGIC val manager = CloudFilesAWSResourceManager
# MAGIC   .newManager
# MAGIC   .option("cloudFiles.region", "us-west-2")
# MAGIC   .create()
# MAGIC 
# MAGIC // List notification services created by Auto Loader
# MAGIC val resourceList = manager.listNotificationServices()
# MAGIC display(resourceList)
# MAGIC 
# MAGIC // Tear down the notification services created for a specific stream ID.
# MAGIC // Stream ID is a GUID string that you can find in the list result above.
# MAGIC //manager.tearDownNotificationServices(<stream-id>)

# COMMAND ----------

# MAGIC %scala
# MAGIC val myResource = resourceList.select('bucketName, 'eventFilter, 'topicArn, 'subscriptionArn, 'queueUrl).filter("bucketName = 'achu-test-data'")
# MAGIC display(myResource)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Merge into a Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC create table achu_demo.encounters_raw
# MAGIC using delta
# MAGIC location "/mnt/achutestdata/demodeltadb/encounters_raw"

# COMMAND ----------

# Readstream to read new records from the raw Delta table
encountersRawDf = (
  spark.readStream
    .format("delta")
    .table("achu_demo.encounters_raw")
)

# COMMAND ----------

from delta.tables import *

encountersLatest = DeltaTable.forName(spark, "achu_demo.encounters_latest")

# COMMAND ----------

# Function that will update rows with the same key and insert rows with new keys
# It also only takes the latest if there are multiple rows for a given key in the set being merged
def foreach_batch_function(df, epoch_id):
    df.createOrReplaceTempView("encountersChanges")
    
    df._jdf.sparkSession().sql("""
      MERGE INTO achu_demo.encounters_latest et
      USING (
        SELECT ec.*
        FROM encountersChanges ec
        JOIN (SELECT patient, max(stop) as maxStop from encountersChanges GROUP BY patient) latest
        ON latest.patient = ec.patient AND latest.maxStop = ec.stop
      ) ec
      ON et.patient = ec.patient
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

encountersRawDf.writeStream \
  .foreachBatch(foreach_batch_function) \
  .trigger(once=True) \
  .option("checkpointLocation", "/mnt/achutestdata/demodeltadb/encounters_latest/_checkpoint") \
  .start() 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 11069
# MAGIC select count(1) from achu_demo.encounters_latest

# COMMAND ----------

# MAGIC %sql
# MAGIC select encounterclass, min(start), max(start) 
# MAGIC from achu_demo.encounters_latest
# MAGIC group by 1
# MAGIC order by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select date_format(start, 'yyyy-MM') as service_date, encounterclass, count(1)
# MAGIC from achu_demo.encounters_latest
# MAGIC where start >= '2013-01-01 00:00:00'
# MAGIC group by 1, 2
# MAGIC order by 1, 2

# COMMAND ----------


