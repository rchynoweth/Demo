# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Logs are created on a per cluster basis and added to sub directories that are the cluster id. 
# MAGIC
# MAGIC At the top leve there are three folders: `driver`, `eventlog`, and `executor`. 
# MAGIC
# MAGIC The driver has three log files in its directory. 
# MAGIC
# MAGIC The event log has two sub directories i.e. `eventlog/*/*/eventlog`.  
# MAGIC
# MAGIC The executor director has two subfolders as well i.e. `executor/*/*/stderr` or stout. 
# MAGIC
# MAGIC It looks like event logs are automatically zipped every 15 minutes. But if the cluster is run for less than 15 minutes then there is only the one file. For historical data we will want to stream the gzip files. 
# MAGIC
# MAGIC To get current data then we can create views. But how do we maky sure the clusters that are terminated we get that data read a single time?? 

# COMMAND ----------

spark.conf.set('spark.sql.jsonParserOptions.allowUnquotedFieldNames', 'true') # there is a duplicate column name issue with some event log data with a column named 'timestamp' which this solves

# COMMAND ----------

import dlt 

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

cluster_id = "0819-192424-mug721"
# log_location = "/cluster-logs/*"
log_location = f"/cluster-logs/{cluster_id}"

# event logs
spark_event_logs = f"{log_location}/eventlog/*/*/"

print(spark_event_logs)

# COMMAND ----------

def clean_column_names(df):
  for c in df.columns:
      df = df.withColumnRenamed(c, c.replace(' ','').replace('.',''))
  return df 

# COMMAND ----------

def get_cluster_id(col_name='input_file_name', sep='/', index=2):
  return split(col(col_name), sep).getItem(index)

# COMMAND ----------

# let's make sure this is correct in each environment
spark_event_log_schema = StructType([
    StructField("App ID", StringType(), True),
    StructField("App Name", StringType(), True),
    StructField("Block Manager ID", StringType(), True),
    StructField("Build Properties", StringType(), True),
    StructField("Classpath Entries", StringType(), True),
    StructField("Completion Time", StringType(), True),
    StructField("Event", StringType(), True),
    StructField("Executor ID", StringType(), True),
    StructField("Executor Info", StringType(), True),
    StructField("Executor Resource Requests", StringType(), True),
    StructField("Hadoop Properties", StringType(), True),
    StructField("JVM Information", StringType(), True),
    StructField("Job ID", StringType(), True),
    StructField("Job Result", StringType(), True),
    StructField("Maximum Memory", StringType(), True),
    StructField("Maximum Offheap Memory", StringType(), True),
    StructField("Maximum Onheap Memory", StringType(), True),
    StructField("Properties", StringType(), True),
    StructField("RDD ID", StringType(), True),
    StructField("Removed Reason", StringType(), True),
    StructField("Resource Profile Id", StringType(), True),
    StructField("Rollover Number", StringType(), True),
    StructField("Spark Properties", StringType(), True),
    StructField("Spark Version", StringType(), True),
    StructField("SparkContext Id", StringType(), True),
    StructField("Stage Attempt ID", StringType(), True),
    StructField("Stage ID", StringType(), True),
    StructField("Stage IDs", StringType(), True),
    StructField("Stage Info", StringType(), True),
    StructField("Stage Infos", StringType(), True),
    StructField("Submission Time", StringType(), True),
    StructField("System Properties", StringType(), True),
    StructField("Task End Reason", StringType(), True),
    StructField("Task Executor Metrics", StringType(), True),
    StructField("Task Info", StringType(), True),
    StructField("Task Metrics", StringType(), True),
    StructField("Task Resource Requests", StringType(), True),
    StructField("Task Type", StringType(), True),
    StructField("Timestamp", StringType(), True),
    StructField("User", StringType(), True),
    StructField("accumUpdates", StringType(), True),
    StructField("closeTime", StringType(), True),
    StructField("database", StringType(), True),
    StructField("description", StringType(), True),
    StructField("details", StringType(), True),
    StructField("errorMessage", StringType(), True),
    StructField("exception", StringType(), True),
    StructField("executionId", StringType(), True),
    StructField("executionPlan", StringType(), True),
    StructField("extraTags", StringType(), True),
    StructField("finishTime", StringType(), True),
    StructField("groupId", StringType(), True),
    StructField("id", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("kind", StringType(), True),
    StructField("modifiedConfigs", StringType(), True),
    StructField("name", StringType(), True),
    StructField("parsedTime", StringType(), True),
    StructField("physicalPlanDescription", StringType(), True),
    StructField("progress", StringType(), True),
    StructField("rootExecutionId", StringType(), True),
    StructField("runId", StringType(), True),
    StructField("sessionId", StringType(), True),
    StructField("sparkPlanInfo", StringType(), True),
    StructField("startTime", StringType(), True),
    StructField("statement", StringType(), True),
    StructField("time", StringType(), True),
    StructField("userName", StringType(), True),
    StructField("_rescued_data", StringType(), True),
    # StructField("input_file_name", StringType(), True), # added after the read
    # StructField("cluster_id", StringType(), True), # added after the read
    StructField("cloudHealthy", StringType(), True),
    StructField("h2oBuildInfo", StringType(), True),
    StructField("h2oClusterInfo", StringType(), True),
    StructField("memoryInfo", StringType(), True),
    StructField("sqlPlanMetrics", StringType(), True),
    StructField("swProperties", StringType(), True),
    StructField("timeInMillis", StringType(), True)
])

# COMMAND ----------

schema_hints = ""
for s in spark_event_log_schema:
  schema_hints += f"`{str(s.name)}` {str(s.dataType).replace('Type()', '')}, "

# schema_hints[:-2]

# COMMAND ----------

# DBTITLE 1,Bronze Event Log Data 
###
## Table for event log snapshots
### 
@dlt.table(name='spark_event_logs_bronze')
def spark_event_logs_bronze():
  ## USE FRAMEWORK FOR READ
  df = (
    spark
    .readStream
    .format('cloudFiles')
    .option('cloudFiles.format', 'json')
    .option('cloudFiles.backfillInterval', '1 day')
    .option('cloudFiles.allowOverwrites', 'true')
    .option('cloudFiles.schemaHints', schema_hints[:-2])
    .load(spark_event_logs)
    .withColumn("input_file_name", input_file_name() )
    .withColumn('cluster_id', get_cluster_id())
  )

  # ADD THIS CLEAN FUNCTION TO LIBRARY AS TRANSFORM
  df = clean_column_names(df)
  return df

# COMMAND ----------

@dlt.view(name='spark_event_types')
def spark_event_types():
  return dlt.read('spark_event_logs_bronze').select('Event').distinct()

# COMMAND ----------

# DBTITLE 1,Silver Event Log - Tracking Driver and Executors
@dlt.table(name='spark_vm_events_silver')
def cluster_vm_events_silver():
  return (dlt.read_stream('spark_event_logs_bronze')
          .filter(col('Event').isin(['SparkListenerBlockManagerAdded', 'SparkListenerExecutorRemoved', 'SparkListenerExecutorAdded']))
          .filter( (get_json_object(col("BlockManagerID"), "$.Executor ID") == "driver") | (col("ExecutorID").isNotNull() & get_json_object(col("BlockManagerID"), "$.Executor ID").isNull()) ) # Gets rid of the extra rows when executors are added
          .select(
            col("cluster_id"),
            when(get_json_object(col("BlockManagerID"), "$.Executor ID").isNotNull(), get_json_object(col("BlockManagerID"), "$.Executor ID")).otherwise(col("ExecutorID")).alias("ExecutorID"), # get the proper executor id because of the driver being different
            get_json_object(col("BlockManagerID"), "$.Host").alias("HostIP"),
            get_json_object(col("BlockManagerID"), "$.Port").alias("HostPort"),
            col("BlockManagerID"),
            col("MaximumMemory"),
            col("MaximumOnheapMemory"),
            col("MaximumOffheapMemory"),
            col("Event"),
            col("ExecutorInfo"),
            get_json_object(col("ExecutorInfo"), "$.Total Cores").alias("TotalCores"),
            from_unixtime(col("Timestamp")/1000).alias("datetime")
      )
  )



# COMMAND ----------

# DBTITLE 1,Silver Task Events
@dlt.table(name='spark_tasks_silver')
def spark_tasks_silver():
  return (
    dlt.read_stream('spark_event_logs_bronze').filter(col('Event').isin(['SparkListenerTaskGettingResult','SparkListenerTaskStart', 'SparkListenerTaskEnd']))
    . select(
      'cluster_id',
      'StageID',
      'Event', 
      'StageAttemptID', 
      'TaskEndReason', 
      'TaskExecutorMetrics', 
      'TaskInfo', 
      'TaskMetrics', 
      'TaskResourceRequests', 
      'TaskType',
      get_json_object(col("TaskInfo"), "$.Task ID").alias("TaskID"),
      get_json_object(col("TaskInfo"), "$.Task Attempt").alias("TaskAttemptID"),
      get_json_object(col("TaskInfo"), "$.Executor ID").alias("ExecutorID"),
      get_json_object(col("TaskInfo"), "$.Host").alias("Host"),
      get_json_object(col("TaskInfo"), "$.Launch Time").alias("TaskStartTime"),
      get_json_object(col("TaskInfo"), "$.Finish time").alias("TaskEndTime"),
      get_json_object(col("TaskInfo"), "$.Accumulables").alias("Accumulables"),
      get_json_object(col("TaskMetrics"), "$.Peak Execution Memory").alias("PeakExecutionMemory"),
      get_json_object(col("TaskMetrics"), "$.Result Size").alias("ResultSize"),
      get_json_object(col("TaskMetrics"), "$.Memory Bytes Spilled").alias("MemoryBytesSpilled"),
      get_json_object(col("TaskMetrics"), "$.Disk Bytes Spilled").alias("DiskBytesSpilled"),
      get_json_object(col("TaskMetrics"), "$.Shuffle Read Metrics.Total Records Read").alias("TotalRecordsRead"),
      get_json_object(col("TaskMetrics"), "$.Shuffle Write Metrics.Shuffle Bytes Written").alias("ShuffleBytesWritten"),
      get_json_object(col("TaskMetrics"), "$.Shuffle Write Metrics.Shuffle Write Time").alias("ShuffleWriteTime"),
      get_json_object(col("TaskMetrics"), "$.Shuffle Write Metrics.Shuffle Records Written").alias("ShuffleRecordsWritten"),
      get_json_object(col("TaskMetrics"), "$.Input Metrics.Bytes Read").alias("InputBytes"),
      get_json_object(col("TaskMetrics"), "$.Input Metrics.Records Read").alias("InputRecords"),
      get_json_object(col("TaskMetrics"), "$.Output Metrics.Bytes Written").alias("OutputBytes"),
      get_json_object(col("TaskMetrics"), "$.Output Metrics.Records Written").alias("OutputRecords")
    )
  )

# COMMAND ----------

@dlt.table(name='spark_jobs_silver')
def spark_jobs_silver():
  return (
    dlt.read_stream('spark_event_logs_bronze')
    .filter(col('Event').isin(['SparkListenerJobStart', 'SparkListenerJobEnd']))
    .select(
      'JobID',
      'cluster_id',
      get_json_object(col('JobResult'), "$.Result").alias('JobResult'),
      'CompletionTime',
      from_unixtime(col("CompletionTime")).alias("CompletionDatetime"),
      'Event',
      'StageIDs',
      'StageInfos',
      'SubmissionTime',
      from_unixtime(col('SubmissionTime')).alias('SubmissionDatetime')
    )
  )

# COMMAND ----------

@dlt.table(name='spark_stages_silver')
def spark_stages_silver():
  return (
    dlt.read_stream('spark_event_logs_bronze')
    .filter(col('Event').isin(['SparkListenerStageCompleted', 'SparkListenerStageSubmitted']))
    .select(
      'cluster_id',
      get_json_object(col('StageInfo'), '$.Stage ID').alias('StageID'),
      get_json_object(col('StageInfo'), '$.Stage Attempt ID').alias('StageAttemptID'),
      get_json_object(col('StageInfo'), '$.Number Of Tasks').alias('NumberOfTasks'),
      get_json_object(col('StageInfo'), '$.Submission Time').alias('SubmissionTime'),
      from_unixtime(get_json_object(col('StageInfo'), '$.Submission Time')).alias('SubmissionDatetime'),
      get_json_object(col('StageInfo'), '$.Completion Time').alias('CompletionTime'),
      from_unixtime(get_json_object(col('StageInfo'), '$.Completion Time')).alias('CompletionDatetime'),
      'StageInfo'
    )
  )

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC
# MAGIC from rac_demo_db.spark_stages_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spark_event_types   -- SparkListenerTaskGettingResult, SparkListenerTaskStart, SparkListenerTaskEnd
# MAGIC
# MAGIC -- SparkListenerJobStart, SparkListenerJobEnd
# MAGIC
# MAGIC -- SparkListenerStageCompleted, SparkListenerStageSubmitted

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rac_demo_db.cluster_event_logs_bronze

# COMMAND ----------


