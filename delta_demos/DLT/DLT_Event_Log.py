# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables - The Event Log
# MAGIC 
# MAGIC Tips and Best Practices:
# MAGIC - Centralize Delta Live Table Pipelines in a single storage location
# MAGIC   - Let's assume you have a pipeline, `demo_dlt_pipeline` and you have an Azure Data Lake Storage Gen2 account and container respectfully named: `adlsstorage` and `adlscontainer`. Then please use the following URI: `abfss://adlscontainer@adlsstorage.dfs.core.windows.net/dlt_pipelines/demo_dlt_pipeline`. 
# MAGIC   - This allows you to easily map the pipeline to the folder by name. 
# MAGIC   - If you have another pipeline, `demo_dlt_pipeline2` then the URI would be: `abfss://adlscontainer@adlsstorage.dfs.core.windows.net/dlt_pipelines/demo_dlt_pipeline2`. 
# MAGIC   - This allows you to read both event logs with `spark.read.load("abfss://adlscontainer@adlsstorage.dfs.core.windows.net/dlt_pipelines/*/system/events")`. 
# MAGIC   - Please note that the `dlt_pipielines` directory can be named anything you wish and can have any number of subdirectories.
# MAGIC   - Do not put a pipeline location at the container base directory and always provide an external location. 
# MAGIC   
# MAGIC - 
# MAGIC 
# MAGIC 
# MAGIC Public Resources: 
# MAGIC - https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-event-log.html#data-quality

# COMMAND ----------

event_log_path = "dbfs:/pipelines/878e907e-2a8f-43e2-a056-a90402977cec"

# COMMAND ----------

dbutils.fs.ls(path+"/system/events")

# COMMAND ----------

df = spark.read.load(path+"/system/events")
df.createOrReplaceTempView("event_log_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from event_log_raw 

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct event_type from event_log_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from event_log_raw where event_type in ('flow_definition', 'dataset_definition')
# MAGIC   and timestamp >= '2022-10-26T16:00:00'
# MAGIC order by timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from event_log_raw where event_type = 'flow_progress'
# MAGIC   and timestamp >= '2022-10-26T16:00:00'
# MAGIC order by timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   flow_id,
# MAGIC --   name as expectation,
# MAGIC   SUM(num_output_rows) as passing_records,
# MAGIC   SUM(dropped_records) as failing_records
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT 
# MAGIC       timestamp 
# MAGIC     , origin.pipeline_id
# MAGIC     , origin.pipeline_name
# MAGIC     , origin.cluster_id
# MAGIC     , origin.update_id
# MAGIC     , origin.flow_id
# MAGIC     , origin.flow_name
# MAGIC     , origin.batch_id
# MAGIC     , origin.table_id
# MAGIC     , origin.table_name
# MAGIC     , details:flow_progress:status 
# MAGIC     , details:flow_progress:metrics
# MAGIC     , details:flow_progress:metrics:num_output_rows
# MAGIC     , details:flow_progress:data_quality
# MAGIC     , details:flow_progress:data_quality:dropped_records
# MAGIC     , details:flow_progress:dataset
# MAGIC     , details:flow_progress:expectation
# MAGIC     FROM
# MAGIC       event_log_raw
# MAGIC     WHERE
# MAGIC       event_type = 'flow_progress' and timestamp >= '2022-10-26T16:00:00'
# MAGIC   )
# MAGIC GROUP BY
# MAGIC   flow_id

# COMMAND ----------


