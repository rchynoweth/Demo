# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Table ETL - Event Logs

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

## NOTICE - must change the path 
## Example - abfss://<container_name>@<storage account name>.dfs.core.windows.net/pipelines/*/system/events
#     - Please note that 'pipelines' is a directory in ADLS storage where all pipelines are stored 
#         - For example, if I have pipeline1 and pipeline2 then they should be located at /pipelines/pipeline1 and /pipelines/pipeline2
#     - The "*" should align with the directories that contain the top-level pipeline metadata -> contains the following directories: system, tables, autoloader, checkpoints

event_log_path = "/demos/dlt/loans/ryan_chynoweth/system/events"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Raw Event Data

# COMMAND ----------

@dlt.table(name='raw_event_log', comment="{'quality':'bronze'}")
def raw_event_log():
  df = (spark.readStream
        .format('delta')
        .load(event_log_path)
        .withColumn("bronze_load_datetime", current_timestamp())
       )
  
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Tables

# COMMAND ----------

# DBTITLE 1,Distinct Event Types - complete table
@dlt.table(name='event_types', comment="{'quality': 'silver'}")
def event_types():
  return (dlt.read('raw_event_log').select('event_type').distinct())

# COMMAND ----------

# DBTITLE 1,User Actions
user_action_cols = [
  'id'
  , 'origin.pipeline_id'
  , 'origin.pipeline_name'
  , 'timestamp'
  , 'message'
  , 'level'
  , 'maturity_level'
  , 'bronze_load_datetime'
  , 'event_type'
  , 'details'
  , get_json_object('details', '$.user_action').alias('user_action')
  , get_json_object('details', '$.user_action.action').alias('action')
  , get_json_object('details', '$.user_action.user_name').alias('user_name')
  , get_json_object('details', '$.user_action.user_id').alias('user_id')
  , get_json_object('details', '$.user_action.request').alias('request') ## TO-DO -> Split this into columns
]



@dlt.table(name='user_action_details', comment="{'quality': 'silver'}")
def user_action_details():
  return (dlt.read_stream('raw_event_log')
          .filter(col('event_type') == 'user_action')
          .select(user_action_cols)
          .withColumn("silver_load_datetime", current_timestamp())
         )

# COMMAND ----------

# DBTITLE 1,Flow Events
detail_cols = [
  'id'
  , 'origin.pipeline_id'
  , 'origin.pipeline_name'
  , 'origin.flow_id'
  , 'origin.flow_name'
  , 'timestamp'
  , to_date('timestamp').alias('date')
  , 'bronze_load_datetime'
  , 'event_type'
  , 'details'
  , get_json_object('details', '$.flow_progress').alias('flow_progress')
  , get_json_object('details', '$.flow_progress.status').alias('status') # COMPLETED, RUNNING, QUEUED, STARTING
  , get_json_object('details', '$.flow_progress.metrics').alias('metrics') 
  , get_json_object('details', '$.flow_progress.data_quality').alias('data_quality')


  ## Unknown Status
  , get_json_object('details', '$.flow_progress.metrics.executor_time_ms').alias('executor_time_ms') # ??
  , get_json_object('details', '$.flow_progress.metrics.executor_cpu_time_ms').alias('executor_cpu_time_ms') # ??
  
  ## Completed Details
  , get_json_object('details', '$.flow_progress.metrics.num_output_rows').alias('num_output_rows')
  , get_json_object('details', '$.flow_progress.metrics.num_upserted_rows').alias('num_upserted_rows')
  , get_json_object('details', '$.flow_progress.metrics.num_deleted_rows').alias('num_deleted_rows')
  , get_json_object('details', '$.flow_progress.data_quality.dropped_records').alias('dropped_records')
  
  ## Running Details
  , get_json_object('details', '$.flow_progress.metrics.backlog_bytes').alias('backlog_bytes')
  , get_json_object('details', '$.flow_progress.data_quality.expectations').alias('expectations') ## New table for this list object


]



@dlt.table(name='flow_progress_details', comment="{'quality': 'silver'}")
def flow_progress_details():
  return (dlt.read_stream('raw_event_log')
          .filter(col('event_type') == 'flow_progress')
          .select(detail_cols)
          .withColumn("silver_load_datetime", current_timestamp())
         )

# COMMAND ----------

# DBTITLE 1,Completed Events
completed_events_cols = [
  'id'
  , 'pipeline_id'
  , 'pipeline_name'
  , 'flow_id'
  , 'flow_name'
  , 'timestamp'
  , 'bronze_load_datetime'
  , 'event_type'
  , 'status'
  , 'num_output_rows'
  , 'num_upserted_rows'
  , 'num_deleted_rows'
  , 'dropped_records'
]



@dlt.table(name='completed_flows', comment="{'quality': 'silver'}")
def completed_flows():
  return (dlt.read_stream('flow_progress_details')
          .select(completed_events_cols)
          .withColumn("silver_load_datetime", current_timestamp())
         )

# COMMAND ----------

# DBTITLE 1,Daily Flow Agg
daily_flow_agg_group_cols = [
  'pipeline_id'
  , 'pipeline_name'
  , 'date'
]

daily_flow_agg_select_cols = daily_flow_agg_group_cols + [
  'num_output_rows'
  , 'num_upserted_rows'
  , 'num_deleted_rows'
  , 'dropped_records'
]

daily_flow_agg_sum_cols = [
   sum('num_output_rows').alias('num_output_rows')
  , sum('num_upserted_rows').alias('num_upserted_rows')
  , sum('num_deleted_rows').alias('num_deleted_rows')
  , sum('dropped_records').alias('dropped_records') 
]

@dlt.table(name='daily_flow_agg', comment="{'quality':'gold'}")
def daily_flow_agg():
  return (dlt.read('flow_progress_details')
          .select(daily_flow_agg_select_cols)
          .groupBy(daily_flow_agg_group_cols)
          .agg(*daily_flow_agg_sum_cols)
         )

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Expectations
flow_expectation_cols = [
  'id'
  , 'pipeline_id'
  , 'pipeline_name'
  , 'flow_id'
  , 'flow_name'
  , 'timestamp'
  , date_trunc("hour", 'timestamp').alias('hourly_timestamp')
  , to_date('timestamp').alias('date')
  , 'bronze_load_datetime'
  , 'event_type'
  , 'details'
  , get_json_object('details', '$.flow_progress.data_quality.expectations').alias('expectations')
]

@dlt.table(name='flow_progress_expectations', comment="{'quality': 'silver'}")
def flow_progress_expectations():
  return (dlt.read_stream('flow_progress_details')
          .select(flow_expectation_cols) # select the first set of columns 
          .select(flow_expectation_cols + [explode(from_json(col('expectations'), "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"))] ) # explode out the list 
          .select(flow_expectation_cols + ['col.name', 'col.dataset', 'col.passed_records', 'col.failed_records']) # select the exploded columns + original list 
          .withColumn("silver_load_datetime", current_timestamp()) # add timestamp
          .drop('col', 'details', 'expectations') # drop the unneeded etl columns 
         )

# COMMAND ----------

# DBTITLE 1,Daily Expectation Agg
daily_expectations_agg_group_cols = [
  'pipeline_id'
  , 'pipeline_name'
  , 'date'
  , 'name'
  , 'dataset'
]

daily_expectations_agg_select_cols = daily_expectations_agg_group_cols + [
  'passed_records'
  , 'failed_records'
]

daily_expectations_agg_sum_cols = [
   sum('passed_records').alias('passed_records')
  , sum('failed_records').alias('failed_records')
]

@dlt.table(name='daily_expectations_agg', comment="{'quality':'gold'}")
def daily_expectations_agg():
  return (dlt.read('flow_progress_expectations')
          .select(daily_expectations_agg_select_cols)
          .groupBy(daily_expectations_agg_group_cols)
          .agg(*daily_expectations_agg_sum_cols)
         )

# COMMAND ----------

# DBTITLE 1,Cluster Resources
cluster_resource_cols = [
  'id'
  , 'origin.pipeline_id'
  , 'origin.pipeline_name'
  , 'timestamp'
  , date_trunc("hour", 'timestamp').alias('hourly_timestamp')
  , 'bronze_load_datetime'
  , 'event_type'
  , 'details'
  , get_json_object('details', '$.cluster_resources').alias('cluster_resources')
  , get_json_object('details', '$.cluster_resources.summary_duration_ms').alias('summary_duration_ms') 
  , get_json_object('details', '$.cluster_resources.num_task_slots').alias('num_task_slots') 
  , get_json_object('details', '$.cluster_resources.avg_num_task_slots').alias('avg_num_task_slots') 
  , get_json_object('details', '$.cluster_resources.avg_task_slot_utilization').alias('avg_task_slot_utilization') 
  , get_json_object('details', '$.cluster_resources.num_executors').alias('num_executors') 
  , get_json_object('details', '$.cluster_resources.avg_num_queued_tasks').alias('avg_num_queued_tasks') 
  , get_json_object('details', '$.cluster_resources.state').alias('state') 


]



@dlt.table(name='cluster_resource_details', comment="{'quality': 'silver'}")
def cluster_resource_details():
  return (dlt.read_stream('raw_event_log')
          .filter(col('event_type') == 'cluster_resources')
          .select(cluster_resource_cols)
          .withColumn("silver_load_datetime", current_timestamp())
          .drop('details', 'cluster_resources')
         )

# COMMAND ----------

# DBTITLE 1,Origin Data - there is a better way to do this able - ryan to refactor
origin_cols = [
  'id' 
  , 'timestamp'
  , 'bronze_load_datetime'
  , 'event_type'
  , 'origin'
  , 'origin.cloud'
  , 'origin.region'
  , 'origin.org_id'
  , 'origin.user_id'
  , 'origin.pipeline_id'
  , 'origin.pipeline_name'
  , 'origin.cluster_id'
  , 'origin.update_id'
  , 'origin.maintenance_id'
  , 'origin.table_id'
  , 'origin.table_name'
  , 'origin.flow_id'
  , 'origin.flow_name'
  , 'origin.batch_id'
  , 'origin.uc_resource_id'
]



@dlt.table(name='flow_progress_origin', comment="{'quality': 'silver'}")
def flow_progress_origin():
  return (dlt.read_stream('raw_event_log')
          .filter(col('event_type') == 'flow_progress')
          .select(origin_cols)
          .withColumn("silver_load_datetime", current_timestamp())
         )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reporting Tables

# COMMAND ----------

# DBTITLE 1,Hourly Expectations
hourly_expectations_agg_group_cols = [
  'pipeline_id'
  , 'pipeline_name'
  , 'hourly_timestamp'
  , 'name'
  , 'dataset'
]

hourly_expectations_agg_select_cols = hourly_expectations_agg_group_cols + [
  'passed_records'
  , 'failed_records'
]

hourly_expectations_agg_sum_cols = [
   sum('passed_records').alias('passed_records')
  , sum('failed_records').alias('failed_records')
]

@dlt.table(name='hourly_expectations_agg', comment="{'quality':'gold'}")
def hourly_expectations_agg():
  return (dlt.read('flow_progress_expectations')
          .select(hourly_expectations_agg_select_cols)
          .groupBy(hourly_expectations_agg_group_cols)
          .agg(*hourly_expectations_agg_sum_cols)
         )

# COMMAND ----------

# DBTITLE 1,Cluster Resources
hourly_cluster_resources_agg_group_cols = [
  'pipeline_id'
  , 'pipeline_name'
  , 'hourly_timestamp'
]

hourly_cluster_resources_agg_select_cols = hourly_cluster_resources_agg_group_cols + [
   'summary_duration_ms'
  , 'num_task_slots'
  , 'avg_num_task_slots'
  , 'avg_task_slot_utilization'
  , 'num_executors'
  , 'avg_num_queued_tasks'
]

hourly_cluster_resources_agg_sum_cols = [
   (avg('summary_duration_ms')/1000).alias('avg_summary_duration_seconds')
  , max('num_task_slots').alias('max_task_slots')
  , min('num_task_slots').alias('min_task_slots')
  , median('avg_num_task_slots').alias('median_avg_num_task_slots')
  , median('avg_task_slot_utilization').alias('median_avg_task_slot_utilization')
  , max('num_executors').alias('max_num_executors')
  , min('num_executors').alias('min_num_executors')
  , avg('num_executors').alias('avg_num_executors')
  , median('num_executors').alias('median_num_executors')
  , median('avg_num_queued_tasks').alias('median_avg_num_queued_tasks')
]

@dlt.table(name='hourly_cluster_resource_agg', comment="{'quality':'gold'}")
def hourly_cluster_resource_agg():
  return (dlt.read('cluster_resource_details')
          .select(hourly_cluster_resources_agg_select_cols)
          .groupBy(hourly_cluster_resources_agg_group_cols)
          .agg(*hourly_cluster_resources_agg_sum_cols)
         )
