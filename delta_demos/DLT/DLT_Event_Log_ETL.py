# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Table ETL - Event Logs

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

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

# DBTITLE 1,Flow Events
detail_cols = [
  'id'
  , 'sequence.data_plane_id.seq_no' #pk2
  , 'origin.pipeline_id'
  , 'origin.pipeline_name'
  , 'origin.flow_id'
  , 'origin.flow_name'
  , 'timestamp'
  , 'bronze_load_datetime'
  , 'event_type'
  , 'details'
  , get_json_object('details', '$.flow_progress').alias('flow_progress')
  , get_json_object('details', '$.flow_progress.status').alias('status')
  , get_json_object('details', '$.flow_progress.metrics').alias('metrics')
  , get_json_object('details', '$.flow_progress.metrics.backlog_bytes').alias('backlog_bytes')
  , get_json_object('details', '$.flow_progress.metrics.num_output_rows').alias('num_output_rows')
  , get_json_object('details', '$.flow_progress.data_quality').alias('data_quality')
  , get_json_object('details', '$.flow_progress.data_quality.dropped_records').alias('dropped_records')
  , get_json_object('details', '$.flow_progress.metrics.executor_time_ms').alias('executor_time_ms')
  , get_json_object('details', '$.flow_progress.metrics.executor_cpu_time_ms').alias('executor_cpu_time_ms')

]



@dlt.table(name='flow_progress_details', comment="{'quality': 'silver'}")
def flow_progress_details():
  return (dlt.read_stream('raw_event_log')
          .filter(col('event_type') == 'flow_progress')
          .select(detail_cols)
          .withColumn("silver_load_datetime", current_timestamp())
         )

# COMMAND ----------

# DBTITLE 1,Origin Data
origin_cols = [
  'id' # pk1
  , 'sequence.data_plane_id.seq_no' #pk2
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
