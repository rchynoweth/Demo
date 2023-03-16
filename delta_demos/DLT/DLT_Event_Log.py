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
# MAGIC Data Notes  
# MAGIC   * The `details` column contains metadata about each Event sent to the Event Log. There are different fields depending on what type of Event it is. Some examples include:
# MAGIC   * `user_action` Events occur when taking actions like creating the pipeline
# MAGIC   * `flow_definition` Events occur when a pipeline is deployed or updated and have lineage, schema, and execution plan information
# MAGIC     * `output_dataset` and `input_datasets` - output table/view and its upstream table(s)/view(s)
# MAGIC     * `flow_type` - whether this is a complete or append flow
# MAGIC     * `explain_text` - the Spark explain plan
# MAGIC   * `flow_progress` Events occur when a data flow starts running or finishes processing a batch of data
# MAGIC     * `metrics` - currently contains `num_output_rows`
# MAGIC     * `data_quality` - contains an array of the results of the data quality rules for this particular dataset
# MAGIC       * `dropped_records`
# MAGIC       * `expectations`
# MAGIC         * `name`, `dataset`, `passed_records`, `failed_records`
# MAGIC 
# MAGIC 
# MAGIC Public Resources: 
# MAGIC - https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-event-log.html#data-quality

# COMMAND ----------

dbutils.widgets.text("schema_name", "")
schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------

spark.sql(f"use {schema_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_event_log

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from user_action_details

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   row_expectations.dataset as dataset,
# MAGIC   row_expectations.name as expectation,
# MAGIC   SUM(row_expectations.passed_records) as passing_records,
# MAGIC   SUM(row_expectations.failed_records) as failing_records
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       explode(
# MAGIC         from_json(
# MAGIC           details :flow_progress :data_quality :expectations,
# MAGIC           "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
# MAGIC         )
# MAGIC       ) row_expectations
# MAGIC     FROM
# MAGIC       event_log_raw
# MAGIC     WHERE
# MAGIC       event_type = 'flow_progress'
# MAGIC       AND origin.update_id = '${latest_update.id}'
# MAGIC   )
# MAGIC GROUP BY
# MAGIC   row_expectations.dataset,
# MAGIC   row_expectations.name

# COMMAND ----------

spark.sql('use rac_demo_db')

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct * from flow_progress_expectations

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cluster_resource_details
