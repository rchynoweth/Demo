-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Introduction to Databricks System Tables 
-- MAGIC
-- MAGIC System Tables are a Databricks-hosted analytical store for operational and usage data. 
-- MAGIC
-- MAGIC System Tables can be used for monitoring, analyzing performance, usage, and behavior of Databricks Platform components. By querying these tables, users can gain insights into how their jobs, notebooks, users, clusters, ML endpoints, and SQL warehouses are functioning and changing over time. This historical data can be used to optimize performance, troubleshoot issues, track usage patterns, and make data-driven decisions.
-- MAGIC
-- MAGIC Overall, System Tables provide a means to enhance observability and gain valuable insights into the operational aspects of Databricks usage, enabling users to better understand and manage their workflows and resources.
-- MAGIC - Cost and usage analytics 
-- MAGIC - Efficiency analytics 
-- MAGIC - Audit analytics 
-- MAGIC - Service Level Objective analytics 
-- MAGIC - Data Quality analytics 
-- MAGIC
-- MAGIC ## Datasets 
-- MAGIC
-- MAGIC System Tables are available to customers who have Unity Catalog activated in at least one workspace. The data provided is collected from all workspaces in a Databricks account, regardless of the workspace's status with Unity Catalog. For example, if I have 10 workspaces and only one of them have Unity Catalog enabled then data is collected for all the workspaces and is made available via the single workspace in which Unity Catalog is active. 
-- MAGIC
-- MAGIC We will provide an overview of the `system.billing.usage` and `system.access.audit` tables with sample visuals, however, there are other tables available such as: `system.access.column_lineage` and `system.access.table_lineage`
-- MAGIC
-- MAGIC The `audit` table collects various information regarding Databricks objects, users, and actions. While the `usage` table tracks Databricks costs and usage (DBUs) for all compute types available in Databricks. So for example, if your question is related to "monthly active users over time" then the `audit_logs` will be the best data source for that, while if you want to track costs by tag or workspace then leverage the `usage` table. The `usage` table tracks usage on a workspace, compute_id, compute, size, job, and SKU level. Additional tags are made available via a `custom_tags` column which includes tags that are created by the customer. 
-- MAGIC
-- MAGIC ## Dashboard
-- MAGIC
-- MAGIC There is a sample [dashboard](https://e2-dogfood.staging.cloud.databricks.com/sql/dashboards/cae6779e-ca25-4ae2-8c68-c140877111fd?o=6051921418418893) available that leverages the queries covered in this notebook. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Billing Logs
-- MAGIC
-- MAGIC In public prevew the `billing_logs` table schema and associated catalog schema is changing. The new table will be available at `system.billing.usage` and has the following columns: 
-- MAGIC - `usage_record_id`: unique id for the record
-- MAGIC - `insertion_time`: the time the record was added to the table
-- MAGIC - `account_id`: ID of the Databricks account or Azure Subscription ID
-- MAGIC - `workspace_id`: ID of the workspace this usage was associated with
-- MAGIC - `sku_id`: ID of the sku
-- MAGIC - `sku_name`: name of the sku
-- MAGIC - `cloud`: cloud this usage is associated to 
-- MAGIC - `usage_start_time`: start time of usage record
-- MAGIC - `usage_end_time`: end time of usage record 
-- MAGIC - `usage_date`: date of usage record
-- MAGIC - `custom_tags`: tag metadata associated to the usage 
-- MAGIC - `usage_unit`: Unit this usage is measure (i.e. DBUs)
-- MAGIC - `usage_quantity`: number of units consumed
-- MAGIC - `default_tags`: other relevant information about the usage  
-- MAGIC - `corrects_record`: the previous record ID if this record is a correction related to a prior usage record

-- COMMAND ----------

select * from system.billing.usage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Jobs Usage 
-- MAGIC
-- MAGIC Jobs are scheduled code and have extremely predictable usage over time. Since jobs are automated it is important to monitor which jobs are put into production to avoid unnecessary spend. Let's take a look at job spend over time. 

-- COMMAND ----------

select account_id
, workspace_id
, usage_record_id
, sku_name
, cloud 
, usage_start_time 
, usage_end_time
, usage_date
, date_format(usage_date, 'yyyy-MM') as YearMonth
, usage_unit
, usage_quantity
, list_price
, list_price*usage_quantity as list_cost
, custom_tags.Team -- parse out custom tags if available 
, system_metadata.cluster_id
, system_metadata.job_id
, system_metadata.sql_endpoint_id 
, system_metadata.instance_pool_id
, system_metadata.node_type

from system.billing.usage
left join main.default.sku_cost_lookup on sku_name = sku

where system_metadata.job_id is not Null 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Interactive Jobs 
-- MAGIC
-- MAGIC Interactive (All Purpose) compute are clusters meant to be used during the development process. Once a solution is developed it is considered a best practice to move them to job clusters. We will want to keep an eye on how many jobs are created on all purpose and alert the users when that happens to make the change. 

-- COMMAND ----------

-- DBTITLE 0,Interactive Jobs
with created_jobs as (
  select 
    workspace_id
    , event_time as created_time
    , user_identity.email as creator
    , request_id
    , event_id
    , get_json_object(response.result, '$.job_id') as job_id
    , request_params.name as job_name
    , request_params.job_type 
    , request_params.schedule

    from system.access.audit 

    where action_name = 'create' 
    and service_name = 'jobs'
    and response.status_code = 200




), 
deleted_jobs as (

  select request_params.job_id, workspace_id
  from system.access.audit 

  where action_name = 'delete' 
  and service_name = 'jobs'
  and response.status_code = 200


)

select a.workspace_id
, a.sku_name
, a.cloud 
, a.usage_date
, date_format(usage_date, 'yyyy-MM') as YearMonth
, a.usage_unit
, d.list_price
, sum(a.usage_quantity) total_dbus
, sum(a.usage_quantity)*d.list_price as list_cost
, a.system_metadata.cluster_id
, a.system_metadata.job_id
, a.system_metadata.sql_endpoint_id 
, a.system_metadata.instance_pool_id
, a.system_metadata.node_type
, case when b.job_id is not null then TRUE else FALSE end as job_created_flag
, case when c.job_id is not null then TRUE else FALSE end as job_deleted_flag

from system.billing.usage a
  left join created_jobs b on a.workspace_id = b.workspace_id and a.system_metadata.job_id = b.job_id
  left join deleted_jobs c on a.workspace_id = c.workspace_id and a.system_metadata.job_id = c.job_id
  left join main.default.sku_cost_lookup d on sku_name = sku


where system_metadata.job_id is not Null 
and contains(sku_name, 'ALL_PURPOSE')

group by all

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Model Inference Usage 
-- MAGIC
-- MAGIC Databricks has the ability to host and deploy serverless model endpoints for highly available and cost effective REST APIs. Endpoints can scale all the way down to zero and quickly come up to provide a response to the end user optimizing experience and spend. Let's keep and eye on how many models we have deployed the the usage of those models. 

-- COMMAND ----------

select account_id
, workspace_id
, usage_record_id
, sku_name
, cloud 
, usage_start_time 
, usage_end_time
, usage_date
, date_format(usage_date, 'yyyy-MM') as YearMonth
, usage_unit
, usage_quantity
, list_price
, list_price*usage_quantity as list_cost
, custom_tags.Team -- parse out custom tags if available 
, system_metadata
, system_metadata.cluster_id
, system_metadata.job_id
, system_metadata.sql_endpoint_id 
, system_metadata.instance_pool_id
, system_metadata.node_type

from system.billing.usage
left join main.default.sku_cost_lookup on sku_name = sku

where contains(sku_name, 'INFERENCE')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Audit Logs 
-- MAGIC
-- MAGIC The `audit_logs` table has the following columns: 
-- MAGIC - version
-- MAGIC - event_time
-- MAGIC - event_date
-- MAGIC - event_id
-- MAGIC - workspace_id
-- MAGIC - source_ip_address
-- MAGIC - user_agent
-- MAGIC - session_id
-- MAGIC - user_identity
-- MAGIC - service_name 
-- MAGIC - action_name
-- MAGIC - request_id
-- MAGIC - request_params
-- MAGIC - response
-- MAGIC - audit_level
-- MAGIC - account_id
-- MAGIC
-- MAGIC Audit logs give you the ability to see what users are doing within Databricks. They allow you to see the actions users have taken, such as editing or triggering a job and when it occured. 

-- COMMAND ----------

select * from system.access.audit

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Completed Jobs
-- MAGIC
-- MAGIC It is important that job success rate is high. We will want to keep an eye on how each job run is doing and if we see an uptick in failures/errors then we can receive automated alerts of unusual behaviour across jobs instead of single job notifications. 

-- COMMAND ----------

-- job runs by state over time
select event_time
, event_date
, date_format(event_date, 'yyyy-MM') AS year_month
, workspace_id
, service_name
, action_name
, request_id
, account_id
, request_params.runCreatorUserName
, request_params.warehouseId
, request_params.alertId
, request_params.jobId
, request_params.runId
, request_params.runTaskType
, request_params.idInJob
, request_params.jobTerminalState
, request_params.taskKey
, request_params.jobTriggerType
from system.access.audit  
where action_name in ('runFailed', 'runSucceeded');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### External Locations 
-- MAGIC External locations are a common way to load data from cloud object storage into Databricks tables. They can also be used for writing data out of Databricks for external processes. We want to ensure that data is being used properly and is not exfiltrating outside our doman. Let's see how many external locations are being created over time and which ones are the most used. 

-- COMMAND ----------

-- created external locations 
-- monitor possible data exfilltration 
select event_time
, event_date
, date_format(event_date, 'yyyy-MM') AS year_month
, user_identity.email as user_email
, service_name
, action_name
, request_id
, request_params.name as external_location_name
, request_params.skip_validation
, request_params.credential_name
, request_params.url
, request_params.workspace_id
, request_params.metastore_id
, response.status_code


from system.access.audit 


where action_name = 'createExternalLocation' 


-- COMMAND ----------

-- most used external locations

select event_time
, event_date
, date_format(event_date, 'yyyy-MM') AS year_month
, user_identity.email as user_email
, service_name
, action_name
, request_id
, request_params
, request_params.name_arg as external_location_name
, request_params.workspace_id
, request_params.metastore_id
, response.status_code


from system.access.audit 


where action_name = 'getExternalLocation' 


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Users 
-- MAGIC
-- MAGIC An important KPI for any platform team administering Databricks is the number of users that they are supporting. This allows admins to quantify the impact is developer productivity and importance to the organization.  

-- COMMAND ----------

-- Daily and Monthly Active Users 
select distinct
event_date as `Date`
, date_format(event_date, 'yyyy-MM') AS year_month
, workspace_id
, user_identity.email as user_email

from system.access.audit 

-- COMMAND ----------


