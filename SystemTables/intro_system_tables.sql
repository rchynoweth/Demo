-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Introduction to Databricks System Tables 
-- MAGIC
-- MAGIC System Tables are a Databricks-hosted analytical store for operational and usage data. 
-- MAGIC
-- MAGIC System Tables can be used for monitoring, analyzing performance, usage, and behavior of these components within the Databricks platform. By querying these tables, users can gain insights into how their jobs, notebooks, clusters, ML endpoints, and SQL warehouses are functioning and changing over time. This historical data can be used to optimize performance, troubleshoot issues, track usage patterns, and make data-driven decisions.
-- MAGIC
-- MAGIC Overall, System Tables provide a means to enhance observability and gain valuable insights into the operational aspects of Databricks usage, enabling users to better understand and manage their workflows and resources.
-- MAGIC - Cost and usage analytics 
-- MAGIC - Efficiency analytics 
-- MAGIC - Audit analytics 
-- MAGIC - SLO analytics 
-- MAGIC - Data Quality analytics 
-- MAGIC
-- MAGIC ## Datasets 
-- MAGIC
-- MAGIC System Tables are available to customers who have Unity Catalog activated in at least one workspace. The data provided is collected from all workspaces in a Databricks account, regardless of the workspace's status with Unity Catalog. For example, if I have 10 workspaces and only one of them have Unity Catalog enabled then data is collected for all the workspaces and is made available via the workspace in which Unity Catalog is active. 
-- MAGIC
-- MAGIC Today we will provide an overview of the `system.billing.usage` and `system.access.audit` tables with sample visuals. 
-- MAGIC
-- MAGIC The `audit` table collects various information regarding Databricks objects, users, and actions. While the `usage` table tracks Databricks costs and usage (DBUs) for all compute types available in Databricks. So for example, if your question is related to "monthly active users over time" then the `audit_logs` will be the best data source for that, while if you want to track costs by tag or workspace then leverage the `usage` table. The `usage` table tracks usage on a workspace, compute_id, compute, size, job, and SKU level. Additional tags are made available via a `tags` column which includes custom tags that are created by the customer. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC INTERNAL NOTE - BILLING LOGS SCHEMA IS CHANGING WEEK OF 6/19. SCHEMA BELOW IS THE NEW SCHEMA BUT QUERIES ARE ON THE OLD ONES
-- MAGIC
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

-- using these defaults for view creation
use catalog main;
create schema if not exists ryan_chynoweth;
use schema ryan_chynoweth;

-- COMMAND ----------

select * from system.operational_data.billing_logs

-- COMMAND ----------

select account_id
 , workspace_id
 , created_at
 , created_on
 , date_format(created_at, "yyyy-MM") as YearMonth
 , tags.Creator
 , compute_id
 , tags.ClusterName
 , compute_size
 , sku
 , dbus
 , machine_hours 
 , tags.SqlEndpointId
 , tags.ResourceClass
 , tags.JobId
 , tags.RunName
 , tags.ModelName
 , tags.ModelVersion
 , tags.ModelStage

from system.operational_data.billing_logs
where created_on >= '2022-01-01' 
      and tags.JobId is not Null 

-- COMMAND ----------

-- DBTITLE 1,Interactive Jobs
select account_id
 , workspace_id
 , created_at
 , created_on
 , date_format(created_at, "yyyy-MM") as YearMonth
 , tags.Creator
 , compute_id
 , tags.ClusterName
 , compute_size
 , sku
 , dbus
 , machine_hours 
 , tags.SqlEndpointId
 , tags.ResourceClass
 , tags.JobId
 , tags.RunName
 , tags.ModelName
 , tags.ModelVersion
 , tags.ModelStage

from system.operational_data.billing_logs
where created_on >= '2022-01-01' 
      and tags.JobId is not Null and contains(sku, 'JOBS') == FALSE -- want to find interactive jobs 

-- COMMAND ----------

-- let's look at the number of models we have running by stage
select account_id
 , workspace_id
 , created_at
 , created_on
 , date_format(created_at, "yyyy-MM") as YearMonth
 , tags.Creator
 , compute_id
 , tags.ClusterName
 , compute_size
 , sku
 , dbus
 , machine_hours 
 , tags.SqlEndpointId
 , tags.ResourceClass
 , tags.JobId
 , tags.RunName
 , tags.ModelName
 , tags.ModelVersion
 , case when tags.ModelStage is null then 'Other' else tags.ModelStage end as ModelStage

from system.operational_data.billing_logs
where created_on >= '2022-01-01' and contains(sku, 'INFERENCE')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Audit Logs 
-- MAGIC
-- MAGIC The `audit_logs` table has the following columns: 
-- MAGIC - version
-- MAGIC - created_at
-- MAGIC - created_on
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

select distinct action_name from system.operational_data.audit_logs

-- COMMAND ----------

-- create 
-- createShare
-- createExternalLocation
-- deleteSchema
-- editWarehouse, startEndpoint, createEndpoint, 
-- runFailed, runSucceeded
-- deleteWarehouse
-- commandFinish
-- favoriteDashboard

-- COMMAND ----------

create or replace view system_job_runs 
as
select created_at
, created_on
, date_format(created_on, 'yyyy-MM') AS year_month
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
from system.operational_data.audit_logs 
where action_name in ('runFailed', 'runSucceeded', 'runNow', 'runStart', 'runTriggered');

select * from system_job_runs where action_name in ('runFailed', 'runSucceeded', 'runTriggered') and jobTerminalState is not null

-- COMMAND ----------

create or replace view system_external_locations 
as
select created_at
, created_on
, date_format(created_on, 'yyyy-MM') AS year_month
, user_identity.email as user_email
, service_name
, action_name
, request_id
, request_params.name as external_location_name
, request_params.skip_validation
, request_params.credential_name
, request_params.url
, request_params.max_results
, request_params.workspace_id
, request_params.metastore_id
, response.statusCode
, request_params.include_browse
, request_params.name_arg

from system.operational_data.audit_logs

where contains(action_name, 'ExternalLocation') ; 

-- who is creating external locations
select * 
from system_external_locations 
where action_name = 'createExternalLocation' 
order by created_on desc

-- COMMAND ----------

-- most used external locations
select * 
from system_external_locations 
where action_name = 'getExternalLocation'
order by created_on desc

-- COMMAND ----------

-- Daily and Monthly Active Users 
select distinct
created_on as `Date`
, date_format(created_on, 'yyyy-MM') AS year_month
, workspace_id
, user_identity.email as user_email

from system.operational_data.audit_logs


-- COMMAND ----------


