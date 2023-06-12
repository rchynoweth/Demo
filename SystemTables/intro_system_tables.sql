-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Introduction to Databricks System Tables 
-- MAGIC
-- MAGIC System Tables are a Databricks-hosted analytical store for all of Databricks operational data used for historical customer observaibility and insights for jobs, notebooks, clusters, ML Endpoints, and SQL Warehouses. 
-- MAGIC
-- MAGIC System Tables can be used for monitoring, analyzing the performance, usage, and behavior of these components within the Databricks platform. By querying these tables, users can gain insights into how their jobs, notebooks, clusters, ML endpoints, and SQL warehouses are functioning over time. This historical data can be used to optimize performance, troubleshoot issues, track usage patterns, and make data-driven decisions.
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
-- MAGIC There are currently two tables available and are stored in the `operational_data` schema within the `system` catalog. The `audit_logs` data collects various information regarding Databricks objects, users, and actions. The `billing_logs` table tracks Databricks costs and usage (DBUs) for all compute types available in Databricks. So for example, if your question is related to "monthly active users over time" then the `audit_logs` will be the best data source for that, while if you want to track costs by tag or workspace then leverage the `billing_logs`. The `billing_logs` table tracks usage on a workspace, compute_id, compute, size, job, and SKU level. Additional tags are made available via a `tags` column which includes custom tags that are created by the customer. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Billing Logs
-- MAGIC
-- MAGIC The `billing_logs` table has the following columns: 
-- MAGIC - account_id
-- MAGIC - workspace_id
-- MAGIC - created_at
-- MAGIC - created_on
-- MAGIC - compute_id
-- MAGIC - compute_size
-- MAGIC - sku
-- MAGIC - dbus
-- MAGIC - machine_hours
-- MAGIC - tags 
-- MAGIC
-- MAGIC In order to obtain cost data users will need to multiply the `dbus` column by the appropriate price point SKU. As part of this notebook we have a table available at the end of the notebook which shows list pricing for various SKUs. 
-- MAGIC
-- MAGIC The tags column will include custom tags that users have added to particular compute including system tags such as ClusterID and ClusterName. 

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

select * from system.operational_data.audit_logs

-- COMMAND ----------


