# Databricks notebook source
# MAGIC %md
# MAGIC # View DBU Forecasts
# MAGIC
# MAGIC This notebook sources data from Databricks System Tables (`system.operational_data.billing_logs`). Generates Prophet forecasts by SKU and Workspace. 

# COMMAND ----------

# DBTITLE 1,Import libs
from pyspark.sql.functions import *
from libs.ddl_helper import DDLHelper

# COMMAND ----------

# DBTITLE 1,Parameters
# target catalog/schema are the data source tables to create the view
dbutils.widgets.text('TargetCatalog', '')
dbutils.widgets.text('TargetSchema', '')
target_catalog = dbutils.widgets.get('TargetCatalog')
target_schema = dbutils.widgets.get('TargetSchema')

# COMMAND ----------

# DBTITLE 1,Use Data Objects
spark.sql(f'use catalog {target_catalog}')
spark.sql(f'use schema {target_schema}')

# COMMAND ----------

# DBTITLE 1,Use the DDL Helper for Reporting ETL
## DDL Helper is used to create a cost lookup table
# create obj
ddl_help = DDLHelper(spark=spark)
# create table DDL
ddl_help.create_cost_lookup_table(target_catalog=target_catalog, target_schema=target_schema)
# insert overwrite into tale
ddl_help.insert_into_cost_lookup_table(target_catalog=target_catalog, target_schema=target_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Granular Forecasts
# MAGIC
# MAGIC SKUs are not aggregated and we produce forecasts for all SKU types i.e. `STANDARD_ALL_PURPOSE_COMPUTE`, `PREMIUM_ALL_PURPOSE_COMPUTE`
# MAGIC
# MAGIC
# MAGIC The following tables are used for granular forecasting. 
# MAGIC 1. `input_dbus_by_date_system_sku_workspace`  
# MAGIC 1. `output_dbu_forecasts_by_date_system_sku_workspace`  
# MAGIC 1. `dbu_forecast_evals_by_date_system_sku_workspace`  
# MAGIC 1. `vw_dbu_granular_forecasts`   

# COMMAND ----------

# DBTITLE 1,Create granular view for visuals
# create view for reporting visuals
ddl_help.create_granular_forecast_view(target_catalog=target_catalog, target_schema=target_schema)

# COMMAND ----------

# DBTITLE 1,All DBUs
display(spark.read.table(f'{target_catalog}.{target_schema}.vw_dbu_granular_forecasts') )

# COMMAND ----------

# DBTITLE 1,All Purpose DBUs
display(spark.read.table(f'{target_catalog}.{target_schema}.vw_dbu_granular_forecasts').filter(col('sku').contains('ALL_PURPOSE')) )

# COMMAND ----------

# DBTITLE 1,Jobs DBUs
display(spark.read.table(f'{target_catalog}.{target_schema}.vw_dbu_granular_forecasts').filter(col('sku').contains('JOBS')) )

# COMMAND ----------

# DBTITLE 1,DLT DBUs
display(spark.read.table(f'{target_catalog}.{target_schema}.vw_dbu_granular_forecasts').filter(col('sku').contains('DLT')) )

# COMMAND ----------

# DBTITLE 1,SQL DBUs
display(spark.read.table(f'{target_catalog}.{target_schema}.vw_dbu_granular_forecasts').filter(col('sku').contains('SQL')) )

# COMMAND ----------

# DBTITLE 1,Model Inference DBUs
display(spark.read.table(f'{target_catalog}.{target_schema}.vw_dbu_granular_forecasts').filter(col('sku').contains('INFERENCE')) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rollup DBU Forecasts
# MAGIC
# MAGIC This notebook sources data from Databricks System Tables (`system.operational_data.billing_logs`). Generates Prophet forecasts by Consolidated SKU and Workspace. 
# MAGIC
# MAGIC SKUs are aggregated to the top level categories: `ALL_PURPOSE`, `DLT`, `JOBS`, `MODEL_INFERENCE`, and `SQL`. 
# MAGIC
# MAGIC This notebook generates and evaluates forecasts. Data is saved to the following tables: 
# MAGIC 1. `input_dbus_by_date_system_sku_workspace`  
# MAGIC 1. `output_dbu_forecasts_by_date_system_sku_workspace`  
# MAGIC 1. `dbu_forecast_evals_by_date_system_sku_workspace`  
# MAGIC 1. `vw_dbu_granular_forecasts`   

# COMMAND ----------

# DBTITLE 1,Create Consolidated Forecast View
# create view for reporting visuals
ddl_help.create_consolidated_forecast_view(target_catalog=target_catalog, target_schema=target_schema)

# COMMAND ----------

# DBTITLE 1,All DBUs
display(spark.read.table(f'{target_catalog}.{target_schema}.vw_dbu_forecasts') )

# COMMAND ----------

# DBTITLE 1,All Purpose DBUs
display(spark.read.table(f'{target_catalog}.{target_schema}.vw_dbu_forecasts').filter(col('sku')=='ALL_PURPOSE') )

# COMMAND ----------

# DBTITLE 1,Jobs DBUs
display(spark.read.table(f'{target_catalog}.{target_schema}.vw_dbu_forecasts').filter(col('sku')=='JOBS') )

# COMMAND ----------

# DBTITLE 1,DLT DBUs
display(spark.read.table(f'{target_catalog}.{target_schema}.vw_dbu_forecasts').filter(col('sku')=='DLT') )

# COMMAND ----------

# DBTITLE 1,SQL DBUs
display(spark.read.table(f'{target_catalog}.{target_schema}.vw_dbu_forecasts').filter(col('sku')=='SQL') )

# COMMAND ----------

# DBTITLE 1,Model Inference DBUs
display(spark.read.table(f'{target_catalog}.{target_schema}.vw_dbu_forecasts').filter(col('sku')=='MODEL_INFERENCE') )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anomaly Alert Creation
# MAGIC
# MAGIC Using Databricks SQL Alerts, users are able to create a system that will automatically detect when actual usage goes out of the expected bounds. In our scenario we will be alerted if the usage goes above or below the lower and upper bounds of the forecast. In our forecasting algorithm we generate the following columns:  
# MAGIC - `upper_anomaly_alert`: True if actual amount is greater than the upper bound. 
# MAGIC - `lower_anomaly_alert`: True if actual amount is less than the lower bound. 
# MAGIC - `on_trend`: True if `upper_anomaly_alert` or `lower_anomaly_alert` is True. 
# MAGIC
# MAGIC Please see the output forecast output dataset below. 

# COMMAND ----------

display(spark.sql(f'select * from {target_catalog}.{target_schema}.vw_dbu_granular_forecasts'))

# COMMAND ----------


