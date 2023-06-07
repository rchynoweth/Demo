# Databricks notebook source
# MAGIC %md
# MAGIC # Run DBU Forecasts
# MAGIC
# MAGIC This notebook sources data from Databricks System Tables (`system.operational_data.billing_logs`). Generates Prophet forecasts by SKU and Workspace. 
# MAGIC
# MAGIC SKUs are aggregated to the top level categories: All Purpose, DLT, Jobs, Model Inference, and SQL. 
# MAGIC
# MAGIC This notebook generates and evaluates forecasts. Data is saved to the following tables: 
# MAGIC 1. `input_dbus_by_date_sku_workspace`  
# MAGIC 1. `output_dbu_forecasts_by_date_sku_workspace`  
# MAGIC 1. `dbu_forecast_evals_by_date_sku_workspace`  

# COMMAND ----------

from pyspark.sql.functions import *
from libs.dbu_prophet_forecast import DBUProphetForecast

# COMMAND ----------

dpf = DBUProphetForecast(forecast_periods=28)

# COMMAND ----------

dbutils.widgets.text('TargetCatalog', '')
dbutils.widgets.text('TargetSchema', '')
target_catalog = dbutils.widgets.get('TargetCatalog')
target_schema = dbutils.widgets.get('TargetSchema')

# COMMAND ----------

spark.sql(f"create catalog if not exists {target_catalog}")
spark.sql(f"create schema if not exists {target_catalog}.{target_schema}")
spark.sql(f'use catalog {target_catalog}')
spark.sql(f'use schema {target_schema}')

# COMMAND ----------

### 
## applyInPandas functions require us to declare with a wrapper function here
## Note: look for a way to avoid this
### 
def generate_forecast_udf(history_pd):
    return dpf.generate_forecast( history_pd )
  
def evaluate_forecast_udf(evaluation_pd):
    return dpf.evaluate_forecast( evaluation_pd )


# COMMAND ----------

# DBTITLE 1,Read data from System table
df = dpf.load_data(spark=spark).filter(col('workspace_id') == '6051921418418893')

# COMMAND ----------

# DBTITLE 1,Group and save dataframe as table 
input_df = dpf.transform_consolidated_sku_data(df)
(
  input_df
  .write
  .option("mergeSchema", "true")
  .mode('overwrite')
  .saveAsTable(f"{target_catalog}.{target_schema}.input_dbus_by_date_sku_workspace")
)

# COMMAND ----------

# DBTITLE 1,Generate Forecasts
results = (
  input_df
    .groupBy('workspace_id','sku')
    .applyInPandas(generate_forecast_udf, schema=dpf.forecast_result_schema)
    .withColumn('training_date', current_date() )
    )

# COMMAND ----------

# DBTITLE 1,Save Results
(
  results.write
  .option("mergeSchema", "true")
  .mode('overwrite')
  .saveAsTable(f"{target_catalog}.{target_schema}.output_dbu_forecasts_by_date_sku_workspace")
)

# COMMAND ----------

# DBTITLE 1,Evaluate Forecasts
# calculate metrics
results = (
  spark.read
    .table(f'{target_catalog}.{target_schema}.output_dbu_forecasts_by_date_sku_workspace')
    .select('training_date', 'workspace_id', 'sku', 'y', 'yhat')
    .groupBy('training_date', 'workspace_id', 'sku')
    .applyInPandas(evaluate_forecast_udf, schema=dpf.eval_schema)
    )


# COMMAND ----------

# DBTITLE 1,Save Evaluation Results
(
  results.write
  .option("mergeSchema", "true")
  .mode('overwrite')
  .saveAsTable(f"{target_catalog}.{target_schema}.dbu_forecast_evals_by_date_sku_workspace")
)

# COMMAND ----------

# MAGIC %md
# MAGIC The queries below can be used to visualize forecast outputs

# COMMAND ----------

spark.sql(f"""
    create or replace view {target_catalog}.{target_schema}.vw_dbu_forecasts
    as 
    select 
    ds as date
    , workspace_id
    , sku
    , y as dbus
    , case when sku='ALL_PURPOSE' and y is not null then 0.55*y  
      when sku='DLT' and y is not null  then 0.36*y
      when sku='JOBS' and y is not null then 0.30*y
      when sku='SQL' and y is not null then 0.22*y 
      when sku='MODEL_INFERENCE' and y is not null then 0.079*y       
      else NULL end as ApproxListCost
    
    , case when sku='ALL_PURPOSE' and y is null then 0.55*yhat  
      when sku='DLT' and y is null  then 0.36*yhat
      when sku='JOBS' and y is null then 0.30*yhat
      when sku='SQL' and y is null then 0.22*yhat 
      when sku='MODEL_INFERENCE' and y is null then 0.079*yhat       
      else NULL end as ApproxForecastListCost

    , case when yhat < 0 then 0 else yhat end as dbus_predicted
    , case when yhat_upper < 0 then 0 else yhat_upper end as dbus_predicted_upper
    , case when yhat_lower < 0 then 0 else yhat_lower end as dbus_predicted_lower
    , case when y > yhat_upper then TRUE else FALSE end as upper_anomaly_alert
    , case when y < yhat_lower then TRUE else FALSE end as lower_anomaly_alert
    , case when y >= yhat_lower AND y <= yhat_upper THEN true ELSE false END AS on_trend
    , training_date
    
    from {target_catalog}.{target_schema}.output_dbu_forecasts_by_date_sku_workspace

""")

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
