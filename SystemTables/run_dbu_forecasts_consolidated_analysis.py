# Databricks notebook source
# MAGIC %md
# MAGIC # Run Consolidated DBU Forecasts Analysis
# MAGIC
# MAGIC The goal of this notebook is to TEST the performance of our forecast models. In order to do so, we will complete the following: 
# MAGIC 1. Produce and evaluate **two** (2) 28-day horizon forecasts.  
# MAGIC 1. Produce and evaulate **four** (4) 7-day horizon forecasts. 
# MAGIC
# MAGIC To analyze the results we will compare the following metrics: 
# MAGIC 1. **Mean Average Error** (`MAE`): comparing `y` vs `yhat`  
# MAGIC 1. **Mean Squared Error** (`MSE`): comparing `y` vs `yhat` 
# MAGIC 1. **Root Mean Squared Error** (`RMSE`): comparing `y` vs `yhat`   
# MAGIC 1. **Trend Accuracy**: trend accuracy will be a custom metric to evaluate our ability to forecast within the expected trend and gives a boolean variable to analyze. 
# MAGIC     - We will use this metric to evaluate **overall** model accuracy as well as our accuracy on the ability to forecast trend by days further from our max training data. 
# MAGIC         - For example, if the max date in our training set is `2023-05-31` we expect that the trend accuracy will will be higher for `2023-06-01` than for `2023-06-21`.  
# MAGIC     - We will calculate `TRUE` when: `yhat_lower <= y <= yhat_upper`  
# MAGIC     - We will calculate `FALSE` when: `yhat_lower >= y || y >= yhat_upper`  
# MAGIC
# MAGIC
# MAGIC This notebook sources data from Databricks System Tables (`system.operational_data.billing_logs`). Generates Prophet forecasts by SKU and Workspace. 
# MAGIC
# MAGIC SKUs are aggregated to the top level categories: `ALL_PURPOSE`, `DLT`, `JOBS`, `MODEL_INFERENCE`, and `SQL`. 
# MAGIC
# MAGIC
# MAGIC
# MAGIC This notebook generates and evaluates forecasts. Data is saved to the following tables: 
# MAGIC 1. `input_dbus_by_date_sku_workspace`  
# MAGIC 1. `output_dbu_forecasts_by_date_sku_workspace`  
# MAGIC 1. `dbu_forecast_evals_by_date_sku_workspace`  

# COMMAND ----------

# DBTITLE 1,Import libs
from pyspark.sql.functions import *
from libs.dbu_prophet_forecast import DBUProphetForecast
from datetime import timedelta

# COMMAND ----------

# DBTITLE 1,Create forecast objects
dpf28 = DBUProphetForecast(forecast_periods=28)
dpf7 = DBUProphetForecast(forecast_periods=7)

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text('TargetCatalog', '')
dbutils.widgets.text('TargetSchema', 'prophet_forecast_schema')
target_catalog = dbutils.widgets.get('TargetCatalog')
target_schema = dbutils.widgets.get('TargetSchema')

# COMMAND ----------

# DBTITLE 1,Create and Set Target Objects
spark.sql(f'create catalog if not exists {target_catalog}')
spark.sql(f'create schema if not exists {target_catalog}.{target_schema}')
spark.sql(f'use catalog {target_catalog}')
spark.sql(f'use schema {target_schema}')

# COMMAND ----------

### 
## applyInPandas functions require us to declare with a wrapper function here
## Note: look for a way to avoid this
### 
def generate_forecast_udf(history_pd):
    return dpf7.generate_forecast( history_pd )
  
def evaluate_forecast_udf(evaluation_pd):
    return dpf7.evaluate_forecast_w_max_date( evaluation_pd )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7-Day Time Horizon 

# COMMAND ----------

spark.sql(f'drop table if exists input_consolidated_sku_eval_forecasts')
spark.sql(f'drop table if exists output_consolidated_sku_eval_forecasts7')
spark.sql(f'drop table if exists eval_metrics_consolidated_sku_forecasts7')

# COMMAND ----------

# DBTITLE 1,Read data from System table
df = dpf7.load_data(spark=spark).filter(col('workspace_id') == '6051921418418893')

# COMMAND ----------

# DBTITLE 1,Group and save dataframe as table 
input_df = dpf7.transform_consolidated_sku_data(df)

(
  input_df
  .write
  .option("mergeSchema", "true")
  .mode('overwrite')
  .saveAsTable(f"{target_catalog}.{target_schema}.input_consolidated_sku_eval_forecasts")
)

# COMMAND ----------

# Making this simple
# Could likely make this better by adding a new column in the group by
max_date = input_df.agg(max('ds')).collect()[0][0]
date7 = max_date - timedelta(days=7)
date14 = max_date - timedelta(days=14)
date21 = max_date - timedelta(days=21)
date28 = max_date - timedelta(days=28)
date56 = max_date - timedelta(days=56)
date_filter_list = [date7, date14, date21, date28]

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize input_consolidated_sku_eval_forecasts

# COMMAND ----------

input_df = spark.read.table(f"{target_catalog}.{target_schema}.input_consolidated_sku_eval_forecasts")

# COMMAND ----------

# DBTITLE 1,Generate Forecasts
for d in date_filter_list:
  print(f'----> Starting forecasts for max_date {d}') # don't judge my print statement
  results = (
    input_df.filter(input_df.ds < d) # notice we are less than to restrict training data 
      .groupBy('workspace_id','sku')
      .applyInPandas(generate_forecast_udf, schema=dpf7.forecast_result_schema)
      .withColumn('training_date', current_date() )
      .withColumn('max_forecast_date', lit(d))
      )
  
  out = (results.filter(col('ds') >= d).drop('y') # noteice we are greater than or equal to get the proper days after the max training data
        .join(input_df.select(col("y"), 
                            col("ds"),
                            col("workspace_id"), 
                            col("sku")), 
              ["ds", "workspace_id", "sku"], "left")
  )
  
  out.write.mode('append').saveAsTable(f'{target_catalog}.{target_schema}.output_consolidated_sku_eval_forecasts7')

# COMMAND ----------

# DBTITLE 1,Evaluate 7-day Forecasts
metrics = (
  spark.read
    .table(f'{target_catalog}.{target_schema}.output_consolidated_sku_eval_forecasts7')
    .select('training_date', 'workspace_id', 'sku', 'y', 'yhat', 'max_forecast_date')
    .groupBy('training_date', 'workspace_id', 'sku', 'max_forecast_date')
    .applyInPandas(evaluate_forecast_udf, schema=dpf7.analysis_eval_schema)
    )


# COMMAND ----------

# DBTITLE 1,Save 7-day Evaluation Results
(
  metrics.write
  .option("mergeSchema", "true")
  .mode('overwrite')
  .saveAsTable(f"{target_catalog}.{target_schema}.eval_metrics_consolidated_sku_forecasts7")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 28-Day Time Horizon

# COMMAND ----------

# DBTITLE 1,Drop tables just in case
spark.sql(f'drop table if exists output_consolidated_sku_eval_forecasts28')
spark.sql(f'drop table if exists eval_metrics_consolidated_sku_forecasts28')

# COMMAND ----------

### 
## applyInPandas functions require us to declare with a wrapper function here
## Note: look for a way to avoid this
### 
def generate_forecast_udf(history_pd):
    return dpf28.generate_forecast( history_pd )
  
def evaluate_forecast_udf(evaluation_pd):
    return dpf28.evaluate_forecast_w_max_date( evaluation_pd )


# COMMAND ----------

# DBTITLE 1,Re-load input df just in case
input_df = spark.read.table(f"{target_catalog}.{target_schema}.input_consolidated_sku_eval_forecasts") # we can reuse the input table

# COMMAND ----------

# DBTITLE 1,Two 28-Day forecast filter dates
date_filter_list = [date56, date28]
date_filter_list

# COMMAND ----------

# DBTITLE 1,For both filters do the foreast
for d in date_filter_list:
  print(f'----> Starting forecasts for max_date {d}') # don't judge my print statement
  results = (
    input_df.filter(input_df.ds < d) # notice we are less than to restrict training data 
      .groupBy('workspace_id','sku')
      .applyInPandas(generate_forecast_udf, schema=dpf28.forecast_result_schema)
      .withColumn('training_date', current_date() )
      .withColumn('max_forecast_date', lit(d))
      )
  
  out = (results.filter(col('ds') >= d).drop('y') # noteice we are greater than or equal to get the proper days after the max training data
        .join(input_df.select(col("y"), 
                            col("ds"),
                            col("workspace_id"), 
                            col("sku")), 
              ["ds", "workspace_id", "sku"], "left")
  )
  
  out.write.mode('append').saveAsTable(f'{target_catalog}.{target_schema}.output_consolidated_sku_eval_forecasts28')

# COMMAND ----------

# DBTITLE 1,Evaluate both forecasts
metrics = (
  spark.read
    .table(f'{target_catalog}.{target_schema}.output_consolidated_sku_eval_forecasts28')
    .select('training_date', 'workspace_id', 'sku', 'y', 'yhat', 'max_forecast_date')
    .groupBy('training_date', 'workspace_id', 'sku', 'max_forecast_date')
    .applyInPandas(evaluate_forecast_udf, schema=dpf28.analysis_eval_schema)
    )


# COMMAND ----------

# DBTITLE 1,Save Metrics
(
  metrics.write
  .option("mergeSchema", "true")
  .mode('overwrite')
  .saveAsTable(f"{target_catalog}.{target_schema}.eval_metrics_consolidated_sku_forecasts28")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visuals

# COMMAND ----------

# DBTITLE 1,7-Day Accuracy
display(spark.sql(f"""
select *
  , case when y >= yhat_lower AND y <= yhat_upper THEN true ELSE false END AS on_trend
  , DATEDIFF(ds, max_forecast_date) AS diff_days

from {target_catalog}.{target_schema}.output_consolidated_sku_eval_forecasts7
order by ds asc                  
"""))

# COMMAND ----------

# DBTITLE 1,28-Day Accuracy
display(spark.sql(f"""
select *
  , case when y >= yhat_lower AND y <= yhat_upper THEN true ELSE false END AS on_trend
  , DATEDIFF(ds, max_forecast_date) AS diff_days

from {target_catalog}.{target_schema}.output_consolidated_sku_eval_forecasts28
order by ds asc                  
"""))

# COMMAND ----------

# DBTITLE 1,7-Day Regression Metrics
display(spark.sql(f"""
select a.training_date
  , a.workspace_id
  , a.sku
  , a.max_forecast_date
  , a.mae
  , a.mse
  , a.rmse
  , sum(b.y) as TotalDBUs 
  , avg(b.y) as AvgDBUs

  from {target_catalog}.{target_schema}.eval_metrics_consolidated_sku_forecasts7 a
  inner join {target_catalog}.{target_schema}.output_consolidated_sku_eval_forecasts7 b on a.workspace_id = b.workspace_id and a.sku = b.sku and a.max_forecast_date = b.max_forecast_date

  group by 1,2,3,4,5,6,7

  order by TotalDBUs desc

"""))

# COMMAND ----------

# DBTITLE 1,28-Day Regression Metrics
display(spark.sql(f"""
select a.training_date
  , a.workspace_id
  , a.sku
  , a.max_forecast_date
  , a.mae
  , a.mse
  , a.rmse
  , sum(b.y) as TotalDBUs 
  , avg(b.y) as AvgDBUs

  from {target_catalog}.{target_schema}.eval_metrics_consolidated_sku_forecasts28 a
  inner join {target_catalog}.{target_schema}.output_consolidated_sku_eval_forecasts28 b on a.workspace_id = b.workspace_id and a.sku = b.sku and a.max_forecast_date = b.max_forecast_date

  group by 1,2,3,4,5,6,7

  order by TotalDBUs desc

"""))

# COMMAND ----------


