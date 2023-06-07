# Databricks notebook source
# MAGIC %md
# MAGIC # Run Consolidated DBU Forecasts
# MAGIC
# MAGIC This notebook sources data from Databricks System Tables (`system.operational_data.billing_logs`). Generates Prophet forecasts by SKU and Workspace. 
# MAGIC
# MAGIC SKUs are aggregated to the top level categories: `ALL_PURPOSE`, `DLT`, `JOBS`, `MODEL_INFERENCE`, and `SQL`. 
# MAGIC
# MAGIC This notebook generates and evaluates forecasts. Data is saved to the following tables: 
# MAGIC 1. `input_dbus_by_date_sku_workspace`  
# MAGIC 1. `output_dbu_forecasts_by_date_sku_workspace`  
# MAGIC 1. `dbu_forecast_evals_by_date_sku_workspace`  

# COMMAND ----------

# DBTITLE 1,Import libs
from pyspark.sql.functions import *
from libs.dbu_prophet_forecast import DBUProphetForecast

# COMMAND ----------

# DBTITLE 1,Create forecast objects
dpf = DBUProphetForecast(forecast_periods=28)

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text('TargetCatalog', '')
dbutils.widgets.text('TargetSchema', '')
target_catalog = dbutils.widgets.get('TargetCatalog')
target_schema = dbutils.widgets.get('TargetSchema')

# COMMAND ----------

# DBTITLE 1,Create and Set Target Objects
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

# DBTITLE 1,Save Forecast Results
(
  results.write
  .option("mergeSchema", "true")
  .mode('overwrite')
  .saveAsTable(f"{target_catalog}.{target_schema}.output_dbu_forecasts_by_date_sku_workspace")
)

# COMMAND ----------

# DBTITLE 1,Evaluate Forecasts
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
