# Databricks notebook source
# MAGIC %md
# MAGIC # Run Dollar DBU Forecasts - !!INCOMPLETE!!
# MAGIC
# MAGIC This notebook sources data from Databricks System Tables (`system.operational_data.billing_logs`). Generates Prophet forecasts by SKU and Workspace. 
# MAGIC
# MAGIC Please note that SKUs are not aggregated and we produce forecasts for all SKU types i.e. `STANDARD_ALL_PURPOSE_COMPUTE`, `PREMIUM_ALL_PURPOSE_COMPUTE`
# MAGIC
# MAGIC This notebook generates and evaluates forecasts. Data is saved to the following tables: 
# MAGIC 1. `input_dbus_by_date_system_sku_workspace`  
# MAGIC 1. `output_dbu_forecasts_by_date_system_sku_workspace`  
# MAGIC 1. `dbu_forecast_evals_by_date_system_sku_workspace`  

# COMMAND ----------

# DBTITLE 1,Import libs
from pyspark.sql.functions import *
from libs.dbu_prophet_forecast import DBUProphetForecast
from libs.ddl_helper import DDLHelper

# COMMAND ----------

# DBTITLE 1,Create forecast obj
dpf = DBUProphetForecast(forecast_periods=28)

# COMMAND ----------

# DBTITLE 1,Make sure we have a cost table
## DDL Helper is used to create a cost lookup table
# create obj
ddl_help = DDLHelper(spark=spark)
# create table DDL
ddl_help.create_cost_lookup_table(target_catalog=target_catalog, target_schema=target_schema)
# insert overwrite into tale
ddl_help.insert_into_cost_lookup_table(target_catalog=target_catalog, target_schema=target_schema)

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text('TargetCatalog', 'ryan_chynoweth_catalog')
dbutils.widgets.text('TargetSchema', 'prophet_forecast_schema')
target_catalog = dbutils.widgets.get('TargetCatalog')
target_schema = dbutils.widgets.get('TargetSchema')

# COMMAND ----------

# DBTITLE 1,Create data objects
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
    return dpf.generate_dollar_dbu_forecast( history_pd )

## Need to change this for new function
def evaluate_forecast_udf(evaluation_pd):
    return dpf.evaluate_forecast( evaluation_pd ) 


# COMMAND ----------

# DBTITLE 1,Read data from System table
df = dpf.load_data(spark=spark).filter(col('workspace_id') == '6051921418418893')
display(df)

# COMMAND ----------

display(
  df.select(col("created_on").alias('ds'), col('sku'), col('dbus'), col('workspace_id'))
  .join(spark.read.table('sku_cost_lookup'), on='sku', how='left')
  .withColumn('dollar_dbus', col('dbus')*col('list_price'))
  # .drop('dollar_dbus', 'dbus', 'sku')
  # .groupBy(col("ds"), col('workspace_id'))
  # .agg(sum('list_price').alias("y"))
  )

display(group_df)

# COMMAND ----------

group_df = (
  df.select(col("created_on").alias('ds'), col('sku'), col('dbus'), col('workspace_id'))
  .join(spark.read.table('sku_cost_lookup'), on='sku', how='left')
  .withColumn('dollar_dbus', col('dbus')*col('list_price'))
  # .drop('dollar_dbus', 'dbus', 'sku')
  .groupBy(col("ds"), col('workspace_id'))
  .agg(sum('list_price').alias("y"))
  )

display(group_df)

# COMMAND ----------

# DBTITLE 1,Group and save dataframe as table 
(
  group_df
  .write
  .option("mergeSchema", "true")
  .mode('overwrite')
  .saveAsTable(f"{target_catalog}.{target_schema}.input_dollar_dbus")
)

# COMMAND ----------

spark.sql(f"optimize {target_catalog}.{target_schema}.input_dollar_dbus") # makes forecasts faster

# COMMAND ----------

# DBTITLE 1,Generate Forecasts
results = (
  spark.read.table(f"{target_catalog}.{target_schema}.input_dollar_dbus")
    .select('ds', 'y', 'workspace_id')
    .groupBy('workspace_id')
    .applyInPandas(generate_forecast_udf, schema=dpf.dollar_dbu_forecast_result_schema)
    .withColumn('training_date', current_date() )
    )

# COMMAND ----------

# DBTITLE 1,Save Results
(
  results.write
  .option("mergeSchema", "true")
  .mode('overwrite')
  .saveAsTable(f"{target_catalog}.{target_schema}.output_dollar_dbu_forecasts")
)

# COMMAND ----------

# DBTITLE 1,Evaluate Forecasts - Not working
results = (
  spark.read
    .table(f'{target_catalog}.{target_schema}.output_dollar_dbu_forecasts')
    .select('training_date', 'workspace_id', 'sku', 'y', 'yhat')
    .groupBy('training_date', 'workspace_id', 'sku')
    .applyInPandas(evaluate_forecast_udf, schema=dpf.eval_schema)
    )


# COMMAND ----------

# DBTITLE 1,Save Evaluation Results - Not Working
(
  results.write
  .option("mergeSchema", "true")
  .mode('overwrite')
  .saveAsTable(f"{target_catalog}.{target_schema}.dbu_forecast_evals_by_date_system_sku_workspace")
)
