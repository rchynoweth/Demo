# Databricks notebook source
# MAGIC %md
# MAGIC NOTE -> This notebook produces forecasts by workspace and Databricks compute SKU. These forecasts can be used to automate anomolous behavior in Databricks consumption (DBUs). Please refer to the "Demo" notebook for sample visuals. Please note that the Demo notebook does not forecast by workspace. 
# MAGIC
# MAGIC Forecasted should be generated weekly and evaluated daily. Forecasts are at the day level. 

# COMMAND ----------

from prophet import Prophet
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from sklearn.metrics import mean_squared_error, mean_absolute_error
from math import sqrt
from datetime import date, datetime, timedelta

# COMMAND ----------

dbutils.widgets.text('TargetCatalog', '')
dbutils.widgets.text('TargetSchema', '')

# COMMAND ----------

# target_schema = 'ryan_chynoweth_catalog.ryan_chynoweth_schema'
target_catalog = dbutils.widgets.get('TargetCatalog')
target_schema = dbutils.widgets.get('TargetSchema')

# COMMAND ----------

spark.sql(f"create catalog if not exists {target_catalog}")

# COMMAND ----------

spark.sql(f"create schema if not exists {target_catalog}.{target_schema}")

# COMMAND ----------

spark.sql('use catalog system')

# COMMAND ----------

spark.sql('use schema operational_data')

# COMMAND ----------

# we will complete 5 different test
# 1 full 28 day forecast evaluation 
# 4 separate 7 day forecast evaluations 
# the 28 day eval should give a practical idea how far out we can forecast 
# the 7 day evals should give us an idea about our ability to monitor and alert 
test_backfill = 28 # 28 days 
test_increment = 7 # 7 days

# COMMAND ----------

# DBTITLE 1,Read data from System table - BI DBU visuals
df = spark.sql("""
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
 , case when contains(sku, 'ALL_PURPOSE') then 'ALL_PURPOSE'
        when contains(sku, 'JOBS') then 'JOBS'
        when contains(sku, 'DLT') then 'DLT'
        when contains(sku, 'SQL') then 'SQL'
        when contains(sku, 'INFERENCE') then 'MODEL_INFERENCE'
        else 'OTHER' end as consolidated_sku
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
where created_on >= '2021-01-01'
and workspace_id = '6051921418418893' -- filter this due to data availability. 
""")

display(df)

# COMMAND ----------

max_date = df.agg(max('created_on')).collect()[0][0]
print(max_date)

# COMMAND ----------

# DBTITLE 1,Group and save dataframe as table - primary forecast data source
prophet_df = df.select(col('created_on').alias('ds'), col('consolidated_sku').alias('sku'), col('workspace_id'), col('machine_hours'), col('dbus'))

(
  prophet_df
  .groupBy(col("ds"), col('sku'), col('workspace_id'))
  .agg(sum("dbus").alias("y"))
  .write
  .option("mergeSchema", "true")
  .mode('overwrite')
  .saveAsTable(f"{target_catalog}.{target_schema}.dbus_by_date_sku")
)

# COMMAND ----------

# DBTITLE 1,Load DBU table as pandas df
input_dbu_df = spark.read.table(f"{target_catalog}.{target_schema}.dbus_by_date_sku")

input_dbu_df = input_dbu_df.withColumn("workspace_id", col("workspace_id").cast("string"))


# COMMAND ----------

## Drop workspaces/skus that do not have more than 10 rows
## this is a requirement by the Prophet library. 
dbu_df = input_dbu_df.groupBy("sku", "workspace_id").count().filter("count > 10").join(input_dbu_df, on=["sku", "workspace_id"], how="inner").drop('count')

# COMMAND ----------

result_schema =StructType([
  StructField('ds',DateType()),
  StructField('workspace_id', StringType()),
  StructField('sku',StringType()),
  StructField('y',FloatType()),
  StructField('yhat',FloatType()),
  StructField('yhat_upper',FloatType()),
  StructField('yhat_lower',FloatType())
  ])

# COMMAND ----------

# DBTITLE 1,Pandas UDF to forecast by SKU - 7 day forecasts
def generate_forecast( history_pd ):
  
  # TRAIN MODEL AS BEFORE
  # --------------------------------------
  # remove missing values (more likely at day-store-item level)
  history_pd = history_pd.dropna()
  
  # configure the model
  model = Prophet( interval_width=0.85 )
  
  # train the model
  model.fit( history_pd )
  # --------------------------------------
  
  # BUILD FORECAST AS BEFORE
  # --------------------------------------
  # make predictions
  future_pd = model.make_future_dataframe(
    periods=7, 
    freq='d', 
    include_history=True
    )
  forecast_pd = model.predict( future_pd )  
  # --------------------------------------
  
  # ASSEMBLE EXPECTED RESULT SET
  # --------------------------------------
  # get relevant fields from forecast
  f_pd = forecast_pd[ ['ds','yhat', 'yhat_upper', 'yhat_lower'] ].set_index('ds')
  
  # get relevant fields from history
  h_pd = history_pd[['ds','workspace_id','sku','y']].set_index('ds')
  
  # join history and forecast
  results_pd = f_pd.join( h_pd, how='left' )
  results_pd.reset_index(level=0, inplace=True)
  
  # get store & item from incoming data set
  results_pd['sku'] = history_pd['sku'].iloc[0]
  results_pd['workspace_id'] = history_pd['workspace_id'].iloc[0]
  # --------------------------------------
  
  # return expected dataset
  return results_pd[ ['ds', 'workspace_id', 'sku', 'y', 'yhat', 'yhat_upper', 'yhat_lower'] ]  


# COMMAND ----------

spark.sql(f'drop table if exists {target_catalog}.{target_schema}.dbu_forecasts_7evals')

# COMMAND ----------

# DBTITLE 1,Create and save forecasts for 7 day periods
date7 = max_date - timedelta(days=7)
date14 = max_date - timedelta(days=14)
date21 = max_date - timedelta(days=21)
date28 = max_date - timedelta(days=28)
max_dates = [date7, date14, date21, date28]

for d in max_dates:
  print(f"----- {d} ")
  results = (dbu_df.filter(dbu_df.ds < d)
      .groupBy('workspace_id','sku')
      .applyInPandas(generate_forecast, schema=result_schema)
      .withColumn('training_date', current_date() )
      .withColumn('max_forecast_date', lit(d))
    )
  
  out = (results.filter(col('ds') >= d).drop('y')
        .join(dbu_df.select(col("y"), col("ds"), col("workspace_id"), col("sku")), ["ds", "workspace_id", "sku"], "left")
  )
  out.write.mode('append').saveAsTable(f'{target_catalog}.{target_schema}.dbu_forecasts_7evals')



# COMMAND ----------

# DBTITLE 1,Function for 28 day forecast
def generate_forecast( history_pd ):
  
  # TRAIN MODEL AS BEFORE
  # --------------------------------------
  # remove missing values (more likely at day-store-item level)
  history_pd = history_pd.dropna()
  
  # configure the model
  model = Prophet( interval_width=0.85 )
  
  # train the model
  model.fit( history_pd )
  # --------------------------------------
  
  # BUILD FORECAST AS BEFORE
  # --------------------------------------
  # make predictions
  future_pd = model.make_future_dataframe(
    periods=28, 
    freq='d', 
    include_history=True
    )
  forecast_pd = model.predict( future_pd )  
  # --------------------------------------
  
  # ASSEMBLE EXPECTED RESULT SET
  # --------------------------------------
  # get relevant fields from forecast
  f_pd = forecast_pd[ ['ds','yhat', 'yhat_upper', 'yhat_lower'] ].set_index('ds')
  
  # get relevant fields from history
  h_pd = history_pd[['ds','workspace_id','sku','y']].set_index('ds')
  
  # join history and forecast
  results_pd = f_pd.join( h_pd, how='left' )
  results_pd.reset_index(level=0, inplace=True)
  
  # get store & item from incoming data set
  results_pd['sku'] = history_pd['sku'].iloc[0]
  results_pd['workspace_id'] = history_pd['workspace_id'].iloc[0]
  # --------------------------------------
  
  # return expected dataset
  return results_pd[ ['ds', 'workspace_id', 'sku', 'y', 'yhat', 'yhat_upper', 'yhat_lower'] ]  


# COMMAND ----------

# DBTITLE 1,28 day forecast
results = (dbu_df.filter(dbu_df.ds < date28)
    .groupBy('workspace_id','sku')
    .applyInPandas(generate_forecast, schema=result_schema)
    .withColumn('training_date', current_date() )
    .withColumn('max_forecast_date', lit(d))
  )

out = (results.filter(col('ds') >= date28).drop('y')
        .join(dbu_df.select(col("y"), col("ds"), col("workspace_id"), col("sku")), ["ds", "workspace_id", "sku"], "left")
  )

out.write.mode('overwrite').saveAsTable(f'{target_catalog}.{target_schema}.dbu_forecasts_28evals')

# COMMAND ----------

# DBTITLE 1,Evaluate how the forecasts performed
# schema of expected result set
eval_schema =StructType([
  StructField('training_date', DateType()),
  StructField('max_forecast_date', DateType()),
  StructField('workspace_id', StringType()),
  StructField('sku', StringType()),
  StructField('mae', FloatType()),
  StructField('mse', FloatType()),
  StructField('rmse', FloatType())
  ])

# define udf to calculate metrics
def evaluate_forecast( evaluation_pd ):
  
  evaluation_pd = evaluation_pd[evaluation_pd['dbus'].notnull()]
  # get sku in incoming data set
  training_date = evaluation_pd['training_date'].iloc[0]
  sku = evaluation_pd['sku'].iloc[0]
  workspace_id = evaluation_pd['workspace_id'].iloc[0]
  max_forecast_date = evaluation_pd['max_forecast_date'].iloc[0]
  
  # calulate evaluation metrics
  mae = mean_absolute_error( evaluation_pd['dbus'], evaluation_pd['dbus_predicted'] )
  mse = mean_squared_error( evaluation_pd['dbus'], evaluation_pd['dbus_predicted'] )
  rmse = sqrt( mse )
  
  # assemble result set
  results = {'training_date':[training_date], 'workspace_id':[workspace_id], 'max_forecast_date':[max_forecast_date], 'sku':[sku], 'mae':[mae], 'mse':[mse], 'rmse':[rmse]}
  return pd.DataFrame.from_dict( results )

# COMMAND ----------


# calculate metrics
results7 = (
  spark.read
    .table(f'{target_catalog}.{target_schema}.dbu_forecasts_7evals')
    .select('training_date','max_forecast_date', 'workspace_id', 'sku', col('y').alias('dbus'), col('yhat').alias('dbus_predicted'))
    .groupBy('training_date', 'max_forecast_date', 'workspace_id', 'sku')
    .applyInPandas(evaluate_forecast, schema=eval_schema)
    )

results7.write.mode('overwrite').saveAsTable(f'{target_catalog}.{target_schema}.dbu_forecasts_7eval_metrics')


results28 = (
  spark.read
    .table(f'{target_catalog}.{target_schema}.dbu_forecasts_28evals')
    .select('training_date','max_forecast_date', 'workspace_id', 'sku', col('y').alias('dbus'), col('yhat').alias('dbus_predicted'))
    .groupBy('training_date', 'max_forecast_date', 'workspace_id', 'sku')
    .applyInPandas(evaluate_forecast, schema=eval_schema)
    )

results28.write.mode('overwrite').saveAsTable(f'{target_catalog}.{target_schema}.dbu_forecasts_28eval_metrics')


# COMMAND ----------


