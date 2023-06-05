# Databricks notebook source
# MAGIC %md
# MAGIC NOTE -> This notebook produces forecasts by workspace and Databricks compute SKU. These forecasts can be used to automate anomolous behavior in Databricks consumption (DBUs). Forecasted should be generated weekly and evaluated daily. Forecasts are at the day level. 

# COMMAND ----------

from prophet import Prophet
from pyspark.sql.functions import *
import pandas as pd
from sklearn.metrics import mean_squared_error, mean_absolute_error
from math import sqrt
from datetime import date

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
""")

display(df)

# COMMAND ----------

# DBTITLE 1,Jobs Only Visuals
jobs_df = spark.sql("""
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
where created_on >= '2021-01-01' 
      and tags.JobId is not Null 
""")

display(jobs_df)

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

## Drop workspaces/skus that do not have more than 2 rows
## this is a requirement by the Prophet library. 
dbu_df = input_dbu_df.groupBy("sku", "workspace_id").count().filter("count > 2").join(input_dbu_df, on=["sku", "workspace_id"], how="inner").drop('count')

# COMMAND ----------

from pyspark.sql.types import *

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

# DBTITLE 1,Pandas UDF to forecast by SKU
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

# DBTITLE 1,Create and collect forecasts to save as a temp view
results = (
  dbu_df
    .groupBy('workspace_id','sku')
    .applyInPandas(generate_forecast, schema=result_schema)
    .withColumn('training_date', current_date() )
    )

results.createOrReplaceTempView('new_forecasts')

# COMMAND ----------

# DBTITLE 1,Save raw forecasts as table
spark.sql(f"""
create table if not exists {target_catalog}.{target_schema}.dbu_forecasts (
  date date,
  workspace_id string,
  sku string,
  dbus float,
  dbus_predicted float,
  dbus_predicted_upper float,
  dbus_predicted_lower float,
  training_date date
  )
using delta
partitioned by (training_date);
""")

spark.sql(f"""
-- load data to it
insert into {target_catalog}.{target_schema}.dbu_forecasts
select 
  ds as date,
  workspace_id,
  sku,
  y as dbus,
  yhat as dbus_predicted,
  yhat_upper as dbus_predicted_upper,
  yhat_lower as dbus_predicted_lower,
  training_date
from new_forecasts;
""")

# COMMAND ----------

# DBTITLE 1,All DBUs
display(
  spark.sql(f"""
    select 
    date
    , workspace_id
    , sku
    , dbus
    , case when dbus_predicted < 0 then 0 else dbus_predicted end as dbus_predicted
    , case when dbus_predicted_upper < 0 then 0 else dbus_predicted_upper end as dbus_predicted_upper
    , case when dbus_predicted_lower < 0 then 0 else dbus_predicted_lower end as dbus_predicted_lower
    , case when dbus > dbus_predicted_upper then TRUE else FALSE end as upper_anomaly_alert
    , case when dbus < dbus_predicted_lower then TRUE else FALSE end as lower_anomaly_alert
    , training_date
    
    from {target_catalog}.{target_schema}.dbu_forecasts

""") 
)

# COMMAND ----------

# DBTITLE 1,All Purpose DBUs
display(
  spark.sql(f"""
    select 
    date
    , workspace_id
    , sku
    , dbus
    , case when dbus_predicted < 0 then 0 else dbus_predicted end as dbus_predicted
    , case when dbus_predicted_upper < 0 then 0 else dbus_predicted_upper end as dbus_predicted_upper
    , case when dbus_predicted_lower < 0 then 0 else dbus_predicted_lower end as dbus_predicted_lower
    , case when dbus > dbus_predicted_upper then TRUE else FALSE end as upper_anomaly_alert
    , case when dbus < dbus_predicted_lower then TRUE else FALSE end as lower_anomaly_alert
    , training_date
    
    from {target_catalog}.{target_schema}.dbu_forecasts
    where sku = 'ALL_PURPOSE'

""") 
)

# COMMAND ----------

# DBTITLE 1,Jobs DBUs
display(
  spark.sql(f"""
    select 
    date
    , workspace_id
    , sku
    , dbus
    , case when dbus_predicted < 0 then 0 else dbus_predicted end as dbus_predicted
    , case when dbus_predicted_upper < 0 then 0 else dbus_predicted_upper end as dbus_predicted_upper
    , case when dbus_predicted_lower < 0 then 0 else dbus_predicted_lower end as dbus_predicted_lower
    , case when dbus > dbus_predicted_upper then TRUE else FALSE end as upper_anomaly_alert
    , case when dbus < dbus_predicted_lower then TRUE else FALSE end as lower_anomaly_alert
    , training_date
    
    from {target_catalog}.{target_schema}.dbu_forecasts
    where sku = 'JOBS'

""") 
)

# COMMAND ----------

# DBTITLE 1,DLT DBUs
display(
  spark.sql(f"""
    select 
    date
    , workspace_id
    , sku
    , dbus
    , case when dbus_predicted < 0 then 0 else dbus_predicted end as dbus_predicted
    , case when dbus_predicted_upper < 0 then 0 else dbus_predicted_upper end as dbus_predicted_upper
    , case when dbus_predicted_lower < 0 then 0 else dbus_predicted_lower end as dbus_predicted_lower
    , case when dbus > dbus_predicted_upper then TRUE else FALSE end as upper_anomaly_alert
    , case when dbus < dbus_predicted_lower then TRUE else FALSE end as lower_anomaly_alert
    , training_date
    
    from {target_catalog}.{target_schema}.dbu_forecasts
    where sku = 'DLT'

""") 
)

# COMMAND ----------

# DBTITLE 1,SQL DBUs
display(
  spark.sql(f"""
    select 
    date
    , workspace_id
    , sku
    , dbus
    , case when dbus_predicted < 0 then 0 else dbus_predicted end as dbus_predicted
    , case when dbus_predicted_upper < 0 then 0 else dbus_predicted_upper end as dbus_predicted_upper
    , case when dbus_predicted_lower < 0 then 0 else dbus_predicted_lower end as dbus_predicted_lower
    , case when dbus > dbus_predicted_upper then TRUE else FALSE end as upper_anomaly_alert
    , case when dbus < dbus_predicted_lower then TRUE else FALSE end as lower_anomaly_alert
    , training_date
    
    from {target_catalog}.{target_schema}.dbu_forecasts
    where sku = 'SQL'

""") 
)

# COMMAND ----------

# DBTITLE 1,Model Inference DBUs
display(
  spark.sql(f"""
    select 
    date
    , workspace_id
    , sku
    , dbus
    , case when dbus_predicted < 0 then 0 else dbus_predicted end as dbus_predicted
    , case when dbus_predicted_upper < 0 then 0 else dbus_predicted_upper end as dbus_predicted_upper
    , case when dbus_predicted_lower < 0 then 0 else dbus_predicted_lower end as dbus_predicted_lower
    , case when dbus > dbus_predicted_upper then TRUE else FALSE end as upper_anomaly_alert
    , case when dbus < dbus_predicted_lower then TRUE else FALSE end as lower_anomaly_alert
    , training_date
    
    from {target_catalog}.{target_schema}.dbu_forecasts
    where sku = 'MODEL_INFERENCE'

""") 
)

# COMMAND ----------

# DBTITLE 1,Evaluate how the forecasts performed
# schema of expected result set
eval_schema =StructType([
  StructField('training_date', DateType()),
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
  
  # calulate evaluation metrics
  mae = mean_absolute_error( evaluation_pd['dbus'], evaluation_pd['dbus_predicted'] )
  mse = mean_squared_error( evaluation_pd['dbus'], evaluation_pd['dbus_predicted'] )
  rmse = sqrt( mse )
  
  # assemble result set
  results = {'training_date':[training_date], 'workspace_id':[workspace_id], 'sku':[sku], 'mae':[mae], 'mse':[mse], 'rmse':[rmse]}
  return pd.DataFrame.from_dict( results )


# calculate metrics
results = (
  spark.read
    .table(f'{target_catalog}.{target_schema}.dbu_forecasts')
    .select('training_date', 'workspace_id', 'sku', 'dbus', 'dbus_predicted')
    .groupBy('training_date', 'workspace_id', 'sku')
    .applyInPandas(evaluate_forecast, schema=eval_schema)
    )
results.createOrReplaceTempView('new_forecast_evals')

# COMMAND ----------

display(
  spark.sql('select * from new_forecast_evals')
)

# COMMAND ----------

# DBTITLE 1,Save evaluation of forecasts to table
spark.sql(f"""
create table if not exists {target_catalog}.{target_schema}.dbu_forecast_evals (
  workspace_id string,
  sku string,
  mae float,
  mse float,
  rmse float,
  training_date date
  )
using delta
partitioned by (training_date);
""")

spark.sql(f"""
insert into {target_catalog}.{target_schema}.dbu_forecast_evals
select
  sku,
  mae,
  mse,
  rmse,
  training_date
from new_forecast_evals;
""")

# COMMAND ----------

display(
  spark.read.table(f'{target_catalog}.{target_schema}.dbu_forecast_evals')
)

# COMMAND ----------

spark.sql(f"""
create or replace table {target_schema}.cost_lookup_table (
sku string,
listPrice double
)
""")

spark.sql(f"""
INSERT INTO {target_schema}.cost_lookup_table values
("ENTERPRISE_ALL_PURPOSE_COMPUTE", 0.65),
("ENTERPRISE_ALL_PURPOSE_COMPUTE_DLT", 0.65),
("ENTERPRISE_ALL_PURPOSE_COMPUTE_PHOTON", 0.65),
("ENTERPRISE_DLT_ADVANCED_COMPUTE", 0.36),
("ENTERPRISE_DLT_ADVANCED_COMPUTE_PHOTON", 0.36),
("ENTERPRISE_DLT_CORE_COMPUTE", 0.2),
("ENTERPRISE_DLT_CORE_COMPUTE_PHOTON", 0.2),
("ENTERPRISE_DLT_PRO_COMPUTE", 0.25),
("ENTERPRISE_DLT_PRO_COMPUTE_PHOTON", 0.25),
("ENTERPRISE_JOBS_COMPUTE", 0.2),
("ENTERPRISE_JOBS_COMPUTE_PHOTON", 0.2),
("ENTERPRISE_JOBS_LIGHT_COMPUTE", 0.13),
("ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_AP_SINGAPORE", 0.088),
("ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_AP_SYDNEY", 0.088),
("ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_CANADA", 0.078),
("ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_EUROPE_FRANKFURT", 0.084),
("ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_EUROPE_IRELAND", 0.078),
("ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_AP_SINGAPORE", 0.088),
("ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_AP_SYDNEY", 0.088),
("ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_CANADA", 0.078),
("ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_EUROPE_FRANKFURT", 0.084),
("ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_EUROPE_IRELAND", 0.078),
("ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_EAST_N_VIRGINIA", 0.07),
("ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_EAST_OHIO", 0.07),
("ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_WEST_OREGON", 0.07),
("ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_US_EAST_N_VIRGINIA", 0.07),
("ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_US_EAST_OHIO", 0.07),
("ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_US_WEST_OREGON", 0.07),
("ENTERPRISE_SERVERLESS_SQL_COMPUTE", 0.55),
("ENTERPRISE_SERVERLESS_SQL_COMPUTE_AP_SYDNEY", 0.95),
("ENTERPRISE_SERVERLESS_SQL_COMPUTE_EUROPE_FRANKFURT", 0.91),
("ENTERPRISE_SERVERLESS_SQL_COMPUTE_EUROPE_IRELAND", 0.91),
("ENTERPRISE_SERVERLESS_SQL_COMPUTE_US_EAST_N_VIRGINIA", 0.7),
("ENTERPRISE_SERVERLESS_SQL_COMPUTE_US_EAST_OHIO", 0.7),
("ENTERPRISE_SERVERLESS_SQL_COMPUTE_US_WEST_OREGON", 0.7),
("ENTERPRISE_SQL_COMPUTE", 0.22),
("ENTERPRISE_SQL_PRO_COMPUTE_AP_MUMBAI", 0.61),
("ENTERPRISE_SQL_PRO_COMPUTE_AP_SEOUL", 0.74),
("ENTERPRISE_SQL_PRO_COMPUTE_AP_SINGAPORE", 0.69),
("ENTERPRISE_SQL_PRO_COMPUTE_AP_SYDNEY", 0.74),
("ENTERPRISE_SQL_PRO_COMPUTE_AP_TOKYO", 0.78),
("ENTERPRISE_SQL_PRO_COMPUTE_CANADA", 0.62),
("ENTERPRISE_SQL_PRO_COMPUTE_EUROPE_FRANCE", 0.72),
("ENTERPRISE_SQL_PRO_COMPUTE_EUROPE_FRANKFURT", 0.72),
("ENTERPRISE_SQL_PRO_COMPUTE_EUROPE_IRELAND", 0.72),
("ENTERPRISE_SQL_PRO_COMPUTE_EUROPE_LONDON", 0.74),
("ENTERPRISE_SQL_PRO_COMPUTE_SA_BRAZIL", 0.85),
("ENTERPRISE_SQL_PRO_COMPUTE_US_EAST_N_VIRGINIA", 0.55),
("ENTERPRISE_SQL_PRO_COMPUTE_US_EAST_OHIO", 0.55),
("ENTERPRISE_SQL_PRO_COMPUTE_US_WEST_CALIFORNIA", 0.55),
("ENTERPRISE_SQL_PRO_COMPUTE_US_WEST_OREGON", 0.55),
-- PREMIUM tier
("PREMIUM_ALL_PURPOSE_COMPUTE", 0.55),
("PREMIUM_ALL_PURPOSE_COMPUTE_DLT", 0.55),
("PREMIUM_ALL_PURPOSE_COMPUTE_PHOTON", 0.55),
("PREMIUM_DLT_ADVANCED_COMPUTE", 0.36),
("PREMIUM_DLT_ADVANCED_COMPUTE_PHOTON", 0.36),
("PREMIUM_DLT_CORE_COMPUTE", 0.2),
("PREMIUM_DLT_CORE_COMPUTE_PHOTON", 0.2),
("PREMIUM_DLT_PRO_COMPUTE", 0.25),
("PREMIUM_DLT_PRO_COMPUTE_PHOTON", 0.25),
("PREMIUM_JOBS_COMPUTE", 0.15),
("PREMIUM_JOBS_COMPUTE_PHOTON", 0.15),
("PREMIUM_JOBS_LIGHT_COMPUTE", 0.1),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_AP_SINGAPORE", 0.088),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_AP_SYDNEY", 0.088),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_ASIA_SOUTHEAST", 0.088),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_AUSTRALIA_EAST", 0.088),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_CANADA", 0.078),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_CANADA_CENTRAL", 0.078),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_EUROPE_FRANKFURT", 0.084),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_EUROPE_IRELAND", 0.078),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_EU_NORTH", 0.078),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_EU_WEST", 0.084),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_AP_SINGAPORE", 0.088),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_AP_SYDNEY", 0.088),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_ASIA_SOUTHEAST", 0.088),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_AUSTRALIA_EAST", 0.088),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_CANADA", 0.078),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_CANADA_CENTRAL", 0.078),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_EUROPE_FRANKFURT", 0.084),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_EUROPE_IRELAND", 0.078),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_EU_NORTH", 0.078),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_EU_WEST", 0.084),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_CENTRAL", 0.079),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_EAST", 0.07),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_EAST_2", 0.07),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_EAST_N_VIRGINIA", 0.07),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_EAST_OHIO", 0.07),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_NORTH_CENTRAL", 0.07),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_WEST", 0.082),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_WEST_OREGON", 0.07),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_CENTRAL", 0.079),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_EAST", 0.07),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_EAST_2", 0.07),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_EAST_N_VIRGINIA", 0.07),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_EAST_OHIO", 0.07),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_NORTH_CENTRAL", 0.07),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_WEST", 0.082),
("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_WEST_OREGON", 0.07),
("PREMIUM_SERVERLESS_SQL_COMPUTE", 0.55),
("PREMIUM_SERVERLESS_SQL_COMPUTE_AP_SYDNEY", 0.95),
("PREMIUM_SERVERLESS_SQL_COMPUTE_EUROPE_FRANKFURT", 0.91),
("PREMIUM_SERVERLESS_SQL_COMPUTE_EUROPE_IRELAND", 0.91),
("PREMIUM_SERVERLESS_SQL_COMPUTE_EU_NORTH", 0.91),
("PREMIUM_SERVERLESS_SQL_COMPUTE_EU_WEST", 0.91),
("PREMIUM_SERVERLESS_SQL_COMPUTE_US_CENTRAL", 0.7),
("PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST", 0.7),
("PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_2", 0.7),
("PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_N_VIRGINIA", 0.7),
("PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_OHIO", 0.7),
("PREMIUM_SERVERLESS_SQL_COMPUTE_US_WEST", 0.7),
("PREMIUM_SERVERLESS_SQL_COMPUTE_US_WEST_OREGON", 0.7),
("PREMIUM_SQL_COMPUTE", 0.22),
("PREMIUM_SQL_PRO_COMPUTE_AP_MUMBAI", 0.61),
("PREMIUM_SQL_PRO_COMPUTE_AP_SEOUL", 0.74),
("PREMIUM_SQL_PRO_COMPUTE_AP_SINGAPORE", 0.69),
("PREMIUM_SQL_PRO_COMPUTE_AP_SYDNEY", 0.74),
("PREMIUM_SQL_PRO_COMPUTE_AP_TOKYO", 0.78),
("PREMIUM_SQL_PRO_COMPUTE_ASIA_EAST", 0.74),
("PREMIUM_SQL_PRO_COMPUTE_ASIA_SOUTHEAST", 0.69),
("PREMIUM_SQL_PRO_COMPUTE_AUSTRALIA_CENTRAL", 0.74),
("PREMIUM_SQL_PRO_COMPUTE_AUSTRALIA_CENTRAL_2", 0.74),
("PREMIUM_SQL_PRO_COMPUTE_AUSTRALIA_EAST", 0.74),
("PREMIUM_SQL_PRO_COMPUTE_AUSTRALIA_SOUTHEAST", 0.74),
("PREMIUM_SQL_PRO_COMPUTE_BRAZIL_SOUTH", 0.85),
("PREMIUM_SQL_PRO_COMPUTE_CANADA", 0.62),
("PREMIUM_SQL_PRO_COMPUTE_CANADA_CENTRAL", 0.62),
("PREMIUM_SQL_PRO_COMPUTE_CANADA_EAST", 0.62),
("PREMIUM_SQL_PRO_COMPUTE_EUROPE_FRANCE", 0.72),
("PREMIUM_SQL_PRO_COMPUTE_EUROPE_FRANKFURT", 0.72),
("PREMIUM_SQL_PRO_COMPUTE_EUROPE_IRELAND", 0.72),
("PREMIUM_SQL_PRO_COMPUTE_EUROPE_LONDON", 0.74),
("PREMIUM_SQL_PRO_COMPUTE_EU_NORTH", 0.72),
("PREMIUM_SQL_PRO_COMPUTE_EU_WEST", 0.72),
("PREMIUM_SQL_PRO_COMPUTE_FRANCE_CENTRAL", 0.72),
("PREMIUM_SQL_PRO_COMPUTE_GERMANY_WEST_CENTRAL", 0.72),
("PREMIUM_SQL_PRO_COMPUTE_INDIA_CENTRAL", 0.61),
("PREMIUM_SQL_PRO_COMPUTE_INDIA_SOUTH", 0.69),
("PREMIUM_SQL_PRO_COMPUTE_INDIA_WEST", 0.69),
("PREMIUM_SQL_PRO_COMPUTE_JAPAN_EAST", 0.78),
("PREMIUM_SQL_PRO_COMPUTE_JAPAN_WEST", 0.78),
("PREMIUM_SQL_PRO_COMPUTE_KOREA_CENTRAL", 0.74),
("PREMIUM_SQL_PRO_COMPUTE_NORWAY_EAST", 0.72),
("PREMIUM_SQL_PRO_COMPUTE_SA_BRAZIL", 0.85),
("PREMIUM_SQL_PRO_COMPUTE_SOUTH_AFRICA_NORTH", 0.74),
("PREMIUM_SQL_PRO_COMPUTE_SWEDEN_CENTRAL", 0.72),
("PREMIUM_SQL_PRO_COMPUTE_SWITZERLAND_NORTH", 0.85),
("PREMIUM_SQL_PRO_COMPUTE_SWITZERLAND_WEST", 0.96),
("PREMIUM_SQL_PRO_COMPUTE_UAE_NORTH", 0.74),
("PREMIUM_SQL_PRO_COMPUTE_UK_SOUTH", 0.74),
("PREMIUM_SQL_PRO_COMPUTE_UK_WEST", 0.74),
("PREMIUM_SQL_PRO_COMPUTE_US_CENTRAL", 0.55),
("PREMIUM_SQL_PRO_COMPUTE_US_EAST", 0.55),
("PREMIUM_SQL_PRO_COMPUTE_US_EAST_2", 0.55),
("PREMIUM_SQL_PRO_COMPUTE_US_EAST_N_VIRGINIA", 0.55),
("PREMIUM_SQL_PRO_COMPUTE_US_EAST_OHIO", 0.55),
("PREMIUM_SQL_PRO_COMPUTE_US_GOV_ARIZONA", 0.69),
("PREMIUM_SQL_PRO_COMPUTE_US_GOV_VIRGINIA", 0.69),
("PREMIUM_SQL_PRO_COMPUTE_US_NORTH_CENTRAL", 0.55),
("PREMIUM_SQL_PRO_COMPUTE_US_SOUTH_CENTRAL", 0.55),
("PREMIUM_SQL_PRO_COMPUTE_US_WEST", 0.55),
("PREMIUM_SQL_PRO_COMPUTE_US_WEST_2", 0.55),
("PREMIUM_SQL_PRO_COMPUTE_US_WEST_3", 0.55),
("PREMIUM_SQL_PRO_COMPUTE_US_WEST_CALIFORNIA", 0.55),
("PREMIUM_SQL_PRO_COMPUTE_US_WEST_CENTRAL", 0.55),
("PREMIUM_SQL_PRO_COMPUTE_US_WEST_OREGON", 0.55),
-- STANDARD tier
("STANDARD_ALL_PURPOSE_COMPUTE", 0.4),
("STANDARD_ALL_PURPOSE_COMPUTE_DLT", 0.4),
("STANDARD_ALL_PURPOSE_COMPUTE_PHOTON", 0.4),
("STANDARD_DLT_ADVANCED_COMPUTE", 0.36),
("STANDARD_DLT_ADVANCED_COMPUTE_PHOTON", 0.36),
("STANDARD_DLT_CORE_COMPUTE", 0.2),
("STANDARD_DLT_CORE_COMPUTE_PHOTON", 0.2),
("STANDARD_DLT_PRO_COMPUTE", 0.25),
("STANDARD_DLT_PRO_COMPUTE_PHOTON", 0.25),
("STANDARD_JOBS_COMPUTE", 0.1),
("STANDARD_JOBS_COMPUTE_PHOTON", 0.1),
("STANDARD_JOBS_LIGHT_COMPUTE", 0.07);

""")

# COMMAND ----------

display(spark.read.table(f'{target_schema}.cost_lookup_table'))

# COMMAND ----------


