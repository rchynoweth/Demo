# Databricks notebook source
# MAGIC %md
# MAGIC # Nested Models Example
# MAGIC
# MAGIC This notebook shows how best to train and register models that are nested in nature. For example, assume we have 1000 skus which we are generating forecasts, there should be 1 model registered with 1000 sub models. 
# MAGIC
# MAGIC This notebook was created using DBR 14.3LTS Machine Learning. For more information, please refer to this [MLflow Documentation](https://mlflow.org/docs/latest/traditional-ml/hyperparameter-tuning-with-child-runs/part1-child-runs.html) on parent/child runs. 

# COMMAND ----------

import mlflow
from prophet import Prophet
import pandas as pd
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.types import * 

# COMMAND ----------

exp_name = "/Users/ryan.chynoweth@databricks.com/rac_nested_forecasting"
exp = mlflow.set_experiment(experiment_name=exp_name)
exp_id = exp.experiment_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a fake dataset
# MAGIC 10 skus and 6 months of daily data

# COMMAND ----------

# Define the number of unique SKUs
num_skus = 10

# Generate a date range for 6 months (assuming 30 days per month)
num_days = 6 * 30
date_range = pd.date_range(start='2024-01-01', periods=num_days, freq='D')

# Generate random 'y' values for each day and SKU combination
random_y = np.random.randint(1, 100, size=(num_days, num_skus))

# Generate SKUs
skus = [f'SKU_{i+1}' for i in range(num_skus)]

# Create a DataFrame with repeated date range for each SKU
dates_repeated = np.repeat(date_range, num_skus)

# Create a DataFrame with repeated SKUs for each day
skus_repeated = np.tile(skus, num_days)

# Flatten the random_y values to match the repeated structure
random_y_flattened = random_y.flatten()

# Create the DataFrame
df_6_months = pd.DataFrame({
    'ds': dates_repeated,
    'y': random_y_flattened,
    'sku': skus_repeated
})



# Print the dataset
spark_df = spark.createDataFrame(df_6_months)
display(spark_df)

# COMMAND ----------

forecast_result_schema = StructType([
      StructField('ds',DateType()),
      # StructField('workspace_id', StringType()),
      StructField('sku',StringType()),
      StructField('y',FloatType()),
      StructField('yhat',FloatType()),
      StructField('yhat_upper',FloatType()),
      StructField('yhat_lower',FloatType())
      ])
    

# COMMAND ----------

def create_forecast(run_id):
  def generate_forecast(self, history_pd):
    sku = history_pd['sku'].iloc[0]

    with mlflow.start_run(nested=True, experiment_id=exp_id, tags={'sku': sku}, run_id=run_id):
      history_pd = history_pd.dropna()
      print(f"------- SKU: {sku}")
      
      # train and configure the model
      model = Prophet( interval_width=0.85 )
      model.fit( history_pd )

      # make predictions
      future_pd = model.make_future_dataframe(
        periods=30, 
        freq='d', 
        include_history=True
        )
      mlflow.prophet.log_model(model, artifact_path=f"prophet_model_{sku}")
      forecast_pd = model.predict( future_pd )  
      
      # ASSEMBLE EXPECTED RESULT SET
      # --------------------------------------
      # get relevant fields from forecast
      f_pd = forecast_pd[ ['ds','yhat', 'yhat_upper', 'yhat_lower'] ].set_index('ds')
      
      # get relevant fields from history
      h_pd = history_pd[['ds','sku','y']].set_index('ds')
      
      # join history and forecast
      results_pd = f_pd.join( h_pd, how='left' )
      results_pd.reset_index(level=0, inplace=True)
      
      # get sku from incoming data set
      results_pd['sku'] = history_pd['sku'].iloc[0]

      return results_pd[ ['ds', 'sku', 'y', 'yhat', 'yhat_upper', 'yhat_lower'] ]  
  return generate_forecast

# COMMAND ----------



# COMMAND ----------


with mlflow.start_run(experiment_id=exp_id) as run:
      run_id = run.info.run_id

      (spark_df
       .groupBy('sku')
       .applyInPandas(create_forecast(run_id), schema=forecast_result_schema)
       .write
       .mode('overwrite')
       .saveAsTable('rac_demo_catalog.default.output_prophet_test')
      )

# COMMAND ----------


