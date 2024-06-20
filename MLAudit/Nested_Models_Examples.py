# Databricks notebook source
# MAGIC %md
# MAGIC # Nested Models Example
# MAGIC
# MAGIC This notebook shows how best to train and register models that are nested in nature. For example, assume we have 10 skus which we are generating forecasts, there should be 1 model registered with 10 sub models. 
# MAGIC
# MAGIC This notebook was created using DBR 14.3LTS Machine Learning. For more information, please refer to this [MLflow Documentation](https://mlflow.org/docs/latest/traditional-ml/hyperparameter-tuning-with-child-runs/part1-child-runs.html) on parent/child runs. 

# COMMAND ----------

import mlflow
from prophet import Prophet
import pandas as pd
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.types import * 

mlflow.set_registry_uri("databricks-uc")


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

# expected schema output
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

# wrapping the generate_forecasts allows us to nest the child
# models into a single experiment run for production deployment
# We need to pass run_id to the function to keep the 
# parent/child relationship 

def create_forecast(run_id):
  def generate_forecast(self, history_pd):
    sku = history_pd['sku'].iloc[0]

    with mlflow.start_run(nested=True, experiment_id=exp_id, run_id=run_id):
      history_pd = history_pd.dropna()
      
      # train and configure the model
      model = Prophet( interval_width=0.85 )
      model.fit( history_pd )

      # make predictions
      future_pd = model.make_future_dataframe(
        periods=30, 
        freq='d', 
        include_history=True
        )
      
      # log model to workspace experiment with unique name
      # the unique name allows us to look up the appropriate model later on 
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


# Execute the training run 
with mlflow.start_run(experiment_id=exp_id) as run:
      run_id = run.info.run_id
      # group by sku and train a model for each sku
      (spark_df
       .groupBy('sku')
       .applyInPandas(create_forecast(run_id), schema=forecast_result_schema)
       .write
       .mode('overwrite')
       .saveAsTable('rac_demo_catalog.default.output_prophet_test')
      )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examine the ML flow Experiment
# MAGIC
# MAGIC Navigating to the experiment you will see that each of the 10 models are logged to the same experiment run.  
# MAGIC
# MAGIC <img src="https://github.com/rchynoweth/Demo/blob/cf51bf92d8ed07df7cce73956022416326ff4778/MLAudit/ModelList.png?raw=true"/> 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Models in Batch Inference

# COMMAND ----------

# get some test data
test_df = df_6_months[df_6_months['sku'] == 'SKU_1']
test_df.head()

# COMMAND ----------

# get the latest run we executed previously
latest_exp_run = mlflow.search_runs(experiment_ids=[exp_id]).sort_values('start_time', ascending=False).iloc[0]
run = mlflow.get_run(latest_exp_run.run_id)

# COMMAND ----------

# Python wrapper class 
import mlflow 

class BatchModel(mlflow.pyfunc.PythonModel):
  def __init__(self, run_id):
    self.client = mlflow.MlflowClient()
    self.run = mlflow.get_run(run_id)
    self.artifact_path = self.run.info.artifact_uri
    self.models = {}
    self.load_models()

  ## NOTICE - 
  # If you have a LOT of models 
  # it may be too big to fit into memory and you may want to 
  # parse it out into smaller chunks 
  def load_models(self):
    # get all the models
    self.artifacts = self.client.list_artifacts(run.info.run_id)
    # for all models in the run, we want to load them into a dict
    for a in self.artifacts:
      self.models[a.path] = mlflow.prophet.load_model(f"{self.artifact_path}/{a.path}")

  def predict(self, model_input):
    # get the incoming data sku
    sku = model_input['sku'].iloc[0]
    # format the model name for look up
    model_name = f"prophet_model_{sku}"
    # load the model
    model = self.models.get(model_name)
    # forecast the data
    forecast_pd = model.predict( model_input )
    # return the output
    return forecast_pd


# COMMAND ----------

# create wrapper model object
model = BatchModel(run.info.run_id)

# COMMAND ----------

# make prediction
output = model.predict(test_df)
display(output)

# COMMAND ----------

# log wrapper model
model_name = "custom_model"
with mlflow.start_run() as register_run:
  mlflow.pyfunc.log_model(
    artifact_path=model_name, 
    python_model=model,
    signature=mlflow.models.infer_signature(test_df, output)
    )

# COMMAND ----------

model_uri = f'runs:/{register_run.info.run_id}/{model_name}'
reg = mlflow.register_model(model_uri=model_uri, name="rac_demo_catalog.default.custom_prophet_model", await_registration_for=600) 


# COMMAND ----------


