# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Model Monitoring Example
# MAGIC 
# MAGIC Pre-requisites:
# MAGIC - ML runtime 10.2+ 
# MAGIC 
# MAGIC [References](https://drive.google.com/drive/folders/1e7sbhf-Hp-gv395XqvIL-4FeYWxUGn20?usp=sharing):
# MAGIC - User guide on core concepts
# MAGIC - API reference for API details and guidelines 
# MAGIC 
# MAGIC Workflow:
# MAGIC 1. Read data 
# MAGIC 1. Train a model 
# MAGIC 1. Register a model and transition to "Production"
# MAGIC 1. Create a monitor
# MAGIC     - Log baseline
# MAGIC     - Log scoring data 
# MAGIC 1. Retrieve analysis of comparisons between data sets 

# COMMAND ----------

# MAGIC %pip install "https://ml-team-public-read.s3.amazonaws.com/wheels/model-monitoring/e7394149-4c48-42a3-94af-11cee8964415/databricks_model_monitoring-0.0.2-py3-none-any.whl"

# COMMAND ----------

import copy
import datetime
import json
import os
import requests
import time
from datetime import timedelta, datetime

import mlflow
import pandas as pd
import sklearn
from mlflow.models.signature import infer_signature 
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F, types as T
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

import databricks.model_monitoring as mm

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("monitor_name", "airbnb_monitor")
dbutils.widgets.text("monitor_db", "airbnb_monitor_db")
dbutils.widgets.text("model_name", "airbnb_rf")

username_prefix = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply("user").split(".")[0] + "_"
monitor_name = username_prefix + dbutils.widgets.get("monitor_name") 
monitor_db = username_prefix + dbutils.widgets.get("monitor_db") 
model_name = username_prefix + dbutils.widgets.get("model_name")

print(f"monitor_name={monitor_name}, monitor_db={monitor_db}, model_name={model_name}")

# COMMAND ----------

# MAGIC %md ## Helper methods
# MAGIC 
# MAGIC We define a few methods, mostly for cleanup, to make the notebook easy to rerun. You are not expected to use these methods in a normal setup.

# COMMAND ----------

def cleanup_existing_monitor(monitor_db, model_name, monitor_name):
  try:
    mm_info = mm.get_monitor_info(model_name=model_name, monitor_name=monitor_name)
  except:
    print("No existing monitor to clean up...")
    return
  print(f"Deleting existing monitor for {model_name}")
  mm.delete_monitor(model_name=model_name, monitor_name=monitor_name)

def cleanup_registered_model(registry_model_name):
  """
  Utilty function to delete a registered model in MLflow model registry.
  To delete a model in the model registry all model versions must first be archived.
  This function thus first archives all versions of a model in the registry prior to
  deleting the model
  
  :param registry_model_name: (str) Name of model in MLflow model registry
  """
  client = MlflowClient()

  filter_string = f'name="{registry_model_name}"'

  model_versions = client.search_model_versions(filter_string=filter_string)
  
  if len(model_versions) > 0:
    print(f"Deleting following registered model: {registry_model_name}")
    
    # Move any versions of the model to Archived
    for model_version in model_versions:
      try:
        model_version = client.transition_model_version_stage(name=model_version.name,
                                                              version=model_version.version,
                                                              stage="Archived")
      except mlflow.exceptions.RestException:
        pass

    client.delete_registered_model(registry_model_name)
    
  else:
    print("No registered models to delete")    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read data

# COMMAND ----------

# Read data and add an id for eventual label logging.
df = spark.read.format("parquet").load("dbfs:/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet/").withColumn("id", F.expr("uuid()"))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature selection and split data
# MAGIC 
# MAGIC Below, we select a subset of features with different both numerical and categorical types.
# MAGIC We use these features to predict `price` of Airbnb listings. We are going to build a Random Forest regression model subsequently.
# MAGIC 
# MAGIC Furthermore, we are going to create two `test_df`s, i.e. `test_df1` and `test_df2`
# MAGIC - We intentionally create drifts for some of the most important features in `test_df2` later. 
# MAGIC 
# MAGIC Workflow and Scenario Simulation:
# MAGIC - Train a model on `train_df`
# MAGIC - Generate predictions on `test_df1`
# MAGIC - Identify the top 3 important features in the model 
# MAGIC - Create drifts in those 3 features for `test_df2`
# MAGIC - Use the model trained on `train_df` and generate predictions on the drifted `test_df2`
# MAGIC - Compare data statistics and model performances on `test_df1` vs. `test_df2`

# COMMAND ----------

select_cols_list = ["bedrooms", "neighbourhood_cleansed", "accommodates", "cancellation_policy", "beds", "host_is_superhost", "property_type", "minimum_nights", "bathrooms", "host_total_listings_count", "number_of_reviews", "review_scores_value", "review_scores_cleanliness"]

### Keeping the target_col as string, as opposed to a list, because we need to pass this to the model definition later
target_col = "price"

train_df, test_df1, test_df2 = df.select(*select_cols_list+[target_col]+["id"]).randomSplit(weights=[0.5, 0.25, 0.25], seed=42)

print(train_df.count(), test_df1.count(), test_df2.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Month 1: Train a random forest model and generate predictions on `test_df1`

# COMMAND ----------

# Define the training datasets
X_train = train_df.drop("id", target_col)
X_train_pd = X_train.toPandas()
y_train = train_df.select(target_col)
y_train_pd = y_train.toPandas().values.ravel()

# Define categorical preprocessor
categorical_cols = [field for (field, data_type) in X_train.dtypes if data_type == "string"]
one_hot_pipeline = Pipeline(steps=[("one_hot_encoder", OneHotEncoder(handle_unknown="ignore"))])
preprocessor = ColumnTransformer([("onehot", one_hot_pipeline, categorical_cols)], remainder="passthrough", sparse_threshold=0)

# Define the model
skrf_regressor = RandomForestRegressor(
  bootstrap=True,
  criterion="mse",
  max_depth=9,
  max_features=0.5,
  min_samples_leaf=0.1,
  min_samples_split=0.15,
  n_estimators=36,
  random_state=158334362,
)

model = Pipeline([
    ("preprocessor", preprocessor),
    ("regressor", skrf_regressor),
])

# Enable automatic logging of input samples, metrics, parameters, and models
mlflow.sklearn.autolog(log_input_examples=True, silent=True)

with mlflow.start_run(run_name="random_forest_regressor") as mlflow_run:
    model.fit(X_train_pd, y_train_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register model on model registry and transition to Production

# COMMAND ----------

# Register model to MLflow Model Registry
run_id = mlflow_run.info.run_id
cleanup_existing_monitor(monitor_db, model_name, monitor_name)
cleanup_registered_model(model_name)
model_version = mlflow.register_model(model_uri=f"runs:/{run_id}/model", name=model_name)

# COMMAND ----------

client = MlflowClient()
    
model_stage = "Production"
info = client.transition_model_version_stage(
  name=model_version.name,
  version=model_version.version,
  stage=model_stage,
  archive_existing_versions=True
)
print(info)

# COMMAND ----------

from shap import KernelExplainer, summary_plot

# Sample data to use for feature importance
train_sample = X_train_pd.sample(n=min(100, len(X_train_pd.index)))
examples = X_train_pd.sample(n=5)

# Use Kernel SHAP to explain feature importance on the selected example
predict = lambda x: model.predict(pd.DataFrame(x, columns=X_train.columns))
explainer = KernelExplainer(predict, train_sample, link="identity")
shap_values = explainer.shap_values(examples, l1_reg=False)
summary_plot(shap_values, examples)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up model monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC You enable monitoring on a model by calling `create_monitor`. The call specifies a few parameters, including:
# MAGIC <br>
# MAGIC - `logging_schema`: The schema that contains all fields to track in the Logging Table.
# MAGIC - `prediction_col` and `label_col`: The columns in `logging_schema` that correspond to the output of the model and the corresponding ground truth (respectively). These names are primarily used in the output of the analysis, to facilitate access to the corresponding statistics (e.g., distribution of predictions or the ground truth labels).
# MAGIC - `granularities`: It determines how to group data for the purpose of analysis based on the corresponding scoring timestamp. If `1 day`, then the analyses will aggregate data in 1-day windows. If you specify multiple granularities, i.e. `1 day`, `1 month`, then the analyses will include statistics for every 1-day and 1-month window.
# MAGIC - `slicing_exprs`: It determines a list of columns to slice the data. If you provide one column `a`, the analysis computed will also include statistics grouped by the unique values of `a`. For instance, when `a == 1`, the corresponding mean value is `x`, the number of nulls is `y`, etc.
# MAGIC - `id_cols`: The column names in `logging_schema` that uniquely identify a row in order to link ground-truth labels that arrive later.
# MAGIC 
# MAGIC Please see the API reference for the full documentation.

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {monitor_db}")
print(f"Created database {monitor_db}.")

print(f"Creating {monitor_name} for {model_name}")

logging_schema = copy.deepcopy(train_df.schema)
logging_schema.add("prediction", T.DoubleType(), False)

mm_info = mm.create_monitor(model_name=model_name,
                            model_type="regressor",
                            logging_schema=logging_schema,
                            prediction_col="prediction",
                            label_col="price",
                            granularities=["1 day"], 
                            id_cols=["id"], 
                            slicing_exprs=["neighbourhood_cleansed", "cancellation_policy", "accommodates"],
                            database_name=monitor_db,
                            monitor_name=monitor_name
                           )
print("Created....")
print(mm_info)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Examining the monitor
# MAGIC You can view the created monitor on the registered model page.

# COMMAND ----------

monitor_ui_link = f"""Monitor Created: <a href="#mlflow/models/{mm_info.model_name}/monitoring">{monitor_name}</a>"""
displayHTML(monitor_ui_link)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log baseline data
# MAGIC 
# MAGIC - Goal: Compute drift statistics between upcoming scoring data and the data used to train the model.
# MAGIC 
# MAGIC Note:
# MAGIC - Notice that if you were to re-run the cell below, the underlying data version is updated as well. 
# MAGIC 
# MAGIC Use case for this logged_id:
# MAGIC - If the user wants to undo a logging event - we can't use Delta's time travel because other consumers may have logged data in between, but they can always delete all rows corresponding to the batch ID. 

# COMMAND ----------

model_uri = f"models:/{model_name}/{model_stage}"
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type="double")

features = list(X_train.columns)
train_df_with_pred = train_df.withColumn("prediction", loaded_model(*features))
display(train_df_with_pred)

# COMMAND ----------

logged_data_id = mm.log_baseline_data(model_name=model_name,
                                      model_stage=model_stage,
                                      baseline_data=train_df_with_pred, 
                                      monitor_name=monitor_name) 
print(logged_data_id)

# COMMAND ----------

# MAGIC %md
# MAGIC If you need to update your baseline for a specific model version, (e.g., because you made an error) then you can simply call `log_baseline_data()` again with the correct baseline.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log scored data 
# MAGIC 
# MAGIC Imagine that up to this point, the model owner has been responsible for training a model and logging a baseline. 
# MAGIC From now onwards, the model consumer is using the model and triggering analysis jobs. 
# MAGIC 
# MAGIC Here, the model owner logs the scored data to compare the distributions between `test_df1` and `test_df2` later.

# COMMAND ----------

# Compute predictions
model_uri = f"models:/{model_name}/{model_stage}"
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type="double")
pred_df1 = test_df1.withColumn("prediction", loaded_model(*features))


## Simulate different batches of scoring data using timestamp
### first timestamp is two days ago
timestamp1 = (datetime.now() - timedelta(2)).timestamp()
pred_df1 = pred_df1.withColumn("scoring_timestamp", F.lit(timestamp1).cast("timestamp"))

# COMMAND ----------

mm.log_scored_data(
  model_name=model_name,
  model_stage=model_stage,  
  scored_data=pred_df1,
  monitor_name=monitor_name,
  timestamp_col="scoring_timestamp"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log labels
# MAGIC 
# MAGIC We assume that your ground truth labels will come later, so you can choose to log them when they arrive.

# COMMAND ----------

updated_count = mm.log_labels(
  model_name=model_name,
  monitor_name=monitor_name,
  labeled_data=pred_df1.select("id", target_col),
  return_updated_count=True
  )
print(f"{updated_count} of rows were annotated with labels.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run analysis job
# MAGIC Now that we have set up the monitor, logged the baseline, and the first batch of scored_data, we can run the analysis job. 
# MAGIC 
# MAGIC **NOTE:** The analysis job is configured to run on a daily schedule, but is paused by default. This is by design, so you can inspect the execution parameters of the job (e.g. the cluster configuration used by DLT) before you resume its schedule. Users can head to the job page (linked below) to resume or change the schedule. 
# MAGIC 
# MAGIC For the purpose of this demo, since we don't want to wait for a day for the analysis job to be kicked off, we can trigger it ourselves using the `run_analysis` API.

# COMMAND ----------

analysis_job_link = f"""Inspect the <a href="#job/{mm_info.analysis_job_id}">analysis job.</a>"""
displayHTML(analysis_job_link)

mm.run_analysis(
    model_name=model_name,
    monitor_name=monitor_name,
    await_completion=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect the analysis tables

# COMMAND ----------

# MAGIC %md
# MAGIC Notice that the cell below shows that within the monitor_db, there are four other tables:
# MAGIC 
# MAGIC 1. logging
# MAGIC 2. analysis_metrics
# MAGIC 3. drift_metrics
# MAGIC 4. aggregate_base
# MAGIC 
# MAGIC The first table (logging) is a staging area for the data logged by the previous `log_scored_data`, `log_labels` and `log_baseline_data` calls. The next two tables (analysis_metrics and drift_metrics) record the eventual output of the analysis job and are relevant for this part of the notebook. The last table (aggregate_base) is created for internal optimization purpose only and can be ignored for now.

# COMMAND ----------

display(spark.sql(f"SHOW TABLES FROM {monitor_db}"))

# COMMAND ----------

# MAGIC %md
# MAGIC First, let's look at the `analysis_metrics` table.

# COMMAND ----------

analysis_df = spark.sql(f"SELECT * FROM {mm_info.analysis_metrics_table_name}")
display(analysis_df)

# COMMAND ----------

# MAGIC %md
# MAGIC You can see that for every column, the analysis table differentiates baseline data from scoring data and generates analyses based on:
# MAGIC - window
# MAGIC - model stage
# MAGIC - model version
# MAGIC - slice key
# MAGIC 
# MAGIC We can also gain insight into basic summary statistics
# MAGIC - percent_distinct
# MAGIC - data_type
# MAGIC - min
# MAGIC - max
# MAGIC - etc.

# COMMAND ----------

display(analysis_df.filter("column_name='cancellation_policy'"))

# COMMAND ----------

# MAGIC %md
# MAGIC Based on the drift table below, we are able to tell the shifts / changes between the `train_df` and `test_df1`. 

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {mm_info.drift_metrics_table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Since this comparison of `test_df` is made against the baseline `train_df`, we can see that `drift_type = "BASELINE"`. We will see another drift type, called `"CONSECUTIVE"`, when we have multiple batches of `scored_data` to compare.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {mm_info.drift_metrics_table_name}").groupby("drift_type").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect the dashboard
# MAGIC 
# MAGIC Click on the dashboard link below and click "Run All" to view the visualizations. Note that this dashboard will be less informative in the beginning since it only has one data point for the first batch of `scored_data`

# COMMAND ----------

dashboard_link=f"""Examine generated <a href="#workspace/{mm_info.dashboard_notebook_path}">analysis dashboard</a>."""
displayHTML(dashboard_link)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Month 2 - Simulate drifts on `test_df2` and generate predictions
# MAGIC 
# MAGIC Now, let's finally simulate some distributional changes in `test_df2` so we can look at more interesting analyses!
# MAGIC   - We are going to modify the distributions for these features: `neighbourhood_cleansed`, `cancellation_policy`, `accommodates`

# COMMAND ----------

remove_top_neighbourhood_list = ["South of Market", "Western Addition", "Downtown/Civic Center", "Bernal Heights", "Castro/Upper Market"]

test_df2_simulated = (test_df2
                      ### Remove top neighbourhoods to simulate change in distribution
                      .withColumn("neighbourhood_cleansed", 
                                  F.when(F.col("neighbourhood_cleansed").isin(remove_top_neighbourhood_list), "Mission")
                                  .otherwise(F.col("neighbourhood_cleansed")))
                      ### Introduce a new value to a categorical variable
                      .withColumn("cancellation_policy", 
                                  F.when(F.col("cancellation_policy")=="flexible", "super flexible")
                                  .otherwise(F.col("cancellation_policy")))
                      ### Replace all accommodates with 1
                      .withColumn("accommodates", F.lit(1).cast("double"))
                     )
display(test_df2_simulated)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load model and generate predictions on `test_df2_simulated`

# COMMAND ----------

loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type="double")
pred_df2 = test_df2_simulated.withColumn("prediction", loaded_model(*features))

# Simulate scoring that happened yesterday
timestamp2 = (datetime.now() - timedelta(1)).timestamp()
pred_df2 = pred_df2.withColumn("scoring_timestamp", F.lit(timestamp2).cast("timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log scored_data

# COMMAND ----------

mm.log_scored_data(
  model_name=model_name,
  model_stage=model_stage,
  scored_data=pred_df2,
  monitor_name=monitor_name,
  timestamp_col="scoring_timestamp"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log labels

# COMMAND ----------

updated_count2 = mm.log_labels(
  model_name=model_name,
  monitor_name=monitor_name,
  labeled_data=pred_df2.select("id", target_col),
  return_updated_count=True
  )
print(f"{updated_count2} of rows were annotated with labels.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run analysis job
# MAGIC The next scheduled run of the analysis job will pick up the new data and update the analysis metrics. Again, we can trigger a manual run through the `run_analysis` API.

# COMMAND ----------

mm.run_analysis(
    model_name=model_name,
    monitor_name=monitor_name,
    await_completion=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect tables
# MAGIC 
# MAGIC Notice that after we updated the scoring data with a second batch, there are now several new rows in the analysis tables which correspond to the statistics per:
# MAGIC - input column
# MAGIC - input window
# MAGIC - model version
# MAGIC - model stage
# MAGIC - different values of the slicing expressions
# MAGIC 
# MAGIC In particular, we can inspect the statistic for the total count of input rows in each scoring batch and in the baseline data. Notice that the respective counts correspond to `train_df`, `test_df1` and `test_df2`. In `test_df2`, the count has dropped due to our `string_indexer`'s `handleInvalid = "skip"` handling. 
# MAGIC ```
# MAGIC string_indexer = StringIndexer(inputCols=categorical_cols, outputCols=index_output_cols, handleInvalid="skip")
# MAGIC ```

# COMMAND ----------

display(spark.sql(f"SELECT DISTINCT window, log_type, count FROM {mm_info.analysis_metrics_table_name} WHERE column_name=':table' AND slice_key IS NULL"))

# COMMAND ----------

# MAGIC %md
# MAGIC In the analysis metrics table below, we can view the columns' overall statistics, including percent of distinct values, median, etc. 

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {mm_info.analysis_metrics_table_name} WHERE COLUMN_NAME IN ('accommodates', 'cancellation_policy', 'neighbourhood_cleansed') AND slice_key is NULL AND model_stage = 'Production'").drop("granularity", "model_stage", "log_type", "slice_key", "slice_value"))

# COMMAND ----------

# MAGIC %md
# MAGIC Pivoting to inspecting drift metrics table, we can view the drift metrics calculated between batches of data. Since now we have different batches of scored data, we can see the `drift_type == "CONSECUTIVE"` here. Note that only the latest window has `CONSECUTIVE` drift, since it can be compared to the previous window. However, both windows have `BASELINE` drift computed.

# COMMAND ----------

display(spark.sql(f"SELECT DISTINCT window, drift_type FROM {mm_info.drift_metrics_table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can inspect the drift metrics in more detail. In the following query, `window_cmp` represents the window against which `window` is being compared. Also keep in mind that our setup simulates the following two scoring batches:
# MAGIC <br>
# MAGIC - test_df1 (pred_df) = date for two days ago
# MAGIC - test_df2 (pred_df2) = yesterday's date
# MAGIC <br>
# MAGIC There is also the baseline data which corresponds to the training data of the model.
# MAGIC 
# MAGIC We can readily idenfity drifts using the table below! For instance, we can see the following drifts in column `accommodates` due to our simulation:
# MAGIC 1. pred_df vs. pred_df2  with KS statistic of 0.905 at p-value = 0
# MAGIC 2. pred_df2 vs. train_df_with_pred with KS statistic of 0.915 at p-value of 0
# MAGIC <br>
# MAGIC <br>
# MAGIC Unsurprisingly, the following comparison has no drift:
# MAGIC 1. train_df_with_pred vs test_df1 with KS statistic of 0.01 at p-value > 0.05

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {mm_info.drift_metrics_table_name} WHERE COLUMN_NAME IN ('accommodates', 'cancellation_policy', 'neighbourhood_cleansed') AND slice_key is NULL AND model_stage = 'Production'").drop("granularity", "model_version",  "model_stage", "slice_key", "slice_value", "non_null_columns_delta"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect dashboard
# MAGIC 
# MAGIC It is also possible to do similar inspections using the builtin dashboard. Click through the link and the click on "Run All" to update the dashboard.

# COMMAND ----------

displayHTML(dashboard_link)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alerting
# MAGIC 
# MAGIC You can set up alerts on the metrics in the analysis and drift tables using [Databricks SQL alerts](https://docs.databricks.com/sql/user/alerts/index.html). You need to first write a SQL query that contains columns you would like to create an alert on and then navigate to the SQL alerts page to create the alert.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up artifacts
# MAGIC 
# MAGIC Note: The order of deletion is important. We need to delete the monitor first, before we delete the model. 
# MAGIC 
# MAGIC The model registry hosts the monitor; if we delete the model first, you will see an error that it won't be able to find any monitors, even though the artifacts created still exist.

# COMMAND ----------

# print("Deleting the monitor...")
# mm.delete_monitor(model_name=model_name, monitor_name=monitor_name, purge_tables=True)

# COMMAND ----------

# print("Deleting the model...")
# cleanup_registered_model(model_name)
