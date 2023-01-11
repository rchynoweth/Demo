# Databricks notebook source
# MAGIC %md
# MAGIC # Model Training with MLFlow
# MAGIC 
# MAGIC 
# MAGIC It is a best practice to also log a model signature and input example. This allows for better schema checks and, therefore, integration with automated deployment tools.
# MAGIC 
# MAGIC **Signature**
# MAGIC * A model signature is just the schema of the input(s) and the output(s) of the model
# MAGIC * We usually get this with the `infer_schema` function
# MAGIC 
# MAGIC **Input Example**
# MAGIC * This is simply a few example inputs to the model 
# MAGIC * This will be converted to JSON and stored in our MLflow run
# MAGIC * It integrates well with MLflow model serving
# MAGIC 
# MAGIC In general, logging a model with these looks like `.log_model(model, model_name, signature=signature, input_example=input_example)`.
# MAGIC 
# MAGIC Let's look at an example, where we create a `sklearn` Random Forest Regressor model and log it with the signature and input example.

# COMMAND ----------

import pandas as pd
from sklearn.model_selection import train_test_split
from databricks import feature_store
import mlflow 
from mlflow.models.signature import infer_signature
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

# COMMAND ----------

dbutils.widgets.text("DatabaseName", "rac_demo_db")
dbutils.widgets.text("UserName", "ryan.chynoweth@databricks.com")

# COMMAND ----------

database_name = dbutils.widgets.get('DatabaseName')
user_name = dbutils.widgets.get('UserName')

# COMMAND ----------

table_location = '/users/{}/databases/{}'.format(user_name, database_name) 
spark.sql("CREATE DATABASE IF NOT EXISTS {} LOCATION '{}'".format(dbutils.widgets.get('DatabaseName'), table_location))
spark.sql("USE {}".format(dbutils.widgets.get("DatabaseName")))

# COMMAND ----------

table_name = '{}.rac_clean_airbnb_ml_data'.format(database_name) 

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

# Displays most recent table
feature_df = fs.read_table(
  name=table_name
)

display(feature_df)

pdf = feature_df.toPandas()
X = pdf.drop("price", axis=1)
y = pdf["price"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# COMMAND ----------

## 
## TRAIN THE MODEL
## 

with mlflow.start_run(run_name="Signature Example") as run:
  rf = RandomForestRegressor(random_state=42)
  rf_model = rf.fit(X_train, y_train)
  mse = mean_squared_error(rf_model.predict(X_test), y_test)
  mlflow.log_metric("mse", mse)
  
  # Log the model with signature and input example
  signature = infer_signature(X_train, pd.DataFrame(y_train))
  input_example = X_train.head(3)
  mlflow.sklearn.log_model(rf_model, "rf_model", signature=signature, input_example=input_example)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nested Runs
# MAGIC 
# MAGIC A useful organizational tool provided by MLflow is nested runs. Nested runs allow for parent runs and child runs in a tree structure. In the MLflow UI, you can click on a parent run to expand it and see the child runs. 
# MAGIC 
# MAGIC Example applications: 
# MAGIC * In **hyperparameter tuning**, you can nest all associated model runs under a parent run to better organize and compare hyperparameters. 
# MAGIC * In **parallel training** many models such as IoT devices, you can better aggregate the models. More information on this can be found [here]("https://databricks.com/blog/2020/05/19/manage-and-scale-machine-learning-models-for-iot-devices.html").
# MAGIC * In **iterative training** such as neural networks, you can checkpoint results after `n` epochs to save the model and related metrics.

# COMMAND ----------

with mlflow.start_run(run_name="Nested Example") as run:
  # Create nested run with nested=True argument
  with mlflow.start_run(run_name="Child 1", nested=True):
    mlflow.log_param("run_name", "child_1")
    
  with mlflow.start_run(run_name="Child 2", nested=True):
    mlflow.log_param("run_name", "child_2")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hyperparameter Tuning
# MAGIC 
# MAGIC One of the most common use cases for nested runs is hyperparameter tuning. For example, when running **HyperOpt** with SparkTrials on Databricks, it will automatically track the candidate models, parameters, etc as child runs in the MLflow UI.
# MAGIC 
# MAGIC Hyperopt allows for efficient hyperparameter tuning and now integrates with Apache Spark via:
# MAGIC 
# MAGIC * **Trials:** Sequential training of single-node or distributed ML models (e.g. MLlib)
# MAGIC * **SparkTrials:** Parallel training of single-node models (e.g. sklearn). The amount of parallelism is controlled via the `parallelism` parameter. 
# MAGIC 
# MAGIC Let's try using HyperOpt with SparkTrials to find the best sklearn random forest model. 
# MAGIC 
# MAGIC Check out this blog by Sean Owen on [How (Not) to Tune Your Model With Hyperopt](https://databricks.com/blog/2021/04/15/how-not-to-tune-your-model-with-hyperopt.html).

# COMMAND ----------

from hyperopt import fmin, tpe, hp, SparkTrials

# Define objective function
def objective(params):
  model = RandomForestRegressor(n_estimators=int(params["n_estimators"]), 
                                max_depth=int(params["max_depth"]), 
                                min_samples_leaf=int(params["min_samples_leaf"]),
                                min_samples_split=int(params["min_samples_split"]))
  model.fit(X_train, y_train)
  pred = model.predict(X_train)
  score = mean_squared_error(pred, y_train)
  
  # Hyperopt minimizes score, here we minimize mse. 
  return score

# COMMAND ----------

from hyperopt import SparkTrials

# Define search space
search_space = {"n_estimators": hp.quniform("n_estimators", 100, 500, 5),
                "max_depth": hp.quniform("max_depth", 5, 20, 1),
                "min_samples_leaf": hp.quniform("min_samples_leaf", 1, 5, 1),
                "min_samples_split": hp.quniform("min_samples_split", 2, 6, 1)}

# Set parallelism (should be order of magnitude smaller than max_evals)
spark_trials = SparkTrials(parallelism=2)

with mlflow.start_run(run_name="Hyperopt"):
  argmin = fmin(
    fn=objective,
    space=search_space,
    algo=tpe.suggest,
    max_evals=16,
    trials=spark_trials)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Advanced Artifact Tracking
# MAGIC 
# MAGIC In addition to the logging of artifacts you have already seen, there are some more advanced options. 
# MAGIC 
# MAGIC We will now look at: 
# MAGIC * [mlflow.shap](https://www.mlflow.org/docs/latest/python_api/mlflow.shap.html#mlflow.shap): Automatically calculates and logs Shapley feature importance plots
# MAGIC * [mlflow.log_figure](https://mlflow.org/docs/latest/python_api/mlflow.html#mlflow.log_figure): Logs matplotlib and plotly plots

# COMMAND ----------

import matplotlib.pyplot as plt

with mlflow.start_run(run_name="Feature Importance Scores"):
  # Generate and log SHAP plot for first 5 records
  mlflow.shap.log_explanation(rf.predict, X_train[:5])
  
  # Generate feature importance plot
  feature_importances = pd.Series(rf_model.feature_importances_, index=X.columns)
  fig, ax = plt.subplots()
  feature_importances.plot.bar(ax=ax)
  ax.set_title("Feature importances using MDI")
  ax.set_ylabel("Mean decrease in impurity")
  
  # Log figure
  mlflow.log_figure(fig, "feature_importance_rf.png")

# COMMAND ----------


