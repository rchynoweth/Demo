# Databricks notebook source
# MAGIC %pip install --upgrade "mlflow-skinny[databricks]"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

spark.sql('use catalog rac_demo_catalog')

# COMMAND ----------

spark.sql('use rac_demo_db')

# COMMAND ----------

import mlflow
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load data into UC

# COMMAND ----------

# Read the data from CSV files
white_wine = spark.read.csv("dbfs:/databricks-datasets/wine-quality/winequality-white.csv", sep=';', header=True)
red_wine = spark.read.csv("dbfs:/databricks-datasets/wine-quality/winequality-red.csv", sep=';', header=True)
 
# To clean up the data, remove the spaces from the column names, since parquet doesn't allow them
for c in white_wine.columns:
    white_wine = white_wine.withColumnRenamed(c, c.replace(" ", "_"))
for c in red_wine.columns:
    red_wine = red_wine.withColumnRenamed(c, c.replace(" ", "_"))
 
# Write to tables in Unity Catalog
spark.sql("drop table if exists ml.default.white_wine")
spark.sql("drop table if exists ml.default.red_wine")
white_wine.write.saveAsTable("white_wine")
red_wine.write.saveAsTable("red_wine")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from UC

# COMMAND ----------

import numpy as np
import pandas as pd
import sklearn.datasets
import sklearn.metrics
import sklearn.model_selection
import sklearn.ensemble
 
from hyperopt import fmin, tpe, hp, SparkTrials, Trials, STATUS_OK
from hyperopt.pyll import scope

# COMMAND ----------

# Load data from Unity Catalog as Pandas dataframes
white_wine = spark.read.table("white_wine").toPandas()
red_wine = spark.read.table("red_wine").toPandas()
 
# Add Boolean fields for red and white wine
white_wine['is_red'] = 0.0
red_wine['is_red'] = 1.0
data_df = pd.concat([white_wine, red_wine], axis=0)
 
# Define classification labels based on the wine quality
data_labels = data_df['quality'].astype('int') >= 7
data_df = data_df.drop(['quality'], axis=1)
 
# Split 80/20 train-test
X_train, X_test, y_train, y_test = sklearn.model_selection.train_test_split(
  data_df,
  data_labels,
  test_size=0.2,
  random_state=1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train and Load Models

# COMMAND ----------

# Enable MLflow autologging for this notebook
mlflow.autolog()

# COMMAND ----------

with mlflow.start_run(run_name='gradient_boost') as run:
  model = sklearn.ensemble.GradientBoostingClassifier(random_state=0)
  
  # Models, parameters, and training metrics are tracked automatically
  model.fit(X_train, y_train)
 
  predicted_probs = model.predict_proba(X_test)
  roc_auc = sklearn.metrics.roc_auc_score(y_test, predicted_probs[:,1])
  
  # The AUC score on test data is not automatically logged, so log it manually
  mlflow.log_metric("test_auc", roc_auc)
  print("Test AUC of: {}".format(roc_auc))

# COMMAND ----------

# Start a new run and assign a run_name for future reference
with mlflow.start_run(run_name='gradient_boost') as run:
  model_2 = sklearn.ensemble.GradientBoostingClassifier(
    random_state=0, 
    
    # Try a new parameter setting for n_estimators
    n_estimators=200,
  )
  model_2.fit(X_train, y_train)
 
  predicted_probs = model_2.predict_proba(X_test)
  roc_auc = sklearn.metrics.roc_auc_score(y_test, predicted_probs[:,1])
  mlflow.log_metric("test_auc", roc_auc)
  print("Test AUC of: {}".format(roc_auc))

# COMMAND ----------

# After a model has been logged, you can load it in different notebooks or jobs
# mlflow.pyfunc.load_model makes model prediction available under a common API
model_loaded = mlflow.pyfunc.load_model(
  'runs:/{run_id}/model'.format(
    run_id=run.info.run_id
  )
)
 
predictions_loaded = model_loaded.predict(X_test)
predictions_original = model_2.predict(X_test)
 
# The loaded model should match the original
assert(np.array_equal(predictions_loaded, predictions_original))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the best Model

# COMMAND ----------

# Sort runs by their test auc; in case of ties, use the most recent run
best_run = mlflow.search_runs(
  order_by=['metrics.test_auc DESC', 'start_time DESC'],
  max_results=10,
).iloc[0]
print('Best Run')
print('AUC: {}'.format(best_run["metrics.test_auc"]))
print('Num Estimators: {}'.format(best_run["params.n_estimators"]))
print('Max Depth: {}'.format(best_run["params.max_depth"]))
print('Learning Rate: {}'.format(best_run["params.learning_rate"]))
 
best_model_pyfunc = mlflow.pyfunc.load_model(
  'runs:/{run_id}/model'.format(
    run_id=best_run.run_id
  )
)
 
#make a dataset with all predictions
best_model_predictions = X_test
best_model_predictions["prediction"] = best_model_pyfunc.predict(X_test)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save results

# COMMAND ----------

results = spark.createDataFrame(best_model_predictions)
spark.sql("drop table if exists predictions")
 
#Write results back to Unity Catalog from Python
results.write.saveAsTable("predictions")

# COMMAND ----------

model_uri = 'runs:/{run_id}/model'.format(
    run_id=best_run.run_id
  )
 
mlflow.register_model(model_uri, "wine_model")

# COMMAND ----------

model_uri = 'runs:/{run_id}/model'.format(
    run_id=best_run.run_id
  )
 
mlflow.register_model(model_uri, "wine_model")

# COMMAND ----------


