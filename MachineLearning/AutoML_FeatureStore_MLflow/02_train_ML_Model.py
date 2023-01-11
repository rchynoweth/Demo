# Databricks notebook source
# MAGIC %md ---
# MAGIC title: End-to-End MLOps demo with MLFlow, Feature Store and Auto ML, part 2 - Auto ML
# MAGIC authors:
# MAGIC - Rafi Kurlansik
# MAGIC tags:
# MAGIC - python
# MAGIC - auto-ml
# MAGIC created_at: 2021-05-01
# MAGIC updated_at: 2021-05-01
# MAGIC tldr: End-to-end demo of Databricks for MLOps, including MLflow, the registry, webhooks, scoring, feature store and auto ML. Part 2 - auto-generated output of auto ML
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook Links
# MAGIC - AWS demo.cloud: [https://demo.cloud.databricks.com/#notebook/10166871](https://demo.cloud.databricks.com/#notebook/10166871)

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://github.com/RafiKurlansik/laughing-garbanzo/blob/main/step2.png?raw=true">

# COMMAND ----------

# MAGIC %md
# MAGIC # NOTE! Yes, read!
# MAGIC 
# MAGIC This is not a notebook you execute. It's an example of the notebook auto ML generates if you use the auto ML functionality in Databricks on the table you just generated.
# MAGIC 
# MAGIC # Yes, Everything Below Is Auto-Generated!

# COMMAND ----------

# MAGIC %md
# MAGIC # Logistic Regression training
# MAGIC This is an auto-generated notebook. To reproduce these results, attach this notebook to the **ML shard (8.x)** cluster and rerun it.
# MAGIC - Navigate to the parent notebook [here](#notebook/2179450751013685)
# MAGIC - Compare trials in the [MLflow experiment](#mlflow/experiments/2179450751013694/s?orderByKey=metrics.%60val_f1_score%60&orderByAsc=false)
# MAGIC - Clone this notebook into your project folder by selecting **File > Clone** in the notebook toolbar.
# MAGIC 
# MAGIC Runtime Version: _8.x-snapshot-cpu-ml-scala2.12_

# COMMAND ----------

dbutils.widgets.text("DatabaseName", "rac_demo_db")
database_name = dbutils.widgets.get("DatabaseName")
spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))
spark.sql("USE {}".format(database_name))

# COMMAND ----------

dbutils.fs.mkdirs("/Users/ryan.chynoweth@databricks.com/databricks_ml/rac_customer_churn")

# COMMAND ----------

import mlflow
from mlflow import spark as mlflow_spark # renamed to prevent collisions when doing spark.sql

target_col = "churn"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# Load input data into a pandas DataFrame.
import pandas as pd
from databricks import feature_store

fs = feature_store.FeatureStoreClient()


input_pdf = fs.read_table('{}.churn_features'.format(database_name)).toPandas()

# Preview data
input_pdf.head(5)

# COMMAND ----------

from sklearn.model_selection import train_test_split

input_X = input_pdf.drop([target_col, "customerID"], axis=1)
input_y = input_pdf[target_col]

X_train, X_val, y_train, y_val = train_test_split(input_X, input_y, random_state=42, stratify=input_y)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Train classification model
# MAGIC - Log relevant metrics to MLflow to track runs
# MAGIC - All the runs are logged under [this MLflow experiment](#mlflow/experiments/2179450751013694/s?orderByKey=metrics.%60val_f1_score%60&orderByAsc=false)
# MAGIC - Change the model parameters and re-run the training cell to log a different trial to the MLflow experiment
# MAGIC - To view the full list of tunable hyperparameters, check the output of the cell below

# COMMAND ----------

from sklearn.linear_model import LogisticRegression

help(LogisticRegression)

# COMMAND ----------

import mlflow
import sklearn
from sklearn.pipeline import Pipeline

model = LogisticRegression(
  C=0.03309776273903743,
  penalty="l2",
  random_state=43,
)


# Enable automatic logging of input samples, metrics, parameters, and models
mlflow.sklearn.autolog(log_input_examples=True)

with mlflow.start_run(run_name="logistic_regression") as mlflow_run:
    model.fit(X_train, y_train)
    
    # Training metrics are logged by MLflow autologging
    # Log metrics for the validation set
    sklr_val_pred = model.predict(X_val)
    sklr_val_pred_proba = model.predict_proba(X_val)
    sklr_val_metrics = {
        "val_precision_score": sklearn.metrics.precision_score(y_val, sklr_val_pred, average="weighted"),
        "val_recall_score": sklearn.metrics.recall_score(y_val, sklr_val_pred, average="weighted"),
        "val_f1_score": sklearn.metrics.f1_score(y_val, sklr_val_pred, average="weighted"),
        "val_accuracy_score": sklearn.metrics.accuracy_score(y_val, sklr_val_pred, normalize=True),
        "val_log_loss": sklearn.metrics.log_loss(y_val, sklr_val_pred_proba),
    }
    # For binary classification, `roc_auc_score` expects `y_score` to be the probability of the class with the greater label.
    sklr_val_pred_proba = sklr_val_pred_proba[:, 1]
    
    sklr_val_metrics["val_roc_auc_score"] = sklearn.metrics.roc_auc_score(
        y_val,
        sklr_val_pred_proba,
        average="weighted",
        )
    sklr_val_metrics["val_score"] = model.score(X_val, y_val)
    mlflow.log_metrics(sklr_val_metrics)
    display(pd.DataFrame(sklr_val_metrics, index=[0]))

# COMMAND ----------

# Print the absolute model uri path to the logged artifact
# Use mlflow.pyfunc.load_model(<model-uri-path>) to load this model in any notebook
print(f"Model artifact is logged at: { mlflow_run.info.artifact_uri}/model")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature importance
# MAGIC 
# MAGIC SHAP is a game-theoretic approach to explain machine learning models, providing a summary plot
# MAGIC of the relationship between features and model output. Features are ranked in descending order of
# MAGIC importance, and impact/color describe the correlation between the feature and the target variable.
# MAGIC 
# MAGIC For more information on how to read Shapley values, see the [SHAP documentation](https://shap.readthedocs.io/en/latest/example_notebooks/overviews/An%20introduction%20to%20explainable%20AI%20with%20Shapley%20values.html).

# COMMAND ----------

from shap import KernelExplainer, summary_plot

try:
    # Sample background data for SHAP Explainer. Increase the sample size to reduce variance.
    train_sample = X_train.sample(n=min(100, len(X_train.index)))

    # Choose any prediction to explain, or sample multiple examples for more thorough results.
    example = X_val.sample(n=100)

    # Use Kernel SHAP to explain feature importance on example from validation set
    predict = lambda x: model.predict_proba(pd.DataFrame(x, columns=X_train.columns))
    explainer = KernelExplainer(predict, train_sample, link="logit")
    shap_values = explainer.shap_values(example, l1_reg=False)
    summary_plot(shap_values, example)
except Exception as e:
    print(f"An unexpected error occurred while plotting feature importance using SHAP: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inference
# MAGIC [The MLflow Model Registry](https://docs.databricks.com/applications/mlflow/model-registry.html) is a collaborative hub where teams can share ML models, work together from experimentation to online testing and production, integrate with approval and governance workflows, and monitor ML deployments and their performance. The snippets below show how to add the model trained in this notebook to the model registry and to retrieve it later for inference.
# MAGIC 
# MAGIC ### Register to Model Registry
# MAGIC ```
# MAGIC model_name = "Example"
# MAGIC 
# MAGIC model_uri = f"runs:/{ mlflow_run.info.run_id }/model"
# MAGIC registered_model_version = mlflow.register_model(model_uri, model_name)
# MAGIC ```
# MAGIC 
# MAGIC ### Load from Model Registry
# MAGIC ```
# MAGIC model_name = "Example"
# MAGIC model_version = registered_model_version.version
# MAGIC 
# MAGIC model = mlflow.pyfunc.load_model(model_uri=f"models:/{model_name}/{model_version}")
# MAGIC model.predict(input_X)
# MAGIC ```

# COMMAND ----------


