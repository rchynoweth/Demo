# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Databricks AutoML 
# MAGIC 
# MAGIC [Databricks AutoML](https://docs.databricks.com/applications/machine-learning/automl.html) helps you automatically apply machine learning to a dataset. It prepares the dataset for model training and then performs and records a set of trials. Once complete it will display the results and produce a Python notebook with source code for each of the trial runs. The notebook augments the Data Scientist's ability by providing code bases than can be extended upon. AutoML calculates summary statistics on your dataset and saves this information in a notebook that can be reviewed later.  
# MAGIC 
# MAGIC Each model is constructed from **open source** components so that you can deploy them into existing machine learning processes. 
# MAGIC 
# MAGIC AutoML currently supports classification and regression problems using algorithms from the [scikit-learn](https://scikit-learn.org/stable/) and [xgboost](https://xgboost.readthedocs.io/en/latest/) packages.
# MAGIC 
# MAGIC 
# MAGIC #### Supported algorithms
# MAGIC - **Classification models**
# MAGIC   - [Decision trees](https://scikit-learn.org/stable/modules/tree.html#classification)
# MAGIC   - [Random forests](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestClassifier.html#sklearn.ensemble.RandomForestClassifier)
# MAGIC   - [Logistic regression](https://scikit-learn.org/stable/modules/linear_model.html#logistic-regression)
# MAGIC   - [XGBoost](https://xgboost.readthedocs.io/en/latest/python/python_api.html#xgboost.XGBClassifier)
# MAGIC   - [LightGBM](https://lightgbm.readthedocs.io/en/latest/pythonapi/lightgbm.LGBMClassifier.html)
# MAGIC 
# MAGIC - **Regression models**
# MAGIC   - [Stochastic gradient descent (SGD)](https://scikit-learn.org/stable/modules/sgd.html)
# MAGIC   - [Decision trees](https://scikit-learn.org/stable/modules/tree.html#regression)
# MAGIC   - [Random forests](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestRegressor.html#sklearn.ensemble.RandomForestRegressor)
# MAGIC   - [XGBoost](https://xgboost.readthedocs.io/en/latest/python/python_api.html#xgboost.XGBRegressor)
# MAGIC   - [LightGBM](https://lightgbm.readthedocs.io/en/latest/pythonapi/lightgbm.LGBMRegressor.html)
# MAGIC 
# MAGIC - **Forecasting models**
# MAGIC   - [Prophet](https://facebook.github.io/prophet/docs/quick_start.html#python-api) (aka Facebook)
# MAGIC 
# MAGIC #### Demo
# MAGIC 
# MAGIC In this notebook we will use the census income dataset, that contains census data from the 1994 census database. Each row represents a group of individuals. The goal is to determine whether a group has an income of over 50k a year or not. This classification is represented as a string in the income column with values <=50K or >50k.
# MAGIC 
# MAGIC 
# MAGIC ##### Notes
# MAGIC - Must use DBR 8.3 ML or above  
# MAGIC - No additional libraries can be installed on the cluster, expect for the ones preinstalled by the DBR ML Runtime  
# MAGIC - Databricks AutoML is different than the Databricks AutoML Toolkit repository. This is a fully supported project with deep integration on Databricks   
# MAGIC - Information was last updated on August 17, 2021  
# MAGIC - [Source Notebook](https://docs.databricks.com/_static/notebooks/machine-learning/automl-classification-example.html)   

# COMMAND ----------

# DBTITLE 1,Load data into Dataframe
from pyspark.sql.types import DoubleType, StringType, StructType, StructField
 
schema = StructType([
  StructField("age", DoubleType(), False),
  StructField("workclass", StringType(), False),
  StructField("fnlwgt", DoubleType(), False),
  StructField("education", StringType(), False),
  StructField("education_num", DoubleType(), False),
  StructField("marital_status", StringType(), False),
  StructField("occupation", StringType(), False),
  StructField("relationship", StringType(), False),
  StructField("race", StringType(), False),
  StructField("sex", StringType(), False),
  StructField("capital_gain", DoubleType(), False),
  StructField("capital_loss", DoubleType(), False),
  StructField("hours_per_week", DoubleType(), False),
  StructField("native_country", StringType(), False),
  StructField("income", StringType(), False)
])
input_df = spark.read.format("csv").schema(schema).load("/databricks-datasets/adult/adult.data")

# COMMAND ----------

# DBTITLE 1,Split dataset
train_df, test_df = input_df.randomSplit([0.99, 0.01], seed=42)
display(train_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Training
# MAGIC 
# MAGIC The following command starts an AutoML run. You must provide the column that the model should predict in the `target_col` argument.
# MAGIC When the run completes, you can follow the link to the best trial notebook to examine the training code. This notebook also includes a feature importance plot.

# COMMAND ----------

from databricks import automl
summary = automl.classify(train_df, target_col="income", timeout_minutes=30)

# COMMAND ----------

# DBTITLE 1,Display AutoML output
help(summary)

# COMMAND ----------

# DBTITLE 1,Model inference
model_uri = summary.best_trial.model_path # loading model from mlflow experiment

# COMMAND ----------

import mlflow
 
# Prepare test dataset
test_pdf = test_df.toPandas()
y_test = test_pdf["income"]
X_test = test_pdf.drop("income", axis=1)
 
# Run inference using the best model
model = mlflow.pyfunc.load_model(model_uri)
predictions = model.predict(X_test)
test_pdf["income_predicted"] = predictions
display(test_pdf)

# COMMAND ----------

predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type="string")
display(test_df.withColumn("income_predicted", predict_udf()))

# COMMAND ----------

import sklearn.metrics
 
model = mlflow.sklearn.load_model(model_uri)
sklearn.metrics.plot_confusion_matrix(model, X_test, y_test)

# COMMAND ----------


