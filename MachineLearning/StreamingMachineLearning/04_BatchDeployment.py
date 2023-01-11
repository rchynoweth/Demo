# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Deployment
# MAGIC 
# MAGIC Batch inference is the most common way of deploying machine learning models.  This lesson introduces various strategies for deploying models using batch including Spark, write optimizations, and on the JVM.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) We will cover:<br>
# MAGIC  - Explore batch deployment options
# MAGIC  - Apply an `sklearn` model to a Spark DataFrame and save the results
# MAGIC  - Employ write optimizations including partitioning, bucketing, and Z-order
# MAGIC  - Compare other batch deployment options

# COMMAND ----------

dbutils.widgets.text("DatabaseName", "rac_demo_db")
dbutils.widgets.text("UserName", "ryan.chynoweth@databricks.com")

database_name = dbutils.widgets.get('DatabaseName')
user_name = dbutils.widgets.get('UserName')

table_location = '/users/{}/databases/{}'.format(user_name, database_name) 
spark.sql("CREATE DATABASE IF NOT EXISTS {} LOCATION '{}'".format(dbutils.widgets.get('DatabaseName'), table_location))
spark.sql("USE {}".format(dbutils.widgets.get("DatabaseName")))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Tracking Experiments with MLflow
# MAGIC 
# MAGIC Over the course of the machine learning life cycle, data scientists test many different models from various libraries with different hyperparameters.  Tracking these various results poses an organizational challenge.  In brief, storing experiments, results, models, supplementary artifacts, and code creates significant challenges.
# MAGIC 
# MAGIC MLflow Tracking is one of the three main components of MLflow.  It is a logging API specific for machine learning and agnostic to libraries and environments that do the training.  It is organized around the concept of **runs**, which are executions of data science code.  Runs are aggregated into **experiments** where many runs can be a part of a given experiment and an MLflow server can host many experiments.
# MAGIC 
# MAGIC Each run can record the following information:<br><br>
# MAGIC 
# MAGIC - **Parameters:** Key-value pairs of input parameters such as the number of trees in a random forest model
# MAGIC - **Metrics:** Evaluation metrics such as RMSE or Area Under the ROC Curve
# MAGIC - **Artifacts:** Arbitrary output files in any format.  This can include images, pickled models, and data files
# MAGIC - **Source:** The code that originally ran the experiment
# MAGIC 
# MAGIC MLflow tracking also serves as a **model registry** so tracked models can easily be stored and, as necessary, deployed into production.
# MAGIC 
# MAGIC Experiments can be tracked using libraries in Python, R, and Java as well as by using the CLI and REST calls.  This course will use Python, though the majority of MLflow functionality is also exposed in these other APIs.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlflow-tracking.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inference in Batch
# MAGIC 
# MAGIC Batch deployment represents the vast majority of use cases for deploying machine learning models.<br><br>
# MAGIC 
# MAGIC * This normally means running the predictions from a model and saving them somewhere for later use.
# MAGIC * For live serving, results are often saved to a database that will serve the saved prediction quickly. Check out the [03-Azure-Batch-Deployment]($./Cloud-specific Resources/03-Azure-Batch-Deployment) notebook for an example of this.
# MAGIC * In other cases, such as populating emails, they can be stored in less performant data stores such as a blob store.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/batch-predictions.png" width=800px />
# MAGIC 
# MAGIC Writing the results of an inference can be optimized in a number of ways...<br><br>
# MAGIC 
# MAGIC * For large amounts of data, predictions and writes should be performed in parallel
# MAGIC * **The access pattern for the saved predictions should also be kept in mind in how the data is written**
# MAGIC   - For static files or data warehouses, partitioning speeds up data reads
# MAGIC   - For databases, indexing the database on the relevant query generally improves performance
# MAGIC   - In either case, the index is working similar to an index in a book: it allows you to skip ahead to the relevant content

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC There are a few other considerations to ensure the accuracy of a model...<br><br>
# MAGIC 
# MAGIC * First is to make sure that the model matches expectations
# MAGIC   - We'll cover this in further detail in the model drift section
# MAGIC * Second is to **retrain the model on the majority of your dataset**
# MAGIC   - Either use the entire dataset for training or around 95% of it
# MAGIC   - A train/test split is a good method in tuning hyperparameters and estimating how the model will perform on unseen data
# MAGIC   - Retraining the model on the majority of the dataset ensures that you have as much information as possible factored into the model

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Inference in Spark
# MAGIC 
# MAGIC Models trained in various machine learning libraries can be applied at scale using Spark.  To do this, use `mlflow.pyfunc.spark_udf` and pass in the `SparkSession`, name of the model, and run id.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Using UDF's in Spark means that supporting libraries must be installed on every node in the cluster.  In the case of `sklearn`, this is installed in Databricks clusters by default.  When using other libraries, you will need to install them to ensure that they will work as UDFs.  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Start by training an `sklearn` model.  Apply it using a Spark UDF generated by `mlflow`.
# MAGIC 
# MAGIC Import the data.  **Do not perform a train/test split.**
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> It is common to skip the train/test split in training a final model.

# COMMAND ----------

import pandas as pd
from sklearn.model_selection import train_test_split

df = pd.read_csv("/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv")

X = df.drop(["price"], axis=1)
y = df["price"]

# COMMAND ----------

# MAGIC %md
# MAGIC Train a final model

# COMMAND ----------

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

rf = RandomForestRegressor(n_estimators=100, max_depth=5)
rf.fit(X, y)

predictions = X.copy()
predictions["prediction"] = rf.predict(X)

mse = mean_squared_error(y, predictions["prediction"]) # This is on the same data the model was trained

# COMMAND ----------

# MAGIC %md
# MAGIC Log the model.

# COMMAND ----------

import mlflow.sklearn
from sklearn.metrics import mean_squared_error

with mlflow.start_run(run_name="Final RF Model") as run:
  mlflow.sklearn.log_model(rf, "random-forest-model")
  mlflow.log_metric("Train data MSE", mse)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a Spark DataFrame from the Pandas DataFrame.

# COMMAND ----------

XDF = spark.createDataFrame(X)

display(XDF)

# COMMAND ----------

# MAGIC %md
# MAGIC MLflow easily produces a Spark user defined function (UDF).  This bridges the gap between Python environments and applying models at scale using Spark.

# COMMAND ----------

predict = mlflow.pyfunc.spark_udf(spark, run.info.artifact_uri + "/random-forest-model")

# COMMAND ----------

# MAGIC %md
# MAGIC Apply the model as a standard UDF using the column names as the input to the function.

# COMMAND ----------

predictionDF = XDF.withColumn("prediction", predict(*X.columns))

display(predictionDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Optimizations
# MAGIC 
# MAGIC There are many possible optimizations depending on your batch deployment scenerio.  In Spark and Delta Lake, the following optimizations are possible:<br><br>
# MAGIC 
# MAGIC - **Partitioning:** stores data associated with different categorical values in different directories
# MAGIC - **Bucketing:** similar to partitioning but with higher cardinality data (i.e. when more unique values are in your data)
# MAGIC - **Z-Ordering:** colocates related information in the same set of files
# MAGIC - **Data Skipping:** aims at speeding up queries that contain filters (WHERE clauses)
# MAGIC - **Partition Pruning:** speeds up queries by limiting the amount of data read
# MAGIC 
# MAGIC Other optimizations include:<br><br>
# MAGIC 
# MAGIC - **Database indexing:** allows certain table columns to be more effectively queried 
# MAGIC - **Geo-replication:** replicates data in different geographical regions

# COMMAND ----------

# MAGIC %md
# MAGIC Partition by zipcode. This is an indexed column, not the true zipcode.

# COMMAND ----------

delta_partitioned_path = f"/users/{user_name}/mlflow_demos/batch-predictions-partitioned.delta"

predictionDF.write.partitionBy("zipcode").mode("OVERWRITE").format("delta").save(delta_partitioned_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the files.

# COMMAND ----------

display(dbutils.fs.ls(delta_partitioned_path))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Bucket by zipcode.  This is for high cardinality columns.  `.bucketBy()` is a `DataFrameWriter` method that will create a certain number of buckets determined in advance and hashes each value into its respective bucket or folder.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Writing using `.bucketBy()` currently needs to work through the Hive Metastore using `.saveAsTable()`.  For details, [see the JIRA ticket.](https://issues.apache.org/jira/browse/SPARK-19256)

# COMMAND ----------

parquet_bucketed_path = f"/users/{user_name}/mlflow_demos/mlflow-model-training/batch-predictions-bucketed.parquet"

predictionDF.write.bucketBy(5, "neighbourhood_cleansed").mode("OVERWRITE").option("path", parquet_bucketed_path).format("parquet").saveAsTable("batchPredictionsBucketed")

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the underlying files.  You'll see 5 parts, one for each bucket.

# COMMAND ----------

display(dbutils.fs.ls(parquet_bucketed_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Z-Ordering is a form of multi-dimensional clustering that colocates related information in the same set of files.  It reduces the amount of data that needs to be read.  [You can read more about it here.](https://docs.databricks.com/delta/optimizations/file-mgmt.html#z-ordering-multi-dimensional-clustering)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Other Deployment Options
# MAGIC 
# MAGIC There are a number of other common batch deployment options.  One common use case is going from a Python environment for training to a Java environment for deployment.  Here are a few tools that can help with that:<br><br>
# MAGIC 
# MAGIC  - **An Easy Port to Java:** In certain models, such as linear regression, the coefficients of a trained model can be taken and implemented by hand in Java.  This can work with tree-based models as well.
# MAGIC  - **Re-serializing for Java:** Since Python uses Pickle by default to serialize, a library like <a href="https://github.com/jpmml/jpmml-sklearn" target="_blank">jpmml-sklearn</a> can de-serialize `sklearn` libraries and re-serialize them for use in Java environments.
# MAGIC  - **Leveraging Library Functionality:** Some libraries include the ability to deploy to Java such as <a href="https://github.com/dmlc/xgboost/tree/master/jvm-packages" target="_blank">xgboost4j</a>.
# MAGIC  - **Containers:** Containerized solutions are becoming increasingly popular since they offer the encapsulation and reliability offered by jars while offering more deployment options than just the Java environment.

# COMMAND ----------


