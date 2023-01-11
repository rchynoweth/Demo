# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://drive.google.com/thumbnail?id=1xwMbdvl02ntbieEQtaVu1Vdw4_AYhheb" width=100/> 
# MAGIC # MLFlow and Snowflake
# MAGIC 
# MAGIC 
# MAGIC __NOTE:__ Use a cluster running Databricks 7.3 ML or higher.

# COMMAND ----------

database_name = "rac_demo_db"

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# COMMAND ----------

spark.sql(f"USE {database_name}")

# COMMAND ----------

dbutils.notebook.run("/Users/ryan.chynoweth@databricks.com/MLFlow_Demo/2-DemoMLFlowSnowflakeSetup", 0, {"database_name": database_name})

# COMMAND ----------

bronze_path = "/Users/ryan.chynoweth@databricks.com/bronze/mlflow_snowflake_intro"

# COMMAND ----------

# MAGIC %md
# MAGIC Load data from snowflake and save it in our Data Lake as Delta. 

# COMMAND ----------

# Use secrets DBUtil to get Snowflake credentials.
user = dbutils.secrets.get("demo", "rac_snowflake_user")
password = dbutils.secrets.get("demo", "rac_snowflake_password")
url = dbutils.secrets.get("demo", "rac_snowflake_url")

# snowflake connection options
options = {
  "sfUrl":  url,
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "DEMO_DB",
  "sfSchema": "PUBLIC",
  "sfWarehouse": "DEMO_CLUSTER"
}



# COMMAND ----------

# Read the data written by the previous cell back.
df = (spark.read
  .format("snowflake")
  .options(**options)
  .option("dbtable", "rac_mlflow_current_readings_labeled")
  .load())

# show the data
display(df)

# COMMAND ----------

# We will want to save our data as a delta table in our hive metastore. 
# this allows us to only read from Snowflake one time
# Better performance!
df.write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("rac_mlflow_current_readings_labeled")

# COMMAND ----------

# MAGIC %md
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

# These are the Spark ML library functions we'll be using

from pyspark.ml.feature import IndexToString, StringIndexer, VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator 
  
import mlflow
from mlflow import spark as mlflow_spark # renamed to prevent collisions when doing spark.sql

import time

# Tuning libraries
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder 
from pyspark.ml import Pipeline


# COMMAND ----------

# MAGIC %md
# MAGIC ###PART 1... 
# MAGIC 
# MAGIC __*Developing*__ a Machine Learning model with the help of __MLflow Tracking__
# MAGIC 
# MAGIC In Part 1, we play the role of a Data Scientist developing and testing a new model.  We'll see how MLflow Tracking helps us organize and evaluate our work.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lets take a look at our raw training data
# MAGIC 
# MAGIC SELECT * FROM rac_mlflow_current_readings_labeled

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's take a closer look at the number of values for each column
# MAGIC -- There are 7 different operational_status values.  That's what we are trying to predict when we process an unlabeled row.
# MAGIC -- There are only 2 device_type values, and only 18 device_id values.  In our model, those will be "categorical" features.
# MAGIC -- There are lots of distinct values for reading_1, reading_2, and reading_3.  In our model, those will be "continuous" features.
# MAGIC 
# MAGIC SELECT 
# MAGIC   COUNT(DISTINCT device_operational_status) AS distinct_operational_statuses,
# MAGIC   COUNT(DISTINCT device_type) AS distinct_device_types,
# MAGIC   COUNT(DISTINCT device_id) AS distinct_device_ids,
# MAGIC   COUNT(DISTINCT reading_1) AS distinct_reading_1s,
# MAGIC   COUNT(DISTINCT reading_2) AS distinct_reading_2s,
# MAGIC   COUNT(DISTINCT reading_3) AS distinct_reading_3s
# MAGIC FROM rac_mlflow_current_readings_labeled

# COMMAND ----------

# MAGIC %md
# MAGIC #### Building a Model with MLflow Tracking
# MAGIC 
# MAGIC The cell below creates a Python function that builds, tests, and trains a Decision Tree model.  
# MAGIC 
# MAGIC We made it a function so that you can easily call it many times with different parameters.  This is a convenient way to create many different __Runs__ of an experiment, and will help us show the value of MLflow Tracking.
# MAGIC 
# MAGIC Read through the code in the cell below, and notice how we use MLflow Tracking in several different ways:
# MAGIC   
# MAGIC   - First, we __initiate__ MLflow Tracking like this:   
# MAGIC   ```with mlflow.start_run() as run:```
# MAGIC 
# MAGIC Then we illustrate several things we can do with Tracking:
# MAGIC   - __Tags__ let us assign free-form name-value pairs to be associated with the run.  
# MAGIC   - __Parameters__ let us name and record single values for a run.  
# MAGIC   - __Metrics__ also let us name and record single numeric values for a run.  We can optionally record *multiple* values under a single name.
# MAGIC   - Finally, we will log the __Model__ itself.

# COMMAND ----------

# We will create a training function that accepts a max depth parameter
# this will allow us to do a small hyper parameter tuning example


with mlflow.start_run() as run:

  # Start a timer to get overall elapsed time for this function
  overall_start_time = time.time()

  # Log a Tag for the run
  mlflow.set_tag("Owner", "ryan.chynoweth@databricks.com")


  ## STEP 1: Read in the raw data to use for training
  # We'll use an MLflow metric to log the time taken in each step 
  start_time = time.time()

  df_raw_data = spark.sql("""
    SELECT 
      device_type,
      device_operational_status AS label,
      device_id,
      reading_1,
      reading_2,
      reading_3
    FROM rac_mlflow_current_readings_labeled
  """)

  # STEP 2: Index the Categorical data so the Decision Tree can use it

  # Create a numerical index of device_type values (it's a category, but Decision Trees don't need OneHotEncoding)
  device_type_indexer = StringIndexer(inputCol="device_type", outputCol="device_type_index")

  # Create a numerical index of device_id values (it's a category, but Decision Trees don't need OneHotEncoding)
  device_id_indexer = StringIndexer(inputCol="device_id", outputCol="device_id_index")

  # Create a numerical index of label values (device status) 
  label_indexer = StringIndexer(inputCol="label", outputCol="label_index")


  assembler = VectorAssembler( 
  inputCols=["device_type_index", "device_id_index", "reading_1", "reading_2", "reading_3"], 
  outputCol="features")

  # STEP 3: Randomly split data into training and test sets. Set seed for reproducibility

  (training_data, test_data) = df_raw_data.randomSplit([0.7, 0.3], seed=100)

  # Log the size of the training and test data
  # NOTE: these metrics only occur once... they are not series
  mlflow.log_metric("Training Data Rows", training_data.count())
  mlflow.log_metric("Test Data Rows", test_data.count())


  ######
  ## Step 4: Set up pipeline object
  ######
  dtClassifier = DecisionTreeClassifier(labelCol="label_index", featuresCol="features")

  pipeline = Pipeline(stages=[
    device_type_indexer, 
    device_id_indexer, 
    label_indexer, 
    assembler, 
    dtClassifier
  ])
  
  # we will just search one parameter for time savings
  grid = (ParamGridBuilder()
           .addGrid(dtClassifier.maxDepth, [2,3,4,5,6,7,8,9,10])
           .build()
         )




  ########################
  # STEP 5: TRAIN MODEL
  ########################
  
  evaluator = MulticlassClassificationEvaluator(
    labelCol="label_index", predictionCol="prediction", metricName="accuracy")

  cvModel = CrossValidator(estimator=pipeline, evaluator=evaluator, estimatorParamMaps=grid, numFolds=3)

  fittedCvModel = cvModel.fit(training_data)
  

  # STEP 6: Test the model
  
  bestModel = fittedCvModel.bestModel
  
  df_predictions = bestModel.transform(test_data)
  
  # STEP 7: Determine the model's accuracy
  # Log the model's accuracy in MLflow
  accuracy = evaluator.evaluate(df_predictions, {evaluator.metricName: "accuracy"})
  
  mlflow.log_metric("Accuracy", accuracy)

  # We'll also use an MLflow metric to log overall time
  overall_end_time = time.time()
  overall_elapsed_time = overall_end_time - overall_start_time
  mlflow.log_metric("Overall Elapsed Time", overall_elapsed_time)

  # Log the model itself
  mlflow_spark.log_model(bestModel, "spark-model")

# COMMAND ----------

# MAGIC %md
# MAGIC Now we're ready to do a few runs.
# MAGIC 
# MAGIC Before you begin, click "Experiment" in the upper right of the notebook.  This is our link to the MLflow UI.
# MAGIC 
# MAGIC If this is the first time you have run this notebook, you will see that no runs have been recorded yet: <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/mlflow_runs_no_runs_yet_v2.PNG?raw=true" width=300/>
# MAGIC 
# MAGIC In the cell below, set the p_max_depth parameter (hint: try values between 1 and 10).  Set the p_owner value to some text string.  Run the cell several times, and change the parameters for each execution.

# COMMAND ----------

# MAGIC %md
# MAGIC ###PART 2...
# MAGIC 
# MAGIC __*Examining* the MLflow UI__
# MAGIC 
# MAGIC Now click "Experiment" again, and you'll see something like this.  Click the link that takes us to the top-level MLflow page: <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/mlflow_runs_v2.png?raw=true" width=300/>
# MAGIC 
# MAGIC The __top-level page__ summarizes all our runs.  
# MAGIC 
# MAGIC Notice how our custom parameters and metrics are displayed.
# MAGIC 
# MAGIC Click on the "Accuracy" or "Overall Elapsed Time" columns to quickly create leaderboard views of your runs.
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/mlflow_summary_page.png?raw=true" width=1500/>
# MAGIC 
# MAGIC Now click on one of the runs to see a __detail page__.  Examine the page to see how our recorded data is shown.
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/mlflow_detail_page.png?raw=true" width=700/>
# MAGIC 
# MAGIC Now click on the "Step Elapsed Time" metric to see how __multiple metric values__ are handled.  Remember that we created this metric to store the elapsed time of each step in our model-building process.  This graph lets us easily see which steps might be bottlenecks.
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/mlflow_metrics_graph.png?raw=true" width=1500/>
# MAGIC 
# MAGIC Consider... how could MLflow help you find your most desirable model?  How could it help you avoid repeating runs?  How could it help you demonstrate your results to your teammates?

# COMMAND ----------

# MAGIC %md
# MAGIC ###PART 3... 
# MAGIC 
# MAGIC Now let's play the role of a developer who wants to __*use*__ a model created by a Data Scientist.
# MAGIC 
# MAGIC Let's see how MLflow can help us *find* a model, *instantiate* it, and *use* it in an application.
# MAGIC 
# MAGIC Imagine that we are building an application in a completely different notebook, but we want to leverage models built by someone else.

# COMMAND ----------

# MAGIC %md
# MAGIC Lets take a look at all the models we created and the data that we tracked. 

# COMMAND ----------

# Let's load the experiment... 
# If this were *really* another notebook, I'd have to obtain the Experiment ID from the MLflow page.  
# But since we are in the original notebook, I can get it as a default value

df_client = spark.read.format("mlflow-experiment").load()
df_client.createOrReplaceTempView("vw_client")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's take a peek at the data that MLflow returns
# MAGIC 
# MAGIC SELECT * FROM vw_client

# COMMAND ----------

# MAGIC %md
# MAGIC Select the best model. 

# COMMAND ----------

# Let's query the MLflow data in a way that shows us the most accurate model in the first row
# This is possible because we logged accuracy as a metric using MLflow
# Then we can grab the run_id to get a handle to the model itself

df_model_selector = spark.sql("""SELECT experiment_id, run_id, end_time, metrics.Accuracy as accuracy, concat(artifact_uri,'/spark-model') as artifact_uri 

    FROM vw_client 
    
    WHERE status='FINISHED'
    ORDER BY metrics.Accuracy desc

  """)



display(df_model_selector)

# COMMAND ----------

# Let's put some interesting columns into Python variables

selected_experiment_id = df_model_selector.first()[0]
selected_model_id = df_model_selector.first()[1]
selected_model_accuracy = df_model_selector.first()[3]
selected_model_uri = df_model_selector.first()[4]

print(f"Selected experiment ID: {selected_experiment_id}")
print(f"Selected model ID: {selected_model_id}")
print(f"Selected model accuracy: {selected_model_accuracy}")
print(f"Selected model URI: {selected_model_uri}")


# COMMAND ----------

# MAGIC %md
# MAGIC Load the model from MLflow

# COMMAND ----------

# Now we can actually instantiate our chosen model with one line of code!


selected_model = mlflow_spark.load_model(selected_model_uri)


# COMMAND ----------

# MAGIC %md
# MAGIC Now we need to make predictions on data that does not have a label and that the model has not seen before. We will load this data from snowflake. 
# MAGIC 
# MAGIC This is a batch prediction process which is common in production workloads. 

# COMMAND ----------

# Read the data written by the previous cell back.
df = (spark.read
  .format("snowflake")
  .options(**options)
  .option("dbtable", "rac_mlflow_current_readings_unlabeled")
  .load())

# show the data
display(df)

# COMMAND ----------

# load to Hive metastore for performance
df.write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("rac_mlflow_current_readings_labeled")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify that the input data we're using has no label column
# MAGIC 
# MAGIC SELECT * FROM rac_mlflow_current_readings_labeled

# COMMAND ----------

# Here we prepare the data so the model can use it
# This is just a subset of the code we saw earlier when we trained the model

# First we read in the raw data
df_client_raw_data = spark.sql("""
  SELECT 
    device_type,
    device_id,
    reading_1,
    reading_2,
    reading_3
  FROM rac_mlflow_current_readings_labeled  
""")
    

# COMMAND ----------

# MAGIC %md
# MAGIC Make predictions on our dataset

# COMMAND ----------

# Now we can actually run the model we just instantiated

df_client_predictions = selected_model.transform(df_client_raw_data)
df_client_predictions.createOrReplaceTempView("vw_client_predictions")
display(df_client_predictions) # Let's take a look at the output... notice the "prediction" column (last column... scroll right)

# COMMAND ----------

# MAGIC %md
# MAGIC Format predictions to make it more readable. 

# COMMAND ----------

# I'm almost ready to write my data with predictions out to a Delta Lake table.  
# But I don't want to  use those numeric prediction values that the model produces.

# I would like to change them to the friendly names that were in my labeled training data
# Fortunately, Spark ML gives us a way to get these values

df = spark.sql("""
  SELECT 
    device_operational_status
  FROM current_readings_labeled
""")

# Create a numerical index of label values (device status) 
label_indexer = StringIndexer(inputCol="device_operational_status", outputCol="device_operational_status_index")
df = label_indexer.fit(df).transform(df)
    
labelReverse = IndexToString().setInputCol("device_operational_status_index")
df_reversed = labelReverse.transform(df)

df_reversed.createOrReplaceTempView("vw_reversed")
display(spark.sql("""
  SELECT DISTINCT
    device_operational_status,
    device_operational_status_index
  FROM vw_reversed
  ORDER BY device_operational_status_index ASC
"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's persist the output of our decision tree application
# MAGIC 
# MAGIC DROP TABLE IF EXISTS rac_mlflow_snowflake_application_output;
# MAGIC 
# MAGIC CREATE TABLE rac_mlflow_snowflake_application_output
# MAGIC USING DELTA
# MAGIC AS (
# MAGIC   SELECT
# MAGIC     device_type,
# MAGIC     device_id,
# MAGIC     reading_1,
# MAGIC     reading_2,
# MAGIC     reading_3,
# MAGIC     CASE   -- Change the numeric predictions to user-friendly text values
# MAGIC       WHEN prediction = 0 THEN "RISING"
# MAGIC       WHEN prediction = 1 THEN "IDLE"
# MAGIC       WHEN prediction = 2 THEN "NORMAL"
# MAGIC       WHEN prediction = 3 THEN "HIGH"
# MAGIC       WHEN prediction = 4 THEN "RESETTING"
# MAGIC       WHEN prediction = 5 THEN "FAILURE"
# MAGIC       WHEN prediction = 6 THEN "DESCENDING"
# MAGIC       ELSE 'UNKNOWN'
# MAGIC     END AS predicted_device_operational_status
# MAGIC   FROM vw_client_predictions
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's verify that our new table looks good
# MAGIC 
# MAGIC SELECT * FROM rac_mlflow_snowflake_application_output

# COMMAND ----------

# MAGIC %md
# MAGIC Write the data back out to snowflake

# COMMAND ----------

df = spark.sql("SELECT * FROM rac_mlflow_snowflake_application_output")

# COMMAND ----------

(df.write
  .format("snowflake")
  .mode("overwrite")
  .options(**options)
  .option("dbtable", "rac_mlflow_snowflake_application_output")
  .save())

# COMMAND ----------

# MAGIC  %md
# MAGIC  ### What just happened?
# MAGIC  
# MAGIC  1. We loaded two datasets, one for training and one for "deployment" from snowflake into Databricks.
# MAGIC    - All intermediate datasets for data preparation and ETL were stored as Delta and registered in the Hive view allowing for greater performance. 
# MAGIC  1. We trained a model using MLflow and cross validation, and we learned a lot about MLflow Tracking in this module.  We tracked: 
# MAGIC    - Parameters
# MAGIC    - Metrics
# MAGIC    - Tags
# MAGIC    - The model itself
# MAGIC  1. We loaded our model and deployed it as a batch process within a Databricks notebook. We could have deployed it as a streaming solution or as a REST API. 
# MAGIC    - Saved predictions in Databricks and in Snowflake!

# COMMAND ----------

# MAGIC %md
# MAGIC # Alternative Options
# MAGIC 
# MAGIC Snowflake supports the [creation of External tables](https://docs.snowflake.com/en/sql-reference/sql/create-external-table.html#materialized-view-on-an-external-table) where you can query sources outside of Snowflake. The supported file types are as follows:  
# MAGIC - CSV
# MAGIC - Parquet
# MAGIC - Json
# MAGIC - Avro
# MAGIC - ORC
# MAGIC - XML
# MAGIC 
# MAGIC This would allow users to avoid writing data into Snowflake as users can create external Snowflake tables with the Parquet type. Delta Lake is simply a storage format on top of parquet so your Delta table would just need to be vacuumed to eliminate old versions of the table. This is a similar process with Azure Synapse's PolyBase support. The advantage here is that you would only need to store the data in one location but access it from both.     

# COMMAND ----------


