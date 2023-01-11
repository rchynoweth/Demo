# Databricks notebook source
# MAGIC %md
# MAGIC # Data Management 
# MAGIC 
# MAGIC In this notebook we will:  
# MAGIC 1. Prepare data for machine learning 
# MAGIC 1. Create a Feature Store  
# MAGIC 1. Show miscellaneous features 

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import * 
from databricks import feature_store

# COMMAND ----------

dbutils.widgets.text("DatabaseName", "rac_demo_db")
dbutils.widgets.text("UserName", "ryan.chynoweth@databricks.com")

# COMMAND ----------

database_name = dbutils.widgets.get('DatabaseName')
user_name = dbutils.widgets.get('UserName')

# COMMAND ----------

table_location = '/users/{}/databases/{}'.format(user_name, database_name) 
spark.sql("CREATE DATABASE IF NOT EXISTS {} LOCATION '{}'".format(dbutils.widgets.get('DatabaseName'), table_location))

# COMMAND ----------

spark.sql("USE {}".format(dbutils.widgets.get("DatabaseName")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load data 

# COMMAND ----------

df = (spark.read
  .format("delta")
  .load("/mnt/training/airbnb/sf-listings/sf-listings-2019-03-06-clean.delta/")
  .withColumn("index", monotonically_increasing_id())
)

display(df)

# COMMAND ----------

df.write.saveAsTable("airbnb_training_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM airbnb_training_data

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE airbnb_training_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Feature Store Table
# MAGIC 
# MAGIC 
# MAGIC Next, we can create the Feature Table using the `create_feature_table` method.
# MAGIC 
# MAGIC This method takes a few parameters as inputs:
# MAGIC * `name`- A feature table name of the form ``<database_name>.<table_name>``
# MAGIC * `keys`- The primary key(s). If multiple columns are required, specify a list of column names.
# MAGIC * `features_df`- Data to insert into this feature table.  The schema of `features_df` will be used as the feature table schema.
# MAGIC * `schema`- Feature table schema. Note that either `schema` or `features_df` must be provided.
# MAGIC * `description`- Description of the feature table
# MAGIC * `partition_columns`- Column(s) used to partition the feature table.

# COMMAND ----------


fs = feature_store.FeatureStoreClient()

help(fs.create_feature_table)


# COMMAND ----------

table_name = '{}.rac_airbnb_data'.format(database_name)

# COMMAND ----------

fs.create_feature_table(
    name=table_name,
    keys=["index"],
    features_df=df,
    partition_columns=["neighbourhood_cleansed"],
    description="Original Airbnb data"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Altering our feature table 

# COMMAND ----------

display(airbnb_df.groupBy("bed_type").count())

# COMMAND ----------

airbnb_df_real_beds = airbnb_df.filter("bed_type = 'Real Bed'")

display(airbnb_df_real_beds)

# COMMAND ----------

# DBTITLE 1,Merge the results
# Now that we have filtered some of our data, we can `merge` the existing feature table in the Feature Store with the new table. Merging updates the feature table schema, and adds new feature values based on the primary key.

fs.write_table(
  name=table_name,
  df=airbnb_df_real_beds,
  mode="merge"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Lastly, we'll condense some of the review columns, we'll do this by finding the average review score for each listing.

# COMMAND ----------

from pyspark.sql.functions import lit, expr

reviewColumns = ["review_scores_accuracy", "review_scores_cleanliness", "review_scores_checkin", 
                 "review_scores_communication", "review_scores_location", "review_scores_value"]

airbnb_df_short_reviews = (airbnb_df_real_beds
  .withColumn("average_review_score", expr("+".join(reviewColumns)) / lit(len(reviewColumns)))
  .drop(*reviewColumns)
)

display(airbnb_df_short_reviews)

# COMMAND ----------

fs.write_table(
  name=table_name,
  df=airbnb_df_short_reviews,
  mode="overwrite"
)

# COMMAND ----------

# Displays most recent table
feature_df = fs.read_table(
  name=table_name
)

display(feature_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We are going to insert data into another feature table for ml training 

# COMMAND ----------

ml_df = spark.read.parquet("/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.parquet/").withColumn("index", monotonically_increasing_id())

# COMMAND ----------

table_name = '{}.rac_clean_airbnb_ml_data'.format(database_name) 

fs.create_feature_table(
    name=table_name,
    keys=["index"],
    features_df=ml_df,
    partition_columns=["neighbourhood_cleansed"],
    description="Original Airbnb data"
)

# COMMAND ----------


