# Databricks notebook source
# MAGIC %md 
# MAGIC # Feature Engineering
# MAGIC 
# MAGIC From Delta table to Feature Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Churn Prediction Feature Engineering
# MAGIC 
# MAGIC <img src="https://github.com/RafiKurlansik/laughing-garbanzo/blob/main/step1.png?raw=true">

# COMMAND ----------

dbutils.widgets.text("DatabaseName", "rac_demo_db")
database_name = dbutils.widgets.get("DatabaseName")
spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))
spark.sql("USE {}".format(database_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Featurization Logic
# MAGIC 
# MAGIC This is a fairly clean dataset so we'll just do some one-hot encoding, and clean up the column names afterward.

# COMMAND ----------

# Read into Spark
telcoDF = spark.table("bronze_customers")

display(telcoDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Using `koalas` to scale my teammates' `pandas` code.

# COMMAND ----------

from databricks.feature_store import feature_table
import databricks.koalas as ks

def compute_churn_features(data):
  
  # Convert to koalas
  data = data.to_koalas()
  
  # OHE
  data = ks.get_dummies(data, 
                        columns=['gender', 'partner', 'dependents',
                                 'phoneService', 'multipleLines', 'internetService',
                                 'onlineSecurity', 'onlineBackup', 'deviceProtection',
                                 'techSupport', 'streamingTV', 'streamingMovies',
                                 'contract', 'paperlessBilling', 'paymentMethod'],dtype = 'int64')
  
  # Convert label to int and rename column
  data['churnString'] = data['churnString'].map({'Yes': 1, 'No': 0})
  data = data.astype({'churnString': 'int32'})
  data = data.rename(columns = {'churnString': 'churn'})
  
  # Clean up column names
  data.columns = data.columns.str.replace(' ', '')
  data.columns = data.columns.str.replace('(', '-')
  data.columns = data.columns.str.replace(')', '')
  
  # Drop missing values
  data = data.dropna()
  
  return data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Compute and write features

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

churn_features_df = compute_churn_features(telcoDF)
display(churn_features_df)

# COMMAND ----------


churn_feature_table = fs.create_feature_table(
  name='{}.churn_features'.format(database_name),
  keys='customerID',
  schema=churn_features_df.spark.schema(),
  description='These features are derived from the ibm_telco_churn.bronze_customers table in the lakehouse.  I created dummy variables for the categorical columns, cleaned up their names, and added a boolean flag for whether the customer churned or not.  No aggregations were performed.'
)

fs.write_table(df=churn_features_df.to_spark(), name='{}.churn_features'.format(database_name), mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC As an alternative we could always write to Delta Lake:

# COMMAND ----------

# # Write out silver-level data to Delta lake
# trainingDF = spark.createDataFrame(training_df)

# trainingDF.write.format('delta').mode('overwrite').save(silver_tbl_path)

# # Create silver table
# spark.sql('''
#   CREATE TABLE `{}`.{}
#   USING DELTA 
#   LOCATION '{}'
#   '''.format(database_name,silver_tbl_name,silver_tbl_path))

# # Drop customer ID for AutoML
# automlDF = trainingDF.drop('customerID')

# # Write out silver-level data to Delta lake
# automlDF.write.format('delta').mode('overwrite').save(automl_tbl_path)

# # Create silver table
# _ = spark.sql('''
#   CREATE TABLE `{}`.{}
#   USING DELTA 
#   LOCATION '{}'
#   '''.format(database_name,automl_tbl_name,automl_tbl_path))

# COMMAND ----------


