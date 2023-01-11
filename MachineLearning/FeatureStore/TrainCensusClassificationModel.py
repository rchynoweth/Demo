# Databricks notebook source
# MAGIC %md
# MAGIC # Train Census Classification Model
# MAGIC 
# MAGIC In this example we source data from our Census Data table stored in our feature store, and create a classification model to predict the income category of the individual. We will then show how batch inference can be done using the feature store.  

# COMMAND ----------

dbutils.widgets.text("DatabaseName", "rac_demo_db")
spark.sql("USE {}".format(dbutils.widgets.get("DatabaseName")))

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import OneHotEncoder
from databricks import feature_store

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

# COMMAND ----------

# DBTITLE 1,Load datasets from feature store
df = fs.read_table(name="{}.census_data_features".format(dbutils.widgets.get("DatabaseName")) )

# COMMAND ----------

display(df)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


