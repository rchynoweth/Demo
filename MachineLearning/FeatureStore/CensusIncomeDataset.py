# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Feature Store
# MAGIC 
# MAGIC Databricks Feature Store is a centralized repository of features. It enables feature sharing and discovery across your organization and also ensures that the same feature computation code is used for model training and inference.  
# MAGIC 
# MAGIC 
# MAGIC ### Why Feature Store?
# MAGIC 
# MAGIC Raw data needs to be processed and transformed before it can be used in machine learning. This process is called “feature engineering” and includes transformations such as aggregating data (for example, the number of purchases by a user in a given time window) and more complex calculations that may themselves be the result of machine learning algorithms such as word embeddings. 
# MAGIC 
# MAGIC Converting raw data into features for model training is time-consuming. Creating and maintaining feature definition pipelines requires significant effort. Teams often want to explore and leverage features created by other data scientists in the organization.  
# MAGIC 
# MAGIC Another challenge is maintaining consistency between training and serving. A feature pipeline might be created by a data scientist and reimplemented by an engineer for model serving in production. This is slow and may affect the performance of the model once deployed if data drifts. 
# MAGIC 
# MAGIC The Databricks feature store allows users to create, explore, and re-use existing features. Feature store datasets can easily be published for real-time inference. 
# MAGIC 
# MAGIC Feature tables are stored as Delta Tables, and if models are deployed with MLflow then models can automatically retrieve features from the Feature Store making retraining and scoring simple.  
# MAGIC 
# MAGIC #### Concepts
# MAGIC 
# MAGIC **Feature Table** - features are stored in delta tables and contain additional metadata information. Feature tables must have a primary key and are computed and updated using a common computation function. The table metadata tracks the data lineage of the table and all notebooks and jobs that are associated with the table.  
# MAGIC 
# MAGIC **Online Store**  - an online store is a low-latency database used for real-time model inference. The following stores are supported:  
# MAGIC - Amazon Aurora (MySQL compatible)  
# MAGIC - Amazon RDS MySQL  
# MAGIC 
# MAGIC **Offline Store** - offline feature store is used during the development process for feature discovery and training. It is also used for batch inference and contains tables that are stored as Delta Tables. This is one of the more common usages since it is ideal for batch predictions.   
# MAGIC 
# MAGIC **Streaming** - In addition to batch processing. You can write feature values to a feature table from a streaming source and the computation code can utilize structured streaming to transform raw data streams into features.  
# MAGIC 
# MAGIC **Training Set** - A training set consists of a list of features and a DataFrame containing raw training data, labels, and primary keys by which to look up features. You create the training set by specifying features to extract from the store and provide this set during the model training process.  
# MAGIC 
# MAGIC **Model packaging** - a machine learning model trained using features from Databricks feature store retains references to these features. At inference time the model can optionally retrieve feature values from the store. The caller only needs to provide the primary key of the features used in the model and the model will retrieve all features required. This is done with `FeatureStoreClient.log_model()`.   
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC #### Notes
# MAGIC - Must use Databricks Runtime 8.3 ML or above
# MAGIC - Last updated on August 17, 2021  

# COMMAND ----------

dbutils.widgets.text("DatabaseName", "rac_demo_db")
spark.sql("USE {}".format(dbutils.widgets.get("DatabaseName")))

# COMMAND ----------

# DBTITLE 1,Read data
from pyspark.sql.types import DoubleType, StringType, StructType, StructField
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.ml.feature import Imputer, StringIndexer
from databricks import feature_store



import pandas as pd

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

input_df = input_df.repartition(10)

# COMMAND ----------

# DBTITLE 1,Create a transformation function
def transform_censusdata(input_df):
  numeric_columns = ['age', 'capital_gain', 'capital_loss', 'education_num', 'fnlwgt', 'hours_per_week']
  string_columns = ['marital_status', 'native_country', 'occupation', 'race', 'relationship', 'sex', 'workclass']
  
  table_list = [t.name for t in spark.catalog.listTables(dbutils.widgets.get("DatabaseName"))]

  
  max_id = 0 if "CensusIncomeDataset" not in table_list else spark.sql("SELECT max(id) from CensusIncomeDataset").collect()[0][0]
  
  input_df = input_df.withColumn("id", monotonically_increasing_id()+max_id)

  # Fill the blank values in the numeric columns
  imputer = Imputer(
      inputCols=numeric_columns, 
      outputCols=["{}_imputed".format(c) for c in numeric_columns]
  )

  imputer_model = imputer.fit(input_df)

  df = imputer_model.transform(input_df)

  # string indexing
  si = StringIndexer()
  si.setInputCols(string_columns)
  si.setOutputCols(["{}_indexed".format(c) for c in string_columns])
  si_model = si.fit(df)
  df = si_model.transform(df)

  return df

# COMMAND ----------

df = transform_censusdata(input_df)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("CensusIncomeDataset")

# COMMAND ----------

df = spark.sql("select * from CensusIncomeDataset")

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

# COMMAND ----------

fs.create_feature_table(
    name="{}.census_data_features".format(dbutils.widgets.get("DatabaseName")),
    keys=["id"],
    features_df=df,
    description="Census Data Features",
)

# COMMAND ----------

## Schedule this command in order to write/update data
fs.write_table(
  df=df,
  name="{}.census_data_features".format(dbutils.widgets.get("DatabaseName")),
  mode='overwrite'
)

# COMMAND ----------


