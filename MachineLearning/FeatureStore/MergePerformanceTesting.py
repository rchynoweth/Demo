# Databricks notebook source
# MAGIC %md
# MAGIC # Merge Testing on Feature Store Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Historical Sales Data 
# MAGIC
# MAGIC Create sample data and save it as an ordinary table. 

# COMMAND ----------

from pyspark.sql.functions import * 
from databricks import feature_store
from databricks.feature_store import feature_table, FeatureLookup

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

# COMMAND ----------

dbutils.widgets.text('TargetCatalog', 'ryan_chynoweth_catalog')
dbutils.widgets.text('TargetSchema', 'main')
target_catalog = dbutils.widgets.get('TargetCatalog')
target_schema = dbutils.widgets.get('TargetSchema')

# COMMAND ----------

spark.sql(f"create catalog if not exists {target_catalog}")
spark.sql(f"create schema if not exists {target_catalog}.{target_schema}")
spark.sql(f'use catalog {target_catalog}')
spark.sql(f'use schema {target_schema}')

# COMMAND ----------

df = (spark
      .read
      .format('csv')
      .option('header', 'true')
      .option('inferSchema', 'true')
      .load('/databricks-datasets/nyctaxi/tripdata/yellow')
      .withColumn("ds", to_date(col('pickup_datetime')))
      .withColumn("partition_id", (monotonically_increasing_id() % 400).cast("integer"))
)


# COMMAND ----------

(
  df.write
  .format('delta')
  .saveAsTable('time_series_source_data')
)

# COMMAND ----------

spark.sql('optimize time_series_source_data')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Four Target Feature Store Tables 
# MAGIC
# MAGIC We will create four feature store tables where we will withhold two days worth of data. Each table will have different z-order columns to see if this impacts performance. 

# COMMAND ----------

inital_features_df = spark.sql("""
  select ds
  , vendor_id
  , partition_id
  , sum(total_amount) as y 
  from time_series_source_data   
  where ds < '2019-06-01'
  group by all 
""")

display(inital_features_df)

# COMMAND ----------

merge1 = spark.sql("""
  select ds
  , vendor_id
  , partition_id
  , sum(total_amount) as y 
  from time_series_source_data   
  where ds >= '2019-06-01'
  group by all 
""")

merge2 = spark.sql("""
  select ds
  , vendor_id
  , partition_id
  , sum(total_amount) as y 
  from time_series_source_data   
  where ds >= '2019-06-01'
  group by all 
""")

merge3 = spark.sql("""
  select ds
  , vendor_id
  , partition_id
  , sum(total_amount) as y 
  from time_series_source_data   
  where ds >= '2019-06-01'
  group by all 
""")

merge4 = spark.sql("""
  select ds
  , vendor_id
  , partition_id
  , sum(total_amount) as y 
  from time_series_source_data   
  where ds >= '2019-06-01'
  group by all 
""")

# COMMAND ----------

fs.create_table(
    name='perf_fs_test1',
    primary_keys=["ds", 'vendor_id', 'partition_id'],
    df=inital_features_df,
    schema=inital_features_df.schema,
    description="No Z Order"
)

# COMMAND ----------

fs.create_table(
    name='perf_fs_test2',
    primary_keys=["ds", 'vendor_id', 'partition_id'],
    df=inital_features_df,
    schema=inital_features_df.schema,
    description="Z Order on ds"
)

# COMMAND ----------

fs.create_table(
    name='perf_fs_test3',
    primary_keys=["ds", 'vendor_id', 'partition_id'],
    df=inital_features_df,
    schema=inital_features_df.schema,
    description="Z Order on ds and partition_id"
)

# COMMAND ----------

fs.create_table(
    name='perf_fs_test4',
    primary_keys=["ds", 'vendor_id', 'partition_id'],
    df=inital_features_df,
    schema=inital_features_df.schema,
    description="Z Order on all three"
)

# COMMAND ----------


