# Databricks notebook source
# MAGIC %md
# MAGIC # Merge Testing on Feature Store Tables
# MAGIC
# MAGIC
# MAGIC Resources: 
# MAGIC - [Feature Table Docs](https://docs.databricks.com/machine-learning/feature-store/feature-tables.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Historical Sales Data 
# MAGIC
# MAGIC Create sample data and save it as an ordinary table. 

# COMMAND ----------

from pyspark.sql.functions import * 
from databricks import feature_store
from databricks.feature_store import feature_table, FeatureLookup
from datetime import *

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

# COMMAND ----------

dbutils.widgets.text('TargetCatalog', 'ryan_chynoweth_catalog')
dbutils.widgets.text('TargetSchema', 'main')
target_catalog = dbutils.widgets.get('TargetCatalog')
target_schema = dbutils.widgets.get('TargetSchema')

# COMMAND ----------

spark.sql(f"create catalog if not exists {target_catalog}") 
spark.sql(f'use catalog {target_catalog}')
spark.sql(f"create schema if not exists {target_catalog}.{target_schema}")
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

spark.sql('drop table if exists perf_fs_test1')
spark.sql('drop table if exists perf_fs_test2')
spark.sql('drop table if exists perf_fs_test3')
spark.sql('drop table if exists perf_fs_test4')

# COMMAND ----------

inital_features_df = spark.sql("""
  select ds
  , vendor_id
  , partition_id
  , sum(total_amount) as y 
  from time_series_source_data   
  where ds < '2019-06-01'
  group by 1,2,3 
""")

display(inital_features_df)

# COMMAND ----------

merge1 = spark.sql("""
  select ds
  , vendor_id
  , partition_id
  , sum(total_amount) as y 
  from time_series_source_data   
  where ds >= '2019-06-01' and vendor_id is not null
  group by 1,2,3
""")

merge2 = spark.sql("""
  select ds
  , vendor_id
  , partition_id
  , sum(total_amount) as y 
  from time_series_source_data   
  where ds >= '2019-06-01' and vendor_id is not null
  group by 1,2,3 
""")

merge3 = spark.sql("""
  select ds
  , vendor_id
  , partition_id
  , sum(total_amount) as y 
  from time_series_source_data   
  where ds >= '2019-06-01' and vendor_id is not null
  group by 1,2,3 
""")

merge4 = spark.sql("""
  select ds
  , vendor_id
  , partition_id
  , sum(total_amount) as y 
  from time_series_source_data   
  where ds >= '2019-06-01' and vendor_id is not null
  group by 1,2,3 
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

spark.sql('optimize perf_fs_test1')

# COMMAND ----------

fs.create_table(
    name='perf_fs_test2',
    primary_keys=["ds", 'vendor_id', 'partition_id'],
    df=inital_features_df,
    schema=inital_features_df.schema,
    description="Z Order on ds"
)

# COMMAND ----------

spark.sql('optimize perf_fs_test2 zorder by ds')

# COMMAND ----------

fs.create_table(
    name='perf_fs_test3',
    primary_keys=["ds", 'vendor_id', 'partition_id'],
    df=inital_features_df,
    schema=inital_features_df.schema,
    description="Z Order on ds and partition_id"
)

# COMMAND ----------

spark.sql('optimize perf_fs_test3 zorder by (ds, partition_id)')

# COMMAND ----------

fs.create_table(
    name='perf_fs_test4',
    primary_keys=["ds", 'vendor_id', 'partition_id'],
    df=inital_features_df,
    schema=inital_features_df.schema,
    description="Z Order on all three"
)

# COMMAND ----------

spark.sql('optimize perf_fs_test4 zorder by (ds, partition_id, vendor_id)')

# COMMAND ----------

start_time = datetime.utcnow()
fs.write_table(
  name=f'{target_schema}.perf_fs_test1',
  df = merge1,
  mode = 'merge'
)
print(f"---- {start_time} | {datetime.utcnow()} | {datetime.utcnow() - start_time}")

# COMMAND ----------

start_time = datetime.utcnow()
fs.write_table(
  name=f'{target_schema}.perf_fs_test2',
  df = merge2,
  mode = 'merge'
)
print(f"---- {start_time} | {datetime.utcnow()} | {datetime.utcnow() - start_time}")

# COMMAND ----------

start_time = datetime.utcnow()
fs.write_table(
  name=f'{target_schema}.perf_fs_test3',
  df = merge3,
  mode = 'merge'
)
print(f"---- {start_time} | {datetime.utcnow()} | {datetime.utcnow() - start_time}")

# COMMAND ----------

start_time = datetime.utcnow()
fs.write_table(
  name=f'{target_schema}.perf_fs_test4',
  df = merge4,
  mode = 'merge'
)
print(f"---- {start_time} | {datetime.utcnow()} | {datetime.utcnow() - start_time}")

# COMMAND ----------

# MAGIC %md
# MAGIC Redo the baseline to make sure the perf is correct

# COMMAND ----------

fs.drop_table(
  name=f'{target_schema}.perf_fs_test1'
)

# COMMAND ----------

fs.create_table(
    name='perf_fs_test1',
    primary_keys=["ds", 'vendor_id', 'partition_id'],
    df=inital_features_df,
    schema=inital_features_df.schema,
    description="No Z Order"
)

# COMMAND ----------

spark.sql('optimize perf_fs_test1')

# COMMAND ----------

start_time = datetime.utcnow()
fs.write_table(
  name=f'{target_schema}.perf_fs_test1',
  df = merge1,
  mode = 'merge'
)
print(f"---- {start_time} | {datetime.utcnow()} | {datetime.utcnow() - start_time}")
