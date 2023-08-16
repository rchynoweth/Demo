# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *

# COMMAND ----------

num_cores = 64
num_parts = 3
create_data = False
num_rows_per_batch = 1000000000
batches = 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a dataset of records

# COMMAND ----------

if create_data:
  spark.sql("drop table if exists temp_string_table")
  rdd = sc.parallelize(["hello , world!][]  '"] * num_rows_per_batch)
  df = rdd.map(lambda x: (x, )).toDF(["value"])
  for i in range(0, batches):
    df.write.mode('append').saveAsTable('temp_string_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize temp_string_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Original UDF - no repartition

# COMMAND ----------

def rog_string_to_array_udf(rog_string):
    if rog_string is not None:
        rog_string = rog_string.replace('\n', '').replace(' ', '').strip('[').strip(']').replace('"', '').replace("'", "").split(',')
    else:
        rog_string = ['ALL']
    return rog_string

rog_UDF = F.udf(lambda z:rog_string_to_array_udf(z),ArrayType(StringType()))

# COMMAND ----------

udf_df = spark.read.table('temp_string_table')

# COMMAND ----------

udf_df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists temp_udf_string_table

# COMMAND ----------

(udf_df
 .withColumn('new_string', rog_UDF(F.col('value')))
 .write
 .mode('overwrite')
 .saveAsTable('temp_udf_string_table')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp_udf_string_table 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Original UDF - with repartition

# COMMAND ----------

udf_df2 = spark.read.table('temp_string_table')
udf_df2 = udf_df2.repartition(num_cores*num_parts)

# COMMAND ----------

udf_df2.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists temp_udf_string_table2

# COMMAND ----------

(udf_df2
 .withColumn('new_string', rog_UDF(F.col('value')))
 .write
 .mode('overwrite')
 .saveAsTable('temp_udf_string_table2')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp_udf_string_table2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Functions - no repartition

# COMMAND ----------

spark_df = spark.read.table('temp_string_table')

# COMMAND ----------

spark_df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists temp_spark_string_table

# COMMAND ----------

(spark_df
 .withColumn("new_string", F.when(F.col("value").isNull(), F.array(F.lit("ALL")))
             .otherwise(F.split(F.regexp_replace("value", r"[ '\"\[\]\n]", ""), ",")))
 .write
 .mode('overwrite')
 .saveAsTable('temp_spark_string_table')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp_spark_string_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Functions with More Partitions

# COMMAND ----------

spark_df2 = spark.read.table('temp_string_table')
spark_df2 = spark_df2.repartition(num_cores*num_parts)

# COMMAND ----------

spark_df2.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists temp_spark_string_table2

# COMMAND ----------

(spark_df2
 .withColumn("new_string", F.when(F.col("value").isNull(), F.array(F.lit("ALL")))
             .otherwise(F.split(F.regexp_replace("value", r"[ '\"\[\]\n]", ""), ",")))
 .write
 .mode('overwrite')
 .saveAsTable('temp_spark_string_table2')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp_spark_string_table2

# COMMAND ----------


