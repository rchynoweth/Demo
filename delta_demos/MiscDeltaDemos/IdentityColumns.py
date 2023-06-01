# Databricks notebook source
spark.sql("drop table if exists ruleTable")

# COMMAND ----------

spark.sql("""
          create table ruleTable(

  id bigint GENERATED ALWAYS AS identity (start with 0 increment by 1),

  pipelineName string,

  schemaName STRING,

  tableName string,

  constraint string,

  tag string,

  catalogName string,

  ignoreRule boolean,

lastRead timestamp

)
          """)

# COMMAND ----------

display(spark.read.table('ruleTable'))

# COMMAND ----------

from datetime import datetime 
from pyspark.sql.types import *

dt = datetime.utcnow()

# COMMAND ----------


schema = StructType([
    StructField("pipelineName", StringType(), True),
    StructField("schemaName", StringType(), True),
    StructField("tableName", StringType(), True),
    StructField("constraint", StringType(), True),
    StructField("tag", StringType(), True),
    StructField("catalogName", StringType(), True),
    StructField("ignoreRule", BooleanType(), True),
    StructField("lastRead", TimestampType(), True),
])


# Define input data as a list of rows
data = [
    ('my_pipeline', 'ryan_schema', 'ryan_table', 'test_constraint', 'my_tag', 'ryan_catalog', True, dt)
]

# Create Spark DataFrame from list of rows and schema object
df = spark.createDataFrame(data, schema=schema)
display(df)

# COMMAND ----------

df.write.option('mergeSchema', 'true').mode('append').saveAsTable('ruleTable')

# COMMAND ----------

display(spark.read.table('ruleTable'))

# COMMAND ----------

df.write.option('mergeSchema', 'true').mode('append').saveAsTable('ruleTable')
df.write.option('mergeSchema', 'true').mode('append').saveAsTable('ruleTable')
df.write.option('mergeSchema', 'true').mode('append').saveAsTable('ruleTable')
df.write.option('mergeSchema', 'true').mode('append').saveAsTable('ruleTable')
df.write.option('mergeSchema', 'true').mode('append').saveAsTable('ruleTable')

# COMMAND ----------

display(spark.read.table('ruleTable'))

# COMMAND ----------

df.write.mode('append').saveAsTable('ruleTable')

# COMMAND ----------

display(spark.read.table('ruleTable'))

# COMMAND ----------


