# Databricks notebook source
# MAGIC %md
# MAGIC # Cluster Tuning and Sizing

# COMMAND ----------

# MAGIC %md
# MAGIC General Rules:
# MAGIC - Fewer big instances are better than many smaller ones since this reduces shuffling across workers i.e. more partitions available per machine. 
# MAGIC - Use IO-optimized instances for faster spill storage
# MAGIC - Number of cores should be at most inputSize/128mb
# MAGIC - Spark.sql.shuffle.partitions should be a factor of the cores
# MAGIC - On azure use the Ls_v2 and Fs instance types to enable delta cache   
# MAGIC - Use Databricks pools whenever possible:
# MAGIC   - Databricks does not charge for idle instances (~50% cost savings)
# MAGIC   - Faster scaling of auto-scaling clusters (both job and interactive clusters)
# MAGIC   - The pools will essentially allow users to have their own driver but then share a pool of warm machines. 
# MAGIC - High-concurrency clusters for multiple interactive users - or just use pools!
# MAGIC - Use auto-scaling and auto terminate 
# MAGIC - Use case tagging on clusters
# MAGIC 
# MAGIC Cluster sizing is entirely job based. Start your cluster size by calculating the input data size to compute number of cores. Analyzing the query plan during development will give a better idea of further cluster requirements for the job. 
# MAGIC 
# MAGIC Most tuning is done by optimizing partitioning and cache which Databricks handles caching pretty well, especially with Delta. 
# MAGIC 
# MAGIC Note that we are using general purpose clusters. 
# MAGIC 
# MAGIC [Spark Summit 2019 Talk on Optimization](https://www.youtube.com/watch?v=daXEp4HmS-E)
# MAGIC [Azure Databricks Pools Best Practices](https://docs.microsoft.com/en-us/azure/databricks/clusters/instance-pools/pool-best-practices)

# COMMAND ----------

# MAGIC %md
# MAGIC Load data and save to a delta table. Note we are appending many times to create a larger delta table.  

# COMMAND ----------

from delta import DeltaTable
from pyspark.sql.functions import col, monotonically_increasing_id, lit

# COMMAND ----------

delta_table = "/users/ryan.chynoweth@databricks.com/airlines_delta_table"

# COMMAND ----------

df = spark.read.format("delta").load(delta_table)

# COMMAND ----------

df2 = spark.read.format("delta").load(delta_table+"_two")
df2 = df2.withColumn("NewCol", lit(1)).select("id", "NewCol")

# COMMAND ----------

joinedDF = df.join(df2, on=['id'], how='inner')

# COMMAND ----------

dbutils.fs.rm(delta_table+"_join", True)

# COMMAND ----------

joinedDF.write.format("delta").mode("overwrite").save(delta_table+"_join")

# COMMAND ----------

# MAGIC %md
# MAGIC After looking at the Spark UI I can see the following:
# MAGIC - InputDataSize for Table One == 19.5 GB
# MAGIC - InputDatasie for Table Two == 4.5GB
# MAGIC - Number of Cores == 64
# MAGIC - Number of Shuffle Partitions == 200
# MAGIC 
# MAGIC Things to look into:
# MAGIC - 25,000mb/128mb = 195 max cores (we are using 64 so this is fine)
# MAGIC - 25,000mb/180mb = 
# MAGIC 
# MAGIC In conclusiong our shuffle partitions is overall pretty okay. We will need to adjust it slightly but we will most likely just want to increase core count

# COMMAND ----------

df.unpersist()

# COMMAND ----------

df2.unpersist()

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 2368) # this is a factor of 64

# COMMAND ----------

dbutils.fs.rm(delta_table+"_join", True)

# COMMAND ----------

joinedDF.write.format("delta").mode("overwrite").save(delta_table+"_join")

# COMMAND ----------

# MAGIC %md
# MAGIC Increased shuffle partitions and see a 4 minute decrease in write speed. 28% performance increase!
