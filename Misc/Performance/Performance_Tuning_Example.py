# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Basics and Peformance Tips 
# MAGIC 
# MAGIC ## Environment 
# MAGIC 
# MAGIC In this notebook we will use the following Cluster Configuration. Please note the cluster type and number of workers. This results in worker specifications of 320GB of memory and 40 worker cores. We will also be using not be Photon. 
# MAGIC 
# MAGIC <img src="https://racadlsgen2.blob.core.windows.net/public/PerfClusterConfig.png" />
# MAGIC 
# MAGIC 
# MAGIC ## General Tips
# MAGIC - Most of the time fewer larger instances are better than many smaller ones as it decreases shuffling across workers i.e. more partitions available per machine. 
# MAGIC - Use IO-optimized instances for faster spill storage  
# MAGIC - About 128mb max per partition 
# MAGIC - `spark.sql.shuffle.partitions` should be a factor of the cores
# MAGIC - Use instance types to enable delta cache when working with data from delta   
# MAGIC - Use Databricks pools whenever possible/economical for faster autoscaling:
# MAGIC   - Databricks does not charge for idle instances (~50% cost savings)
# MAGIC   - Faster scaling of auto-scaling clusters (both job and interactive clusters)
# MAGIC   - The pools will essentially allow users to have their own driver but then share a pool of warm machines. 
# MAGIC - Use auto-scaling and auto terminate for development 
# MAGIC   - You can use autoscaling on jobs but it is usually better to have a fixed number of workers to avoid time wasted on scaling unless it is a very long running job then the economics tend to work out. 
# MAGIC - Tagging on clusters is crucial to proving the cost-value of the solution 
# MAGIC 
# MAGIC 
# MAGIC Cluster sizing is entirely job based. Start your cluster size by calculating the input data size to compute number of cores. Analyzing the query plan during development will give a better idea of further cluster requirements for the job. Most tuning is done by optimizing partitioning and cache which Databricks handles caching pretty well, especially with Delta. 
# MAGIC 
# MAGIC ## Helpful Resources:
# MAGIC - [Spark Summit 2019 Talk on Optimization](https://www.youtube.com/watch?v=daXEp4HmS-E)  
# MAGIC - [Azure Databricks Pools Best Practices](https://docs.microsoft.com/en-us/azure/databricks/clusters/instance-pools/pool-best-practices)  
# MAGIC - [Spark: The Definitive Guide](https://www.amazon.com/Spark-Definitive-Guide-Processing-Simple/dp/1491912219)  
# MAGIC   - Written by the original creator of Apache Spark and provides a ton of information on how Spark works and best development practices  
# MAGIC - [RDD Whitepaper](https://people.csail.mit.edu/matei/papers/2012/nsdi_spark.pdf)  
# MAGIC   - Chapter 5 talks about how Spark was implemented and the process it takes to execute transformations.  
# MAGIC 
# MAGIC ## Spark UI
# MAGIC 
# MAGIC Please keep in mind the following:
# MAGIC 1. Application: all operations in a Spark session. Spark clusters can have many applications  
# MAGIC 1. Jobs: applications have spark jobs  
# MAGIC 1. Stages: spark jobs have many stages  
# MAGIC 1. Tasks: stages can many tasks and tasks typically map to the number of partitions in the dataframe  
# MAGIC 
# MAGIC There are multiple tabs available for various aspects of monitoring jobs in Spark:
# MAGIC - Jobs: refers to Spark Jobs   
# MAGIC - Stages: pertains to individual stages and the relevant tasks  
# MAGIC - Storage: includes information and the data that is currently cached in the application   
# MAGIC - Environment: information about configurations and seetings for the current spark application  
# MAGIC - SQL / DataFrame: Information regarding the Structured APIs  
# MAGIC - Executors: information about each executor running our application   

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("schema", "")
dbutils.widgets.text("scope", "")
dbutils.widgets.text("sf_wh", "")
dbutils.widgets.text("sf_schema", "")
dbutils.widgets.text("sf_db", "")

# COMMAND ----------

schema_name = dbutils.widgets.get("schema")
spark.sql(f"USE {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Architecture 101

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distributed Data 
# MAGIC 
# MAGIC The basic data structure in Spark is known as a Resilent Distributed Dataset (RDD). RDDs are scala objects that can be distributed across a cluster of machines for parallel processing. Prior to Spark 2.x RDDs were to only way to operate and transform data. Nowadays users should be using Spark SQL and Dataframes. Engineers should have a very good reason for using RDDs over Spark Structured APIs. 
# MAGIC 
# MAGIC A [dataframe](https://www.databricks.com/glossary/what-are-dataframes) is a higher level object that sits on top of RDDs. Dataframes allow users to operate on data in similar ways as one would with other dataframe libraries (i.e. pandas) but in a distributed fashion. Dataframes allow users to operate on sets of columns and rows and enforce data types on the data. 
# MAGIC 
# MAGIC Both RDDs and Dataframes are stored in-memory on the worker nodes of the cluster. If all the data cannot fit in-memory then it will spill to disk, but data spill should be avoided to optimize performance.  
# MAGIC 
# MAGIC In the below image, the left side is represents a non-distributed dataset which are things like a pandas dataframe or excel spreadsheet. The right side of the image represents data distributed across machines in a data center which represents a spark dataframe:
# MAGIC <br></br>
# MAGIC <img src="https://www.databricks.com/wp-content/uploads/2018/05/DataFrames.png" />
# MAGIC 
# MAGIC 
# MAGIC Note that dataframes are a subset of an object called a dataset. For the purposes of our discussion we will skip talking about that. Please refer to The Spark Definitive Guide for more details. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shuffles 
# MAGIC 
# MAGIC As we discussed in the distributed data section of this notebook, when working in Apache Spark data is stored in-memory on the worker nodes of the cluster. Data transformations occur on each node in parallel and only on the data that available on that machine. This means that if one wants to perform a wide transformation then that data will need to be available on that machine. In order to achieve this users can **shuffle** data between workers which is the process of moving rows of data to different machines. 
# MAGIC 
# MAGIC A shuffle will be evident in the Spark UI when you see an exchange and a "partitioning" substring i.e. `RoundRobinPartitioning`, `hashpartitioning`.  
# MAGIC 
# MAGIC Shuffling data is generally one of the most expensive operations in Spark because of disk I/O, network I/O, and data (de)serialization . While there are a lot of details in shuffling, one of the more important parameters to know is the `spark.sql.shuffle.partitions` which has a default value of 200. This parameter determines the number of shuffle partitions which represents the amount of parallelism for a transformation. The number of shuffle partitions determines the number of partitions at the end of each stage. This should idea
# MAGIC 
# MAGIC Please see below for a visual representation of a shuffle:
# MAGIC <br></br>
# MAGIC <img src="https://i0.wp.com/0x0fff.com/wp-content/uploads/2015/08/Spark-Shuffle-Design.png?w=317&ssl=1" />
# MAGIC 
# MAGIC 
# MAGIC Wide transformations causes data to shuffle. Wide transformations are when an input partition contributes data to multiple output partitions. Some examples are: The `groupByKey()`, `reduceByKey()`, `join()`, `distinct()`, and `intersect()`. 
# MAGIC 
# MAGIC 
# MAGIC For more information on transformations check out this [documentation](https://www.databricks.com/glossary/what-are-transformations). 
# MAGIC 
# MAGIC 
# MAGIC #### Shuffle Read and Write 
# MAGIC 
# MAGIC Shuffling data on a cluster is the process of moving data from one CPU to another. This requires the machines to write the data to disk (shuffle write) so that it can then be read (shuffle read) onto another machine and placed on another CPU. This occurs between stages in a job and is often represented in a data "exchange" in the query plan. The more data shuffled results in slower performance. Understanding how much data you are processing is important to understand if there is too much shuffle read/write. For example, if I am working with 1GB of data but shuffling writing 10GB then there is an issue. Where as if I am working with 1TB and shuffling 10GB then that should be seen as a smaller amount. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading and Writing Data 
# MAGIC 
# MAGIC Data solutions typically begin with **READING** data and end with **WRITING** data. 
# MAGIC 
# MAGIC We will discuss the impacts of reading data from Cloud Storage (json files) and reading from Snowflake. We will briefly talk about 
# MAGIC 
# MAGIC Generally when reading data fewer larger files are typically faster, however, you may lose the ability for precision transformation (i.e. only updating a single partition). To write data faster you will generally want more partitions so that you can write in parallel. Keep in mind that overdoing partitions in either case can cause performance issues and should be balanced. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### JSON
# MAGIC Reading JSON files from cloud storage. 

# COMMAND ----------

raw_json_path = "/databricks-datasets/structured-streaming/events"

# COMMAND ----------

json_files = dbutils.fs.ls(raw_json_path)
json_files[0:5]

# COMMAND ----------

print(f"----> We have {len(json_files)} json files.")

# COMMAND ----------

json_df = spark.read.json(raw_json_path)

# COMMAND ----------

print(f"----> We have {json_df.rdd.getNumPartitions()} partitions.")

# COMMAND ----------

# DBTITLE 0,Trigger an Action
json_df.count()

# COMMAND ----------

json_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a looki at the Spark UI. Notice that the json fils are read into 25 partitions. Each partition calculates the row count for itself and outputs to the next stage. The output of the 25 partition counts then gets aggregated into a single result. 
# MAGIC 
# MAGIC <img src="https://racadlsgen2.blob.core.windows.net/public/ExampleCountSparkUI.png" width="500"/>

# COMMAND ----------

# MAGIC %md
# MAGIC What happens when I save the data to Delta?

# COMMAND ----------

json_df.write.mode("overwrite").saveAsTable("event_json")

# COMMAND ----------

delta_json_location = spark.sql("DESC EXTENDED event_json").filter(col("col_name") == "Location").select("data_type").collect()[0][0]
json_table_files = dbutils.fs.ls(delta_json_location)
json_table_files[0:5]

# COMMAND ----------

print(f"----> The table has {len(json_table_files)-1} files corresponding to the number of partitions in our dataframe")

# COMMAND ----------

# MAGIC %md
# MAGIC ### CSV
# MAGIC 
# MAGIC CSV files are splittable and you will see that Spark will split the single CSV file into many partitions. Spark will attempt to distribute the data evenly between the different cores on the cluster. 

# COMMAND ----------

csv_df = spark.read.csv("/databricks-datasets/flights/departuredelays.csv", header=True)
csv_df.count()

# COMMAND ----------

print(f"----> We have {csv_df.rdd.getNumPartitions()} partitions.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Lake
# MAGIC 
# MAGIC Fully Open Source Delta Lake. Support for many different compute engines. This means you do not need to use Databricks to access your data. 

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS json_delta_table")
spark.sql("DROP TABLE IF EXISTS join_agg_delta_example")

# COMMAND ----------

for i in range(0,30):
  (
    json_df.withColumn("id", monotonically_increasing_id())
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("json_delta_table")
  )

# COMMAND ----------

delta_json_location = spark.sql("DESC EXTENDED json_delta_table").filter(col("col_name") == "Location").select("data_type").collect()[0][0]
json_table_files = dbutils.fs.ls(delta_json_location)
print(f"----> Number of files {len(json_table_files)-1}")

# COMMAND ----------

assert len(json_table_files)-1 == (30*json_df.rdd.getNumPartitions())

# COMMAND ----------

delta_df = spark.sql("SELECT * FROM json_delta_table")
display(delta_df)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE json_delta_table2
AS 
SELECT * FROM json_delta_table LIMIT 10000
""")

# COMMAND ----------

delta_df2 = spark.sql("SELECT * FROM json_delta_table2")
display(delta_df2)

# COMMAND ----------

print(f"----> Number of partitions {delta_df.rdd.getNumPartitions()}")
print(f"----> Number of partitions {delta_df2.rdd.getNumPartitions()}")

# COMMAND ----------

(
delta_df.join(delta_df2, on='id', how='inner')
  .select("id")
  .groupBy("id")
  .count()
  .write
  .format('delta')
  .saveAsTable("join_agg_delta_example")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Broadcast Joins**
# MAGIC 
# MAGIC A broadcast joins are used when you have a smaller dataframe that you would like to join to a larger dataframe. The smaller dataframe is stored in memory and whole on all of the executors making it easily accessible for all the partitions on the machine without requiring a shuffle on the larger dataframe. In the case above we could have used a broadcast join since `delta_df2` is smaller and can likely fit into memory. This would look like the following:
# MAGIC ```
# MAGIC (delta_df.join(broadcast(delta_df2), on='id', how='inner')
# MAGIC   .select("id")
# MAGIC   .groupBy("id")
# MAGIC   .count()
# MAGIC   .write
# MAGIC   .format('delta')
# MAGIC   .saveAsTable("join_agg_delta_example")
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Snowflake
# MAGIC 
# MAGIC Propritery data storage. This means to access your data in Snowflake you must use and pay for Snowflake compute. 
# MAGIC 
# MAGIC For the purpose of this demo we will be using a Small Snowflake warehouse and we will use the [Spark Connector for Snowflake](https://docs.snowflake.com/en/user-guide/spark-connector.html). Please note that the connector may have issues with long running tasks, so if you are trying to read and write larger amounts (greater than 1TB) it may be better to write data to cloud storage then use Snowpipe to load the data from the external location in order to decouple the process on Databricks with the process on Snowflake.      
# MAGIC 
# MAGIC 
# MAGIC Limitations of Snowflake:  
# MAGIC 1. Data is not cached in Databricks for a Snowflake data source. 
# MAGIC     - Data should be cached in Snowflake so it doesn't need to be re-read from Snowflakes internal storage 
# MAGIC     - Even with data cached in Snowflake, the data will still need to be transferred from Snowflake to Databricks. Anecdotally I have seen about 5GB/min transfer to a Snowflake stage (i.e. not using Databricks). 
# MAGIC     - This limitation is further highlighted when multiple people are reading the same data and each person is repeatedly reading that data. 
# MAGIC     - RECOMMENDATION - create Delta tables in ADLS for intermediate datasets or datasets that are read multiple times. 
# MAGIC 1. DataFrames will have a single partition when read.  
# MAGIC     - Generally, if you immediately repartitioning your dataframe will improve performance for the entire job.  

# COMMAND ----------

sf_user = dbutils.secrets.get(dbutils.widgets.get("scope"), "snowflake_user")
sf_password = dbutils.secrets.get(dbutils.widgets.get("scope"), "snowflake_password")
sf_url = dbutils.secrets.get(dbutils.widgets.get("scope"), "snowflake_url")
sf_db = dbutils.widgets.get("sf_db")
sf_schema = dbutils.widgets.get("sf_schema")
sf_wh = dbutils.widgets.get("sf_wh")

# COMMAND ----------

sfOptions = {
    "sfURL": sf_url,
    "sfUser": sf_user,
    "sfPassword": sf_password,
    "sfDatabase": sf_db,
    "sfSchema": sf_schema,
    "sfWarehouse": sf_wh
}

# COMMAND ----------

(
  json_df.write
  .format("net.snowflake.spark.snowflake")
  .options(**sfOptions)
  .option("dbtable", "json_data")
  .mode("overwrite")
  .save()
)

# COMMAND ----------

snowflake_df = (
  spark.read.format("net.snowflake.spark.snowflake")
  .options(**sfOptions)
  .option("query",  "SELECT * FROM json_data")
  .load()
)
display(snowflake_df)

# COMMAND ----------

snowflake_df.count()

# COMMAND ----------

snowflake_df.count()

# COMMAND ----------

snowflake_df.rdd.getNumPartitions()

# COMMAND ----------

for i in range(0,20):
  (
  snowflake_df.write
  .format("net.snowflake.spark.snowflake")
  .options(**sfOptions)
  .option("dbtable", "json_data2")
  .mode("append")
  .save()
  )

# COMMAND ----------

sdf = (
  spark.read.format("net.snowflake.spark.snowflake")
  .options(**sfOptions)
  .option("query",  "SELECT * FROM json_data2")
  .load()
)
display(sdf)

# COMMAND ----------

# DBTITLE 1,NOTICE - A SINGLE PARTITION
print(f"----> Number of partitions {sdf.rdd.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark UI Tips
# MAGIC 
# MAGIC In this section we will use a Spark Job from above. When we execute the command below it is broken into 6 stages. 
# MAGIC 
# MAGIC <img src="https://racadlsgen2.blob.core.windows.net/public/Perf-NotebookSparkJob.png" width=500/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Jobs Tab
# MAGIC 
# MAGIC 
# MAGIC Here is what the jobs tab looks like.  
# MAGIC <img src="https://racadlsgen2.blob.core.windows.net/public/PerfJobsTab.png" />
# MAGIC 
# MAGIC Why are there 6 jobs to write this data?
# MAGIC 1. Read delta table 1
# MAGIC 1. Read delta table 2 
# MAGIC 1. Join tables
# MAGIC 1. Select columns 
# MAGIC 1. Group dataframe 
# MAGIC 1. Count
# MAGIC 
# MAGIC If these operations in different cells then the "description" column will specify the exact code. In our case, we put everything in a single cell. 
# MAGIC 
# MAGIC To over simplify this tab will tell you two things:
# MAGIC 1. The "event timeline" will give you an idea of events that have occured i.e. workers being added (important in autoscaling)
# MAGIC 1. Get an undertanding of each jobs duration - helps answer if there is a specific area in the code that is taking longer than expected/others

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stages Tab
# MAGIC 
# MAGIC The stages tab is very similar to the jobs tab and provides a similar view. Ultimately, we are looking for any stages that are taking long than the rest. If we are not satisfied with the processing time of a particular job then we will want to focus on the longest running code segments to reduce that time. Please note that skipped stages are not listed here.  
# MAGIC 
# MAGIC 
# MAGIC <img src="https://racadlsgen2.blob.core.windows.net/public/PerfStagesTab.png"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL and DataFrame Tab
# MAGIC 
# MAGIC 
# MAGIC This is one of the more tab to understand performance. In our case we joined two dataframes, grouped, and aggregated the data which means the likely longest part of the job could be centered around the join. If I look at the stages and go into the longest one you will find the below query plan.  
# MAGIC 
# MAGIC <br></br>
# MAGIC <img src="https://racadlsgen2.blob.core.windows.net/public/PerSQLTab-Join.png" width=400/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster Utilization 
# MAGIC 
# MAGIC The best way to understand how much of your cluster you are using is to take a look at the Ganglia Metrics. Below is a capture of a 15 minute segnment from running this notebook. Ganglia is a web package that is widely used for more than just Spark, please reference this [page](https://flylib.com/books/en/1.535.1.143/1/) for more detailed information.  
# MAGIC 
# MAGIC There are three main areas in this Ganglia report: 
# MAGIC 1. Server Load Distribution - evaluate partitioning strategy 
# MAGIC     - The square on the left side of the page contains squares within it to visualize the distribution of data across the cluster with each sub-square representing a machine in the cluster. The goal is to avoid any red squares. Absent of red you can assume your cluster is balanced relatively well.  
# MAGIC     - Tip - while this may seem obvious, you should use this to determine if your partitioning strategy is creating an inbalanced dataset which will result in less than ideal performance.
# MAGIC     - When you see a node turn red it means that the per-CPU load average on a cluster is greater than 1. This means that the process is processor bound. 
# MAGIC 1. Overview of Cluster  - evaluate cluster size
# MAGIC     - This visual provides a high level view of cluster capacity and usage. The job that we ran previously was pretty small so you can see that we are under utilizing our cluster. This can generally be used to size your cluster
# MAGIC 1. Stacked Graph - detailed view of nodes  
# MAGIC     - Use this visual to analyze if there is anything specific going on in the cluster. Generally, you can use the server load distribution and the stacked graphs to see if data skew is causing an increase in resource utilization on a particular node. 
# MAGIC     - Note that `load_one` is the default metric and represents the average cpu load over a minute. The current system load is the number of processes that are running on the system plus the number of processes that are waiting to run.
# MAGIC 
# MAGIC <br></br>
# MAGIC <img src="https://racadlsgen2.blob.core.windows.net/public/PerfGangliaMetrics.png"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Out of Memory Errors  
# MAGIC 
# MAGIC In Spark there are driver and worker nodes. Worker nodes have executors and executors can perform multiple tasks in parallel. As an example, if I have a 5 node (4 workers and 1 driver) cluster, with 8 CPUs each. Then I will have 4 executors and each executor will be able to perform 8 tasks in parallel. Databricks will have a single executor per a node which is considered a best practice, and is worth noting that in OSS Spark you can configure this. Finally, the executor memory is shared across all tasks on the executor. 
# MAGIC 
# MAGIC Note - out of memory errors occur when an executor/driver RAM and Disk are full. 
# MAGIC 
# MAGIC Common Scenarios:  
# MAGIC 1. Collecting and working with data on the driver  
# MAGIC 1. Broadcast Joins (driver and worker machines) 
# MAGIC 1. Data is simply to large for your cluster 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC When users collect larger amounts of data on the driver (e.g. `.collect()`). Collecting data data on the driver should only be performed when absolutely required. Developers can avoid driver memory issues by not collecting data on the driver or by setting the `spark.driver.maxResultSize`. 
# MAGIC 
# MAGIC Broadcast joins is the process of replicating a dataset on all the executors so that the data is readily available and avoids a shuffle. When you broadcast join the data is first materialized on the driver and then transferred to the workers. If the dataset is too larger for either the driver or the executors then you can receive a memory error. Most of the time the driver will fail before the executors because the driver is more likely equal to or smaller than executor memory. 
# MAGIC 
# MAGIC When working with data it is important to only read the data required for the job. You can limit the amount of data by selecting only the columns you need and filtering the data when possible. Ultimately if you have a lot of data to process then you may need to increase your cluster size (machine size and number).   
# MAGIC 
# MAGIC 
# MAGIC This is unlikely to happen in Databricks due to the 1-1 mapping, but in OSS Spark if you have too many tasks running on your executors then the metadata for running tasks in parallel can cause memory issues. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Spill 
# MAGIC 
# MAGIC Spill is the process of data moving from in-memory to disk and vice versa. The goal of data spills is to ensure resiliancy so that the job doesn't fail if RAM is full and is used as a last resort to avoid a memory error. 
# MAGIC 
# MAGIC In general, if there is a lot a data being spilled to disk then either you need to increase memory requirements on your cluster or need to find code optimizations that can reduce your memory requirements. Data spills negatively impact performance. It essentially boils down to ensuring that there is less data (less than 128MB) per a partition. 
# MAGIC 
# MAGIC Two ways to avoid data spill:
# MAGIC - Data spill is common when shuffles occur, therefore, increasing `spark.sql.shuffle.partitions`.  
# MAGIC - Modify the number of partitions in the dataframe using a `repartition`.  
