// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # Low Shuffle Merge
// MAGIC 
// MAGIC A Spark Shuffle is the process of redistributing or re-partitioning data between the executors in your cluster. The shuffle operation can be very expensive and should be avoided when possible, however, most complex operations require shuffling to some degree. 
// MAGIC 
// MAGIC Merging data is the process of updating, inserting, and deleting data based on a source dataset into a target dataset. Under the hood, when you do a merge in Databricks using Delta we do two Scans. 
// MAGIC 1. A scan is completed to do an inner join between the target and source to figure out which files have matches.   
// MAGIC 1. A second scan is done to do an outer join between the selected files in the target and source to write the updated/deleted/inserted data as needed.  
// MAGIC 
// MAGIC After we do our scans we have now identified which files we are going to be working with. At this point we will complete 
// MAGIC 
// MAGIC **When to use Low Shuffle Merge?**   
// MAGIC 
// MAGIC When merging data, Databricks needs to work on a per file basis to complete the operations. This includes processing all the rows that are stored in the file regardless to whether or not they are being updated. The goal of low shuffle merge optimizes the rows that do not need to be updated. Previously, the unmatched rows were being updated the same as the updated rows which required passing them through multiple stages and being shuffled. Now with the low shuffle merge we are able to process the unchanged rows without shuffles which reduces the overall cost of the operation.  
// MAGIC 
// MAGIC *It is best to use low shuffle merge when there are a small amount of records being updated*, however, you will see improvements in performance regardless due to the fact that we are able to better optimize the lay out of the data. The new merge functionality preserves the existing layout of the unmodified records which means that your performance will not degrade after a merge command and does not necessarily require an `optimize` each time. 
// MAGIC 
// MAGIC 
// MAGIC **How to enable Low Shuffle Merge?**
// MAGIC 
// MAGIC Enabling low shuffle merge is done on the session or cluster level using spark configuration. Run the following command in the notebook:
// MAGIC ```
// MAGIC %python
// MAGIC spark.sql("SET spark.databricks.delta.merge.enableLowShuffle = true")
// MAGIC ```
// MAGIC Or simple put the following into the spark configuration on your cluster
// MAGIC ```
// MAGIC spark.databricks.delta.merge.enableLowShuffle true
// MAGIC ```
// MAGIC 
// MAGIC 
// MAGIC **Resources**: 
// MAGIC - Tech Talk - [DML Internals: Delete, Update, Merge](https://databricks.com/discover/diving-into-delta-lake-talks/how-delete-update-merge-work)
// MAGIC - Merge Into [Documentation](https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-merge-into.html)
// MAGIC - Low Shuffle Merge [Blog](https://databricks.com/blog/2021/09/08/announcing-public-preview-of-low-shuffle-merge.html)
// MAGIC - Low Shuffle Merge [Documentation](https://docs.databricks.com/delta/optimizations/low-shuffle-merge.html)
// MAGIC 
// MAGIC 
// MAGIC **Note this may become the default merge at some point and is worth pointing out that I am using DBR 9.1 LTS.**
// MAGIC 
// MAGIC %md
// MAGIC 
// MAGIC ## How does a MERGE work?
// MAGIC 
// MAGIC Under the hood merge completes the individual update, delete, and insert operations depending on the data scan and predicates applied. Above we stated the following actions are required for a merge. Merging data is the process of updating, inserting, and deleting data based on a source dataset into a target dataset. Under the hood, when you do a merge in Databricks using Delta we do two Scans. 
// MAGIC 1. A scan is completed to do an inner join between the target and source to figure out which files have matches.    
// MAGIC     - This scan determines which rows are updates/deletes (i.e. these are the matching rows with predicates) and which ones are inserts (i.e. the unmatched rows)   
// MAGIC 1. A second scan is done to do an outer join between the selected files in the target and source to write the updated/deleted/inserted data as needed.   
// MAGIC     - This scan is to figure out which files need to be rewritten (updates/deletes) and which ones need to be inserted.   
// MAGIC 
// MAGIC 
// MAGIC To understand a merge you will need to understand each individual action that can occur during a merge. Below is an extremely high level explaination of each of the operations.   
// MAGIC - UPDATE: requires 2 scans   
// MAGIC   - Scan 1: Find and select which files contain data matching predicate. The predicate allows us to skip files and reduce overhead on the unchanged data/files.   
// MAGIC   - Scan 2: Read selected files and rewrite them as new files with unchanged + updated data i.e. join dataframes to update rows, write the file, and add to the delta log appropriatly 
// MAGIC - INSERT:   
// MAGIC   - INSERT operations simply write new files in order.  
// MAGIC   - This highlights why append only pipelines require table optimizations and zordering because we simply append small files to the table.  
// MAGIC - DELETE:  
// MAGIC   - DELETES are essentially an extension of updates. But instead of a join that updates the data we apply a join that DELETES the data. 
// MAGIC   
// MAGIC 
// MAGIC So how does a traditional merge work? 
// MAGIC - Traditional merges essentially work as updates where we are rewriting all data no matter what the operation is i.e. we treat inserts, updates, and deletes the same 
// MAGIC 
// MAGIC How does a low shuffle merge work? 
// MAGIC - It works like an update but instead of processing unchanged rows we pass them through the stages and simply write them out to files then process updates/deletes seperately.  

// COMMAND ----------

// MAGIC %md 
// MAGIC # LSM Demo Summary
// MAGIC * Low Shuffle Merge provides better performance by processing unmodified rows in a separate, more streamlined processing mode, instead of processing them together with the modified rows. 
// MAGIC * As a result, the amount of shuffled data is reduced significantly, leading to improved performance.
// MAGIC * Low Shuffle Merge also removes the need for users to re-run the OPTIMIZE ZORDER BY command after performing a MERGE operation. For the data that has already been sorted (using OPTIMIZE Z-ORDER BY), Low Shuffle Merge maintains that sorting for all records that are not being modified by the MERGE command. These improvements save significant time and compute costs.
// MAGIC * Learn more by going to [this][link] blog
// MAGIC * This demo will show the difference of a merge running normal Delta Lake merge versus LowShuffleMerge (LSM) _and_ compare the differences between the two
// MAGIC 
// MAGIC ## Recommended Cluster setup
// MAGIC * 3 node cluster of i3.large; using DBR 9.0+
// MAGIC * Feel free to use Photon if you would like for even greater performance
// MAGIC 
// MAGIC ## To use this notebook
// MAGIC * Modify the `basePath` in Cmd3
// MAGIC * `Run All` on top
// MAGIC 
// MAGIC ## Duration
// MAGIC * This entire notebook will take < 10 minutes to complete. 

// COMMAND ----------

// MAGIC %md # Setup the testing environment

// COMMAND ----------

// ----- change this ----- 
val basePath = "/users/ryan.chynoweth@databricks.com/lsmDemo"

// COMMAND ----------

// MAGIC %md ## Create two sample tables - normal and lsm
// MAGIC * We are going to create two identical tables so that we have consistency for the testing. This will be a deep clone so that we have two completely separate but - also - identical tables.
// MAGIC * Protip: we are using the [Delta Clone API][deltaClone] to make this happen - super fast and easy
// MAGIC 
// MAGIC [deltaClone]: https://docs.databricks.com/delta/delta-utility.html#clone-a-delta-table

// COMMAND ----------

import org.apache.spark.sql.functions._
import io.delta.tables._

val normalTable = basePath + "normal"
val lsmTable = basePath + "lsm"
val changesetNormalTable = basePath + "changesetNormal"
val changesetLsmTable = basePath + "changesetLsm"
val df = spark.range(1000000000).withColumn("colB", 'id % 20).withColumn("transactionAmt", rand(seed=10) * 1000000000)

display(df)

// COMMAND ----------

// this will take a 5-8 minutes to complete
df.write.format("delta").partitionBy("colB").save(normalTable)

val normalDeltaTable = DeltaTable.forPath(spark, normalTable)
val normalHistory = normalDeltaTable.history()

// optimize the table
sql(s"optimize delta.`$normalTable`")

// make a copy of the table and save it as the lsm table
normalDeltaTable.clone(lsmTable, isShallow=false)    
// initialize the DeltaTables
val lsmDeltaTable = DeltaTable.forPath(spark, lsmTable)
val lsmHistory = lsmDeltaTable.history()

// COMMAND ----------

// MAGIC %md ## Create a changeset
// MAGIC * We are going to create the changeset _and_ persist it into two identical tables so that we have consistency for the testing
// MAGIC * This will be a deep clone so that we have two completely separate but - also - identical tables.

// COMMAND ----------

val changeset = spark.range(10000).withColumn("sourceId", round(rand(seed=3) * 10000000)).withColumn("transactionAmt", rand(seed=10) * 1000000000).withColumn("colB", 'id % 20).dropDuplicates("sourceId")
display(changeset)

// COMMAND ----------

// write out the changeset
changeset.write.format("delta").save(changesetNormalTable)
// clone the changeset
DeltaTable.forPath(spark, changesetNormalTable).clone(changesetLsmTable, isShallow=false)
// read the persisted changesets into dataframes
val changesetNormalDf = spark.read.format("delta").load(changesetNormalTable)
val changesetLsmDf = spark.read.format("delta").load(changesetLsmTable)

// COMMAND ----------

// MAGIC %md # Merge testing

// COMMAND ----------

// MAGIC %md ## Normal Merge

// COMMAND ----------

// disable LSM
spark.conf.set("spark.databricks.delta.merge.enableLowShuffle", "false")

normalDeltaTable
  .as("target")
  .merge(
    changesetNormalDf.as("source"),
    "source.sourceId = target.id")
  .whenMatched
    .updateExpr(
      Map(
        "transactionAmt" -> "source.transactionAmt"
      )
    )
  .whenNotMatched
    .insertExpr(
      Map(
        "id" -> "source.sourceId",
        "transactionAmt" -> "source.transactionAmt",
        "colB" -> "source.colB"
      )
  )
  .execute()

// COMMAND ----------

// MAGIC %md ## Low Shuffle Merge

// COMMAND ----------

// enable LSM
spark.conf.set("spark.databricks.delta.merge.enableLowShuffle", "true")

lsmDeltaTable
  .as("target")
  .merge(
    changesetLsmDf.as("source"),
    "source.sourceId = target.id")
  .whenMatched
    .updateExpr(
      Map(
        "transactionAmt" -> "source.transactionAmt"
      )
    )
  .whenNotMatched
    .insertExpr(
      Map(
        "id" -> "source.sourceId",
        "transactionAmt" -> "source.transactionAmt",
        "colB" -> "source.colB"
      )
  )
  .execute()

// COMMAND ----------

// MAGIC %md # Compare the two methods of Merge
// MAGIC By looking at `differenceInSeconds`, we can see that LSM outperformed the normal merge by a decent amount of time; a positive number indicates that LSM was faster than the normal merge
// MAGIC * `normalMergeMs`: how long the normal merge operation took in ms
// MAGIC * `lsmMergeMs`: how long the LSM merge operation took in ms
// MAGIC * `differenceInMs`: difference between normalMergeMs and lsmMergeMs
// MAGIC * `differenceInSeconds`: previous one but in seconds
// MAGIC * `differencePercentage`: percentage that LSM is faster than normal merge
// MAGIC 
// MAGIC The different rows can be interpreted as:
// MAGIC * `executionTimeMs`: how long the total operation took
// MAGIC * `rewriteTimeMs`: of the entire operation, how long was spent during the write phase
// MAGIC * `scanTimeMs`: of the entire operation, how long was spent on the initial merge scanning phase

// COMMAND ----------

// MAGIC %fs ls /mnt/jbreese-databricks-bucket/temp/lsmDemo/

// COMMAND ----------

// get the most recent versions from the merge
val normalCurrentVersion = normalHistory.select(max('version).alias("currentVersion")).collect.mkString.drop(1).dropRight(1)
val lsmCurrentVersion = lsmHistory.select(max('version).alias("currentVersion")).collect.mkString.drop(1).dropRight(1)

val normalMergeMetrics = normalHistory.filter('version === normalCurrentVersion).select(explode($"operationMetrics")).withColumn("mergeType", lit("normal"))
val lsmMergeMetrics = lsmHistory.filter('version === lsmCurrentVersion).select(explode($"operationMetrics")).withColumn("mergeType", lit("lsm"))
// join the two together
val joinedMergeMetrics = normalMergeMetrics.as("normal").join(lsmMergeMetrics.as("lsm"), Seq("key"), "inner")
// compare just the important parts
val differences = joinedMergeMetrics.filter('key.isin("executionTimeMs", "rewriteTimeMs", "scanTimeMs"))
      .withColumn("differenceInMs", $"normal.value" - $"lsm.value")
      .withColumn("differenceInSeconds", $"differenceInMs"/1000)
      .withColumn("differencePercentage", round((($"normal.value"-$"lsm.value")/$"normal.value")*100,1))
      .withColumn("normalMergeMs", $"normal.value").withColumn("lsmMergeMs", $"lsm.value")
      .drop("mergeType", "value")
      .select("key", "normalMergeMs", "lsmMergeMs", "differenceInMs", "differenceInSeconds", "differencePercentage")

display(differences)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC import requests
// MAGIC import json
// MAGIC 
// MAGIC notebookContext = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
// MAGIC token = notebookContext.apiToken().get()
// MAGIC host = notebookContext.apiUrl().get()
// MAGIC header = {'Authorization': 'Bearer %s' % token, "User-Agent": "lsmDemo"} 
// MAGIC 
// MAGIC def getEndpoint(endpoint):
// MAGIC     response = requests.get(
// MAGIC         url='%s/api/2.0/%s' % (host, endpoint),
// MAGIC         headers = header
// MAGIC     )
// MAGIC     response.json()
// MAGIC 
// MAGIC getEndpoint("clusters/list")
