# Databricks notebook source
from delta import DeltaTable
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
# from pyspark.sql.functions import to_timestamp # use this after the table is saved with a string type
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import pickle
import os
import hashlib

# COMMAND ----------

dbutils.widgets.text('Starting Directory', "/")
dbutils.widgets.text('Directory Checkpoint Location', "/dbfs/tmp/storage_checkpoint")
dbutils.widgets.dropdown('Load Checkpoint', "False", ['True', 'False'])
dbutils.widgets.text('Output Table Name', "")

starting_dir = dbutils.widgets.get('Starting Directory')
dir_checkpoint_location = dbutils.widgets.get('Directory Checkpoint Location')
load_checkpoint = dbutils.widgets.get('Load Checkpoint')
output_table_name = dbutils.widgets.get('Output Table Name')

# COMMAND ----------

## NOTICE - 
# volumes are reserved for Unity Catalog in Databricks and we will not scan those. You can provide an explicit volume as the 'starting_dir' if you wish to scan. 
# the mnts directory is the same as volumes. Please provide the mount you wish to scan as the starting_dir parameter. 
# the dbfs:/ directory should be excluded as it will have a recursive scan affect that we want to avoid. 
# lastly, there is functionality to check directories based on all lower case letters so...
#             ... if you add anything here please make sure that is the case

exclude_dirs = ['dbfs:/volumes/', 'dbfs:/volume/', 'dbfs:/', 'dbfs:/mnt/']
exclude_dirs.extend(['dbfs:'+f.mountPoint.lower() for f in dbutils.fs.mounts() if f.mountPoint.lower() != '/'])
exclude_dirs = [d + "/" if d[-1] != "/" else d for d in exclude_dirs]
exclude_dirs

# COMMAND ----------

schema = StructType([
    StructField("pathHash", StringType(), True),
    StructField("path", StringType(), True),
    StructField("name", StringType(), True),
    StructField("size", LongType(), True),
    StructField("modificationTime", LongType(), True),
    StructField("isDeltaTable", BooleanType(), True),
    StructField("errorMessage", StringType(), True),
    StructField("scanDatetime", StringType(), True)
    # StructField("catalog_name", StringType(), True),
    # StructField("schema_name", StringType(), True),
    # StructField("object_name", StringType(), True) # we will just do a join to information schema after the df is created. 
])

df = spark.createDataFrame([], schema)
display(df)

# COMMAND ----------

## when writing to a table/dir
# the directory does not get updated
# the data files get updated
# delta log is the last thing to get updated
# Essentially directory timestamps do not get updated if files underneath are created. We could get this information if needed though. 

# COMMAND ----------

def save_checkpoint(data):
  print("Saving Directory Checkpoint")
  os.makedirs(dir_checkpoint_location, exist_ok=True)
  with open(dir_checkpoint_location+"/checkpoint.pkl", 'wb') as f:
    pickle.dump(data, f)

def load_checkpoint():
  with open(dir_checkpoint_location+"/checkpoint.pkl", 'rb') as f:
    return pickle.load(f)

# COMMAND ----------


def process_directory(path):
    try: 
        files = dbutils.fs.ls(path)
        if len(files) > 10000:
            data = [Row(hashPath=hashlib.sha256(path.encode()).hexdigest(),
                        path=path, 
                        name=None, 
                        size=None, 
                        modificationTime=None, 
                        isDeltaTable=False,
                        errorMessage="Directory contains more than 10,000 total files. Please manually inspect.",
                        scanDatetime=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                    ) ]
            return data, []
        try:
            start = time.time()
            human_start = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))
            print(f"Starting Path: {path} | Listed Files - {len(files)} - {human_start}")
            data = []
            new_dirs = []
            for f in files:
                data.append(
                    Row(hashPath=hashlib.sha256(f.path.encode()).hexdigest(),
                        path=f.path, 
                        name=f.name, 
                        size=f.size, 
                        modificationTime=f.modificationTime, 
                        isDeltaTable=DeltaTable.isDeltaTable(spark, f.path),
                        errorMessage=None,
                        scanDatetime=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                    ) 
                )
                if f.isDir():
                    new_dirs.append(f.path)        
            end = time.time()
            human_end = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
            print(f"Finishing Path: {path} | New Dirs: {len(new_dirs)} | New Files: {len(files)} | {human_end} | Total Time: {end-start}")
            return data, new_dirs
        except Exception as e:
            start = time.time()
            human_start = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))
            print(f"Catching exception. Path: {path} | Listed Files - {len(files)} - {human_start}")
            for f in files:
                data.append(
                    Row(hashPath=hashlib.sha256(f.path.encode()).hexdigest(),
                        path=f.path, 
                        name=f.name, 
                        size=f.size, 
                        modificationTime=f.modificationTime, 
                        isDeltaTable=None,
                        errorMessage=str(e),
                        scanDatetime=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                    )
                )
                if f.isDir():
                    new_dirs.append(f.path)        

            end = time.time()
            human_end = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
            print(f"Finishing Path: {path} | New Dirs: {len(new_dirs)} | New Files: {len(files)} | {human_end} | Total Time: {end-start}")
            return data, new_dirs
        print(f"Returning nothing for PATH: {path}")
        return [], []
    except Exception as e:
        print(f"EXCEPTION UNKNOWN: {str(e)}")
        return [], []


# COMMAND ----------

dirs = [starting_dir]
if load_checkpoint == "True":
    dirs = load_checkpoint()


while len(dirs)>0:
    print(f"Length of directories: {len(dirs)}")
    with ThreadPoolExecutor(max_workers=32) as executor:
        save_checkpoint(dirs)
        futures = [executor.submit(process_directory, path) for path in dirs]
        outstanding = len(futures)
        tots = len(futures)
        dirs = []
        # all_data = []
        for future in as_completed(futures):
            outstanding -= 1
            print(f"Outstanding Tasks: {outstanding} | Total Tasks: {tots} ")
            data, new_dirs = future.result()
            # all_data.extend(data)
            dirs.extend([d for d in new_dirs if d.lower() not in exclude_dirs])
            # write each batch
        (
            spark.createDataFrame(data, schema)
            .write
            .mode('append')
            .saveAsTable(f'{output_table_name}')
        )
        # df = df.union(spark.createDataFrame(all_data, schema))

# COMMAND ----------

# df.coalesce(200).write.saveAsTable("rac_demo_catalog.rac_demo_db.storage_analytics_20240911")

# COMMAND ----------

# df.count()

# COMMAND ----------

# display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # TO DO
# MAGIC - Must add some form of checkpointing to be able to stop and start. 
# MAGIC - This took something like 5.5 hours and maybe I don't need to do the entire storage account to build the rest out. Find the exact time to be able to compute proper metrics. 
# MAGIC - 558,948 files and directories
# MAGIC - Look at cluster logs to see if tasks were distributed over the cluster properly

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analytics
# MAGIC

# COMMAND ----------

approximate_storage_cost_per_gb = 0.195

# COMMAND ----------

from pyspark.sql import Window
import pyspark.sql.functions as F

analytics_df = spark.sql(f"""
                         select *
                         , to_timestamp(from_unixtime(modificationTime/1000)) as lastModified
                         , concat(year(from_unixtime(modificationTime/1000)), '-', month(from_unixtime(modificationTime/1000))) as YearMonth
                         , {approximate_storage_cost_per_gb} as cost_per_gb
                         , size/1000000000 as sizeInGB
                         , (size/1000000000)*{approximate_storage_cost_per_gb} as estimatedListCost
                         from {output_table_name}
                         """)


# Define the window specification
# window_spec = Window.orderBy("id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# # Add a new column with the cumulative sum
# df_with_cumsum = df.withColumn("cumulative_sum", F.sum("value").over(window_spec))


windowSpec = Window.orderBy("lastModified").rowsBetween(Window.unboundedPreceding, Window.currentRow)
analytics_df = analytics_df.withColumn("cumulativeSizeInGB", F.sum("sizeInGB").over(windowSpec))

display(analytics_df)

# COMMAND ----------


