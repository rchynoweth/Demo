# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
import os
import hashlib
import json
import time

# COMMAND ----------

dbutils.widgets.text('Starting Directory', "/dbfs/")
dbutils.widgets.text('Output Table Name', "") 
dbutils.widgets.text('Number of Worker Cores', "32")

starting_dir = dbutils.widgets.get('Starting Directory')
output_table_name = dbutils.widgets.get('Output Table Name')
num_cores = int(dbutils.widgets.get("Number of Worker Cores"))

# COMMAND ----------

## NOTICE - 
# volumes are reserved for Unity Catalog in Databricks and we will not scan those. You can provide an explicit volume as the 'starting_dir' if you wish to scan. 
# the mnts directory is the same as volumes. Please provide the mount you wish to scan as the starting_dir parameter. 
# the dbfs:/ directory should be excluded as it will have a recursive scan affect that we want to avoid. 
# lastly, there is functionality to check directories based on all lower case letters so...
#             ... if you add anything here please make sure that is the case

exclude_dirs = ['/dbfs/volumes/', '/dbfs/volume/', '/dbfs/', '/dbfs/mnt/']
exclude_dirs.extend(['/dbfs'+f.mountPoint.lower() for f in dbutils.fs.mounts() if f.mountPoint.lower() != '/'])
exclude_dirs = [d[:-1] if d[-1] == "/" else d for d in exclude_dirs]
exclude_clause = "('{}')".format("', '".join(exclude_dirs))
# exclude_clause

# COMMAND ----------

## when writing to a table/dir
# the directory does not get updated
# the data files get updated
# delta log is the last thing to get updated
# Essentially directory timestamps do not get updated if files underneath are created. We could get this information if needed though. 

# COMMAND ----------

import os
import time
import hashlib
from pyspark.sql.functions import udf

def process_directory(path):
    try:
        # Initialize variables
        file_count = len(os.listdir(path))
        max_files = 10000
        data = []
        new_dirs = []

        # Stop processing if the directory has too many files
        if file_count > max_files:
            return json.dumps([{
                'hashPath': hashlib.sha256(path.encode()).hexdigest(),
                'path': path,
                'name': None,
                'size': None,
                'modificationTime': None,
                'isDeltaTable': None, # not currently used
                'isDirectory': True,
                'errorMessage': "Directory contains more than 10,000 total files. Please manually inspect.",
                'scanDatetime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                'new_dirs': []
            }])
        
        # scan directory collects statistics too
        with os.scandir(path) as entries:
            for f in entries:
                # Collect file metadata
                data.append({
                    'hashPath': hashlib.sha256(f.path.encode()).hexdigest(),
                    'path': f.path,
                    'name': f.name,
                    'size': f.stat().st_size,
                    'modificationTime': f.stat().st_mtime,
                    'isDeltaTable': None, # not currently used
                    'isDirectory': f.is_dir(),
                    'errorMessage': None,
                    'scanDatetime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                })

                # Collect directories for further processing
                if f.is_dir():
                    new_dirs.append(f.path)

        return json.dumps({'new_dirs': new_dirs, 'data': data})

    # just return an exception if needed
    except Exception as e:
        print(f"EXCEPTION UNKNOWN: {str(e)}")
        return json.dumps({'errorMessage': str(e)})

# Register as UDF (make sure the return type is compatible with Spark DataFrame)
process_directory_udf = udf(process_directory)


# COMMAND ----------

# create starting DF
level = 0
df = spark.createDataFrame([(d,) for d in [starting_dir]], ["dirs"])

# create cols and save to table
(df
  .withColumn('path_data', process_directory_udf(df.dirs))
  .withColumn('new_paths', get_json_object(col('path_data'), "$.data"))
  .withColumn('new_dirs', get_json_object(col('path_data'), "$.new_dirs"))
  .withColumn('level', lit(level))
  .write
  .mode('append')
  .option('mergeSchema', 'true')
  .saveAsTable(output_table_name)

)

# show the base table on level 0
display(spark.sql(f"select * from {output_table_name}"))

# COMMAND ----------


while True: 
  # select only the new directories from the previous pass to scan
  df = spark.sql(f""" 
                 with cte as (
                    select explode(from_json(new_dirs, 'array<string>')) as dirs
                    from {output_table_name}
                    where level = {level} and new_dirs is not null
                  )
                  select dirs 
                  from cte 
                  where dirs not in {exclude_clause}
                 """)
  # increment level
  level +=1 
  
  # break if empty
  if df.isEmpty():
    print("breaking.... ")
    break
  else :
    # need to do this in parallel
    df = df.repartition(num_cores)
    print(f"Level: {level} | Num Rows: {df.count()}")
  
  # show for debug purposes
  # df.show()

  # create columns and append to output table
  (df
    .withColumn('path_data', process_directory_udf(df.dirs))
    .withColumn('new_paths', get_json_object(col('path_data'), "$.data"))
    .withColumn('new_dirs', get_json_object(col('path_data'), "$.new_dirs"))
    .withColumn('level', lit(level))
    .write
    .mode('append')
    .option('mergeSchema', 'true')
    .saveAsTable(output_table_name)
  )

# COMMAND ----------

display(spark.sql(f"select * from {output_table_name}"))

# COMMAND ----------

display(spark.sql(f"select count(1) from {output_table_name}"))

# 53 minutes 
# 1,138,186 files/directories listed
# 70 levels of nested directories

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analytics
# MAGIC

# COMMAND ----------

approximate_storage_cost_per_gb = 0.195

# COMMAND ----------

# Define the schema for the JSON structure
schema = StructType([
    StructField("new_dirs", ArrayType(StringType()), True),
    StructField("data", ArrayType(
        StructType([
            StructField("hashPath", StringType(), True),
            StructField("path", StringType(), True),
            StructField("name", StringType(), True),
            StructField("size", LongType(), True),
            StructField("modificationTime", DoubleType(), True),
            StructField("isDeltaTable", BooleanType(), True),
            StructField("isDirectory", BooleanType(), True),
            StructField("errorMessage", StringType(), True),
            StructField("scanDatetime", StringType(), True),
        ])
    ), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storage analysis
# MAGIC

# COMMAND ----------


df = (spark.sql(f"select path_data from {output_table_name}")
      .withColumn("parsed_json", from_json(col("path_data"), schema))
      .withColumn("data_column", col("parsed_json.data"))
      .select('path_data', explode(col('data_column')).alias("data"))
      .select('path_data', "data.path", "data.size", "data.isDeltaTable", "data.errorMessage", "data.scanDatetime", "data.hashPath", "data.name", "data.modificationTime", "data.isDirectory")
      .withColumn("lastModified", when(to_timestamp(from_unixtime(col("modificationTime"))) == '1970-01-01T00:00:00.000+00:00', None).otherwise(to_timestamp(from_unixtime(col("modificationTime")))))
      .withColumn('approximate_storage_cost_per_gb', lit(approximate_storage_cost_per_gb))
      .withColumn('sizeInGB', col("size")/1000000000)
      .withColumn('estimatedListCost', (col('size')/1000000000)*approximate_storage_cost_per_gb)
      .withColumn("year_month", date_format(col("lastModified"), "yyyy-MM"))
      )

df.createOrReplaceTempView('analytics_df')

display(df)

# COMMAND ----------

df2 = spark.sql("select year_month, sum(sizeInGB) as sizeInGB from analytics_df group by year_month")

# Define the window spec, partitioning by year_month and ordering by lastModified
window_spec = Window.orderBy("year_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate the cumulative sum of sizeInGB
df2 = (df2.withColumn("cumulativeSizeInGB", sum(col("sizeInGB")).over(window_spec))
       .withColumn('ListStorageCost', col('cumulativeSizeInGB')*approximate_storage_cost_per_gb)
       )

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Large Directories 
# MAGIC There is an issue with these two queries because of the schema enforcement on the nested column eliminating them 

# COMMAND ----------

display(
  spark.read
  .table(output_table_name)
  .withColumn("errorMessage", get_json_object(col("path_data"), "$[0].errorMessage"))
  .filter(col("errorMessage").isNotNull())

)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Errors

# COMMAND ----------

display(
  spark.read
  .table(output_table_name)
  .withColumn("errorMessage", get_json_object(col("path_data"), "$.errorMessage"))
  .filter(col("errorMessage").isNotNull())

)

# COMMAND ----------


