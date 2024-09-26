# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import col

# COMMAND ----------

dbutils.widgets.text("Output Table Name", "")
output_table_name = dbutils.widgets.get("Output Table Name")

# COMMAND ----------

# spark.sql(f"drop table if exists {output_table_name}")

# COMMAND ----------

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS {output_table_name} (
    format STRING COMMENT 'null',
    name STRING COMMENT 'null',
    location STRING COMMENT 'null',
    numFiles DOUBLE COMMENT 'null',
    sizeInBytes DOUBLE COMMENT 'null',
    totalFilecount DOUBLE COMMENT 'null',
    totalSize DOUBLE COMMENT 'null',
    error STRING COMMENT 'null'
  )
""")

# COMMAND ----------

def get_cumulative_size(path):
    """
    Return the total cumulative size of all files under a parent directory (excluding _delta_log files)
    :param path: the parent directory for the size calculation
    :return: integer value of cumulative file sizes
    """
    _total_size = 0
    for entry in dbutils.fs.ls(path):
        if(entry.name == "_delta_log/"):
            continue
        if(entry.name[-1] == "/"): 
            _total_size += get_cumulative_size(entry.path)
        else:
            _total_size += entry.size
    return _total_size

def get_cumulative_filecount(path):
    """
    Return the total cumulative count of all files under a parent directory (excluding _delta_log files)
    :param path: the parent directory for the size calculation
    :return: integer value of cumulative file counts
    """
    _total_count = 0
    for entry in dbutils.fs.ls(path):
        if(entry.name == "_delta_log/"):
            continue
        if(entry.name[-1] == "/"): 
            _total_count += get_cumulative_filecount(entry.path)
        else:
            _total_count += 1
    return _total_count

def get_single_table_info(schema_and_table_name):
    """
    Return table details and cumulative file size and counts for a single table
    :param schema_and_table_name: qualified table name to be analyzed
    :return: pandas dataframe containing table details
    """
    columns_to_fetch = ["format", "name", "location", "numFiles", "sizeInBytes"]
    
    try:
        # Fetching the table details from Spark SQL
        _dtls_pdf = spark.sql(f"describe detail {schema_and_table_name}").select(*columns_to_fetch).toPandas()
        
        # Calculating cumulative file count and size
        _dtls_pdf["totalFilecount"] = _dtls_pdf["location"].apply(lambda x: get_cumulative_filecount(x))
        _dtls_pdf["totalSize"] = _dtls_pdf["location"].apply(lambda x: get_cumulative_size(x))
        
        # Adding an empty "error" column
        _dtls_pdf["error"] = ['']  # No error occurred
    except Exception as e:
        # Handling exception by creating a default row
        data = {
            "format": [''],
            "name": [schema_and_table_name],
            "location": [''],
            "numFiles": [0],  # Use float instead of int to allow for NaN
            "sizeInBytes": [0],  # Use float instead of int to allow for NaN
            "totalFilecount": [0],
            "totalSize":[0],
            "error": str(e)  # Storing the error message
        }
        _dtls_pdf = pd.DataFrame(data)
    
    return _dtls_pdf
  
def get_all_table_info(schema_name):
    """
    Return table details and cumulative file size and counts for all tables in a schema
    :param schema_name: schema name to be analyzed
    :return: pandas dataframe containing all table details
    """
    dtl_list = []
    all_tables = spark.sql(f"show tables in hive_metastore.{schema_name}").select("tableName").toPandas()["tableName"].to_list()
    if len(all_tables) == 0:
      pass
    for table_name in all_tables:
        dtl_list.append(get_single_table_info(f"hive_metastore.{schema_name}.{table_name}"))
    return pd.concat(dtl_list, ignore_index=True).astype({"totalSize":"int64","totalFilecount":"int64"}) if dtl_list else pd.DataFrame(columns=["totalSize", "totalFilecount"])

# COMMAND ----------

hms_schemas = [d.databaseName for d in spark.sql('SHOW SCHEMAS in hive_metastore').select('databaseName').collect()]

# COMMAND ----------

i = 1
for s in hms_schemas:
  print(f"Analyzing: {s} | {i} out of {len(hms_schemas)}")
  pdf = get_all_table_info(f'{s}')
  if not pdf.empty:
    (
      spark.createDataFrame(pdf.fillna(int(0)))
      .withColumn("numFiles", col("numFiles").cast("double"))
      .withColumn("sizeInBytes", col("sizeInBytes").cast("double"))
      .withColumn("totalFilecount", col("totalFilecount").cast("double"))
      .withColumn("totalSize", col("totalSize").cast("double"))
      .coalesce(4)
      .write
      .option("mergeSchema", "true")
      .mode('append')
      .saveAsTable(output_table_name)
    )
  else :
    print(f"Empty Schema: {s}")
  i+=1

# COMMAND ----------

pdf.head()

# COMMAND ----------

pdf.fillna(int(0)).head()

# COMMAND ----------

pdf.dtypes

# COMMAND ----------

display(spark.sql(f"""
                  describe table {output_table_name}2
                  """))

# COMMAND ----------

display(spark.sql(f"""
                  select * from {output_table_name}
                  """))

# COMMAND ----------


