# Databricks notebook source
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit 
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd


# COMMAND ----------

catalogs = spark.sql("show catalogs")
catalog_list = [c.catalog for c in catalogs.select('catalog').collect()]

# COMMAND ----------

def get_schemas(c):
  try:
    return [c+f".{s.databaseName}" for s in spark.sql(f"show schemas in {c}").select('databaseName').collect() if s.databaseName != 'information_schema']
  except Exception as e:
    return f"Error getting schemas for catalog: {c}"
  
def get_tables(cs):
  try:
    return [cs+f".{t.table_name}" for t in spark.sql(f"show tables in {cs}").select('tableName').collect()]
  except Exception as e:
    return f"Error getting tables for schema: {cs}"

# COMMAND ----------

schema_errors = []
schemas = []
exclude_cats = ['__databricks_internal', 'system']

with ThreadPoolExecutor(max_workers=32) as executor:
    futures = [executor.submit(get_schemas, c) for c in catalog_list if c not in exclude_cats]
    outstanding = len(futures)
    tots = len(futures)
    for future in as_completed(futures):
        outstanding -= 1
        print(f"Outstanding Catalogs: {outstanding} | Total Catalogs: {tots}")
        list_data = future.result()
        if type(list_data) == str:
          schema_errors.append(list_data)
        else:
          schemas.extend(list_data)

print(f"Schema Error Count: {len(schema_errors)} | Schema Count: {len(schemas)}")

# COMMAND ----------

table_errors = []
tables = []

with ThreadPoolExecutor(max_workers=32) as executor:
    futures = [executor.submit(get_tables, cs) for cs in schemas]
    outstanding = len(futures)
    tots = len(futures)
    for future in as_completed(futures):
        outstanding -= 1
        print(f"Outstanding Catalogs: {outstanding} | Total Catalogs: {tots}")
        list_data = future.result()
        if type(list_data) == str:
          table_errors.append(list_data)
        else:
          tables.extend(list_data)

print(f"Schema Error Count: {len(table_errors)} | Schema Count: {len(tables)}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Hive Metastore Analytics 

# COMMAND ----------

import pandas as pd

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
    columns_to_fetch = ["format", "name", "location","partitionColumns","numFiles", "sizeInBytes"]
    try:
        _dtls_pdf = spark.sql(f"describe detail {schema_and_table_name}").select(*columns_to_fetch).toPandas()
        _dtls_pdf["totalFilecount"] = _dtls_pdf["location"].apply(lambda x: get_cumulative_filecount(x))
        _dtls_pdf["totalSize"] = _dtls_pdf["location"].apply(lambda x: get_cumulative_size(x))
    except:
        _dtls_pdf = pd.DataFrame(columns=columns_to_fetch)
        _dtls_pdf["name"] = schema_and_table_name
    return _dtls_pdf
  
def get_all_table_info(schema_name):
    """
    Return table details and cumulative file size and counts for all tables in a schema
    :param schema_name: schema name to be analyzed
    :return: pandas dataframe containing all table details
    """
    dtl_list = []
    all_tables = spark.sql(f"show tables in {schema_name}").select("tableName").toPandas()["tableName"].to_list()
    if len(all_tables) == 0:
      pass
    for table_name in all_tables:
        print(f"analyzing: {table_name}")
        dtl_list.append(get_single_table_info(f"{schema_name}.{table_name}"))
    return pd.concat(dtl_list, ignore_index=True).astype({"totalSize":"int64","totalFilecount":"int64"})

# COMMAND ----------

spark.sql(f"show tables in hive_metastore.{hms_schemas[0]}").select("tableName").toPandas()["tableName"].to_list()

# COMMAND ----------

hms_schemas = [d.databaseName for d in spark.sql('SHOW SCHEMAS in hive_metastore').select('databaseName').collect()]
hms_schemas

# COMMAND ----------

'hive_metastore.'+hms_schemas[0]

# COMMAND ----------

pdf = get_all_table_info(f'hive_metastore.{hms_schemas[0]}')

# COMMAND ----------


