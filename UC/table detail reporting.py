# Databricks notebook source
dbutils.widgets.text('schema_name', '')

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
    for table_name in spark.sql(f"show tables in {schema_name}").select("tableName").toPandas()["tableName"].to_list():
        print(f"analyzing: {table_name}")
        dtl_list.append(get_single_table_info(f"{schema_name}.{table_name}"))
    return pd.concat(dtl_list, ignore_index=True).astype({"totalSize":"int64","totalFilecount":"int64"})

# COMMAND ----------

schema_name = dbutils.widgets.get('schema_name')
get_all_table_info(schema_name)

# COMMAND ----------


