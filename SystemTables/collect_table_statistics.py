# Databricks notebook source
from libs.data_collector import DataCollector
from pyspark.sql.functions import *

# COMMAND ----------

catalog = 'main'
spark.sql(f'use catalog {catalog}')

# COMMAND ----------

dc = DataCollector(spark=spark)
catalog_list = dc.get_catalogs()
schema_list = dc.get_schemas(catalog_name='main')
schema_list

# COMMAND ----------

dc.run_parallel_schema_analysis(schema_list=schema_list)

# COMMAND ----------

def run_schema_analysis(row):
  """
  Runs Analysis on the entire schema 
  :param item: a row item with catalog and schema names 
  """
  catalog_name, schema_name = row.catalog_name, row.schema_name
  spark.sql(f'use {catalog_name}')
  sql_command = f"ANALYZE TABLES IN {schema_name} COMPUTE STATISTICS NOSCAN"
  spark.sql(sql_command)

# COMMAND ----------

results = pool.map(run_schema_analysis, schema_list)

# COMMAND ----------



# COMMAND ----------

import pandas as pd
import requests
from multiprocessing import Pool
 
# Define the API call function
def api_call(row):
    response = requests.get(f'https://api.example.com/?id={row["id"]}')
    return response.json()
 
# Load the data into a Pandas DataFrame
data = pd.read_csv('data.csv')
 
# Define the number of processes to use
num_processes = 4
 
# Create a Pool object to handle the parallel processing
pool = Pool(processes=num_processes)
 
# Apply the API call function to each row of the DataFrame in parallel
results = pool.map(api_call, [row for index, row in data.iterrows()])
 
# Combine the results into a new DataFrame
output = pd.DataFrame(results)
 
# Merge the output DataFrame back into the original DataFrame
data = pd.concat([data, output], axis=1)

# COMMAND ----------



# COMMAND ----------


