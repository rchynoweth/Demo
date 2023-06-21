# Databricks notebook source
from multiprocessing import Pool 

from pyspark.sql.functions import *

# COMMAND ----------

catalog = 'main'
spark.sql(f'use catalog {catalog}')

# COMMAND ----------

# MAGIC %sql
# MAGIC ANALYZE TABLES IN main.prophet_forecast_schema COMPUTE STATISTICS NOSCAN

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

df = df.withColumn("data_type", regexp_replace(col("data_type"), " bytes", ""))
df = df.withColumn("data_type_double", df["data_type"].cast("double"))

# COMMAND ----------

sql_command = f"ANALYZE TABLES IN prophet_forecast_schema COMPUTE STATISTICS NOSCAN"
spark.sql(sql_command)
desc_sql_command = f" DESC EXTENDED main.prophet_forecast_schema.sku_cost_lookup"
df = (spark.sql(desc_sql_command)
      .filter(col('col_name')=='Statistics')
      .withColumn("data_type", regexp_replace(col("data_type"), " bytes", ""))
      .withColumn("data_type_double", col('data_type').cast("double"))
      .select('data_type_double')
    )
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC show catalogs

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


