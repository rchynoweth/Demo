# Databricks notebook source
input_path = "/databricks-datasets/structured-streaming/events"
output_path = "abfss://snowflakedemo@racadlsgen2.dfs.core.windows.net/event_json_data"

# COMMAND ----------

files = [f.name for f in dbutils.fs.ls(input_path)]
files

# COMMAND ----------

print(f"File Count = {len(files)}")

# COMMAND ----------

dbutils.fs.rm(output_path, True)
dbutils.fs.mkdirs(output_path)

# COMMAND ----------

i = 0 
for f in files:
  print(f"File Count: {i}")
  full_input = f"{input_path}/{f}"
  full_output = f"{output_path}/{f}"
  dbutils.fs.cp(full_input, full_output)
  i += 1
  
  

# COMMAND ----------

dbutils.fs.ls(output_path)

# COMMAND ----------


