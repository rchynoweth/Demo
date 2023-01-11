# Databricks notebook source
import numpy as np
import pandas as pd
import databricks.koalas as ks

colnames = ["age", "workclass", "fnlwgt", "education", "education_num","marital_status", "occupation","relationship", "race", "sex","capital_gain","capital_loss","hours_per_week", "native_country", "income"]
coltypes = ["double", "string", "double", "string", "double", "string", "string", "string", "string", "string", "double", "double", "double", "string", "string"]

pdf = pd.read_csv("/dbfs/databricks-datasets/adult/adult.data", names=colnames, header=None)

for i in range(0, len(pdf.columns)):
  pdf[colnames[i]] = pdf[colnames[i]].astype(coltypes[i])


  
df = spark.createDataFrame(pdf)
display(df)

# COMMAND ----------

mount_delta_path = "/mnt/bldemo/delta_tables/census_data_delta"
df.write.format("delta").mode("overwrite").save(mount_delta_path)

# COMMAND ----------

dbutils.fs.ls(mount_delta_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists census_data_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE census_data_delta USING DELTA LOCATION '/mnt/bldemo/delta_tables/census_data_delta';

# COMMAND ----------

display(
spark.sql("""
SELECT * FROM census_data_delta
""")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Stop!! Now switch this notebook to a cluster with TABLE ACLs and see if you can select data. 

# COMMAND ----------

display(
spark.sql("""
SELECT * FROM census_data_delta
""")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED census_data_delta

# COMMAND ----------


