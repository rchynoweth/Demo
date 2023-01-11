# Databricks notebook source
# MAGIC %md
# MAGIC # Working with Pandas and Koalas 
# MAGIC 
# MAGIC [Blog](https://www.google.com/search?q=read+data+with+koalas&oq=read+data+with+koalas&aqs=chrome..69i57.3186j0j1&sourceid=chrome&ie=UTF-8)
# MAGIC 
# MAGIC This notebook highlights how koalas is able to maintain the same syntax and functionality of pandas dataframes but is much more suited to work in an Apache Spark environment. The syntax is the same for all commands here but there are a few differences that should be noted when converting pandas to koalas. 
# MAGIC 
# MAGIC 1. Koalas is distributed so you are able to partition the rows of the dataframe and distribute the data across a cluster
# MAGIC   - It is known that pandas scalability on larger datasets is due to the single threaded nature 
# MAGIC 2. Reading and writing pandas dataframes to Delta tables is not simple
# MAGIC   - Reading data first needs to be read as a Spark/Koalas DF then converted. 
# MAGIC   - Reading files from storage will require '/dbfs/' (not a big deal but noteworthy)
# MAGIC   - Writing pandas to delta tables requires converting to koalas/spark first 

# COMMAND ----------

import numpy as np
import pandas as pd
import databricks.koalas as ks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Data from Storage

# COMMAND ----------

colnames = ["age", "workclass", "fnlwgt", "education", "education_num","marital_status", "occupation","relationship", "race", "sex","capital_gain","capital_loss","hours_per_week", "native_country", "income"]
coltypes = ["double", "string", "double", "string", "double", "string", "string", "string", "string", "string", "double", "double", "double", "string", "string"]

# COMMAND ----------

# DBTITLE 1,Koalas
kdf = ks.read_csv("/databricks-datasets/adult/adult.data", names=colnames, header=None)

for i in range(0, len(kdf.columns)):
  kdf[colnames[i]] = kdf[colnames[i]].astype(coltypes[i])

kdf['id'] = kdf.index

kdf = kdf.spark.repartition(8)
display(kdf)

# COMMAND ----------

kdf.dtypes

# COMMAND ----------

# DBTITLE 1,Pandas 
## NOTICE - '/dbfs/'
pdf = pd.read_csv("/dbfs/databricks-datasets/adult/adult.data", names=colnames, header=None)

for i in range(0, len(pdf.columns)):
  pdf[colnames[i]] = pdf[colnames[i]].astype(coltypes[i])
  
pdf['id'] = pdf.index
display(pdf)

# COMMAND ----------

pdf.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations 
# MAGIC 
# MAGIC We won't go into all the details of transformations but largely want to show how the syntax would mostly remain the same. 
# MAGIC 
# MAGIC Check out some Koalas [transform and apply functions](https://koalas.readthedocs.io/en/latest/user_guide/transform_apply.html) which correspond to [pandas transforms](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.transform.html) and [pandas apply](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.apply.html.

# COMMAND ----------

# DBTITLE 1,Koalas Transform 
def simple_age_addition(age):
  return age + 1  

kdf['new_age'] = kdf.age.apply(simple_age_addition)
display(kdf[['age', 'new_age']])

# COMMAND ----------

# DBTITLE 1,Pandas
pdf['new_age'] = pdf.age.apply(simple_age_addition)
display(pdf[['age', 'new_age']])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining Data 

# COMMAND ----------

# DBTITLE 1,Koalas
kdf2 = kdf[:].drop('new_age')

display(kdf2)

# COMMAND ----------

display(kdf)

# COMMAND ----------

display(ks.merge(kdf,kdf2,on='id'))

# COMMAND ----------

# DBTITLE 1,Pandas
pdf2 = pdf[:].drop('new_age', axis=1)

display(pdf2)

# COMMAND ----------

display(pdf)

# COMMAND ----------

display(pd.merge(pdf,pdf2,on='id'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing to Delta tables

# COMMAND ----------

# DBTITLE 1,Koalas 
kdf.to_table("koalas_delta_table", format='delta', mode='append')
# kdf.to_delta requires file paths. Better to use to_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM koalas_delta_table

# COMMAND ----------

# DBTITLE 1,Pandas
(spark.createDataFrame(pdf)
 .write
 .format('delta')
 .saveAsTable('pandas_delta_table')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pandas_delta_table

# COMMAND ----------


