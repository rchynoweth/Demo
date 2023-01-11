# Databricks notebook source
# MAGIC %md
# MAGIC # Reading Copy Book Files 
# MAGIC 
# MAGIC [Cobrix](https://github.com/AbsaOSS/cobrix) - COBOL Parser Git Repository 

# COMMAND ----------

dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container", "")

# COMMAND ----------

storage_account = dbutils.widgets.get("storage_account")
container = dbutils.widgets.get("container")
data_directory = "abfss://"+container+"@"+storage_account+".dfs.core.windows.net/"
print(data_directory)

# COMMAND ----------

dbutils.fs.ls(data_directory)

# COMMAND ----------

import copybook

# COMMAND ----------

f = copybook.parse_file('abfss://albert@racadlsgen2.dfs.core.windows.net/copyBookfile.pvt')
f

# COMMAND ----------


