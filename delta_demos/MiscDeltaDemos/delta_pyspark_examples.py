# Databricks notebook source
spark.sql("use rac_demo_db")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists boston_housing_dataset

# COMMAND ----------

from sklearn.datasets import load_boston
import pandas as pd
from delta.tables import *


# COMMAND ----------

boston = load_boston()
pdf = pd.DataFrame(boston.data, columns=boston.feature_names)
pdf['id'] = pdf.index

df = spark.createDataFrame(pdf)

pdf['RAD'] = pdf['RAD']+100

df2 = spark.createDataFrame(pdf.sample(round(len(pdf)*0.5)))

display(df2)

# COMMAND ----------

df.write.format("delta").saveAsTable("boston_housing_dataset")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from boston_housing_dataset where RAD > 100

# COMMAND ----------

deltaTable = DeltaTable.forName(spark, "boston_housing_dataset")

# COMMAND ----------

( deltaTable.alias("target").merge(
    df2.alias("source"), "target.id = source.id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()

)




# COMMAND ----------

# MAGIC %sql
# MAGIC select * from boston_housing_dataset where RAD > 100

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


