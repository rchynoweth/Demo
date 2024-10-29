# Databricks notebook source
from pyspark.sql import Row

# Create a list of Rows
data = [
    Row(col1="value1_1", col2="value1_2", col3="value1_3"),
    Row(col1="value2_1", col2="value2_2", col3="value2_3"),
    Row(col1="value3_1", col2="value3_2", col3="value3_3"),
    Row(col1="value4_1", col2="value4_2", col3="value4_3"),
    Row(col1="value5_1", col2="value5_2", col3="value5_3"),
    Row(col1="value6_1", col2="value6_2", col3="value6_3"),
    Row(col1="value7_1", col2="value7_2", col3="value7_3"),
    Row(col1="value8_1", col2="value8_2", col3="value8_3"),
    Row(col1="value9_1", col2="value9_2", col3="value9_3"),
    Row(col1="value10_1", col2="value10_2", col3="value10_3")
]

# Create DataFrame
df = spark.createDataFrame(data)

# Display DataFrame
display(df)

# COMMAND ----------

try:
  cnt = (spark.read
  .format("bigquery")
  .option("table", 'rac_dataset.test_read_write_table')
  .option("project", 'fe-dev-sandbox')
  .load()
  ).count()
  print(f"cnt: {cnt}")
except :
  print("Table doesn't exist yet.")

# COMMAND ----------

# write to table
(df.write
  .mode('append')
  .format("bigquery")
  .option("project", 'fe-dev-sandbox')
  .option("writeMethod", "direct")
  # .option("parentProject", <parent-project-id>)
  .save('rac_dataset.test_read_write_table')
)

# COMMAND ----------

# check the row count. Should be +10 when compared to cell 2. 
(spark.read
  .format("bigquery")
  .option("table", 'rac_dataset.test_read_write_table')
  .option("project", 'fe-dev-sandbox')
  .load()
).count()

# COMMAND ----------

# (spark.read
#   .format("bigquery")
#   .option("materializationDataset", "rac_dataset")
#   .option("parentProject", "fe-dev-sandbox")
#   .option("project", "fe-dev-sandbox")
#   .option("cacheExpirationTimeInMinutes",0)
#   .option("enableReadSessionCaching", "false")
#   .option("materializationExpirationTimeInMinutes",1)
#   .option("viewsEnabled", "true")
#   .option("query", "select * from rac_dataset.test_read_write_table")
#   .load()
# ).count()


(spark.read
  .format("bigquery")
  .option("materializationDataset", "rac_dataset")
  .option("parentProject", "fe-dev-sandbox")
  .option("project", "fe-dev-sandbox")
  .option("cacheExpirationTimeInMinutes", "0")
  .option("enableReadSessionCaching", "false")
  .option("materializationExpirationTimeInMinutes", "1")
  .option("viewsEnabled", "true")
  .option("query", "select * from rac_dataset.test_read_write_table")
  .load()
).count()

# COMMAND ----------

display(spark.read
  .format("bigquery")
  .option("materializationDataset", "rac_dataset")
  .option("parentProject", "fe-dev-sandbox")
  .option("project", "fe-dev-sandbox")
  .option("cacheExpirationTimeInMinutes",0)
  .option("enableReadSessionCaching", "false")
  .option("optimizedEmptyProjection", "false")
  .option("materializationExpirationTimeInMinutes",1)
  .option("viewsEnabled", "true")
  .option("query", "select count(1) from rac_dataset.test_read_write_table")
  .load()
)

# COMMAND ----------

# write to table
(df.write
  .mode('append')
  .format("bigquery")
  .option("project", 'fe-dev-sandbox')
  .option("writeMethod", "direct")
  # .option("parentProject", <parent-project-id>)
  .save('rac_dataset.test_read_write_table')
)

# COMMAND ----------

display(spark.read
  .format("bigquery")
  .option("materializationDataset", "rac_dataset")
  .option("parentProject", "fe-dev-sandbox")
  .option("project", "fe-dev-sandbox")
  .option("cacheExpirationTimeInMinutes",0)
  .option("enableReadSessionCaching", "false")
  .option("optimizedEmptyProjection", "false")
  .option("materializationExpirationTimeInMinutes",1)
  .option("viewsEnabled", "true")
  .option("query", "select count(1) from rac_dataset.test_read_write_table")
  .load()
)

# COMMAND ----------

(spark.read
  .format("bigquery")
  .option("materializationDataset", "rac_dataset")
  .option("parentProject", "fe-dev-sandbox")
  .option("project", "fe-dev-sandbox")
  .option("cacheExpirationTimeInMinutes",0)
  .option("enableReadSessionCaching", "false")
  .option("optimizedEmptyProjection", "false")
  .option("materializationExpirationTimeInMinutes",1)
  .option("viewsEnabled", "true")
  .option("query", "select * from rac_dataset.test_read_write_table")
  .load()
  .write
  .saveAsTable('test_read_write_table')
  )

display(spark.sql("select count(1) from test_read_write_table"))

# COMMAND ----------

# write to table
(df.write
  .mode('append')
  .format("bigquery")
  .option("project", 'fe-dev-sandbox')
  .option("writeMethod", "direct")
  # .option("parentProject", <parent-project-id>)
  .save('rac_dataset.test_read_write_table')
)

# COMMAND ----------

(spark.read
  .format("bigquery")
  .option("materializationDataset", "rac_dataset")
  .option("parentProject", "fe-dev-sandbox")
  .option("project", "fe-dev-sandbox")
  .option("cacheExpirationTimeInMinutes",0)
  .option("enableReadSessionCaching", "false")
  .option("optimizedEmptyProjection", "false")
  .option("materializationExpirationTimeInMinutes",1)
  .option("viewsEnabled", "true")
  .option("query", "select * from rac_dataset.test_read_write_table")
  .load()
  .write
  .mode('overwrite').saveAsTable('test_read_write_table2')
  )

display(spark.sql("select count(1) from test_read_write_table2"))

# COMMAND ----------


