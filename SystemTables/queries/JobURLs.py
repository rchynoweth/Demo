# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

workspace_list = ['https://adb-984752964297111.11.azuredatabricks.net/']

# COMMAND ----------

workspace_df = (spark.createDataFrame([(url,) for url in workspace_list], ["workspace_url"])
                .withColumn("workspace_id", regexp_extract("workspace_url", r"https://adb-(\d+)\.11\.azuredatabricks\.net/", 1))
                )
display(workspace_df)

# COMMAND ----------

jobs_df = spark.read.table("system.workflow.jobs")
display(jobs_df)

# COMMAND ----------



# COMMAND ----------

joined_df = jobs_df.join(workspace_df, on="workspace_id", how="inner")
joined_df = joined_df.withColumn("job_url", concat(col("workspace_url"), lit("jobs/"), col("job_id")))

display(joined_df)

# COMMAND ----------


