# Databricks notebook source
catalog_name = 'rac_demo_catalog'
schema_name = 'rac_demo_db'

# COMMAND ----------

# create a managed volume 
spark.sql(f"""
          create volume if not exists {catalog_name}.{schema_name}.system_checkpoints
          """)

# COMMAND ----------

# managed volumes have a default location based on catalog and schema
volume_path = f"/Volumes/{catalog_name}/{schema_name}/system_checkpoints"

# COMMAND ----------

stream_df = spark.readStream.table('system.billing.usage')

# COMMAND ----------

# notice that we will store checkpoints by the system schema and table 
# e.g. /Volumes/<catalog name>/<schema_name>/system_checkpoints/<system table schema>/<system table name>
(stream_df.writeStream
    .format("delta")
    .option("checkpointLocation", f"{volume_path}/billing/usage")
    .trigger(availableNow=True) # ensures we only load new data
    .table(f"{catalog_name}.{schema_name}.historical_usage")
  )

# COMMAND ----------


