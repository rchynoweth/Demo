# Databricks notebook source
# MAGIC %md
# MAGIC # Load Test for Cross Region Latency
# MAGIC
# MAGIC Workspace is in us-central-1 which is in Council Bluffs, Iowa. We will use a secondary region as columbus which is in Columbus, Ohio. This is about ~1,250KM which is about the same distance between Los Angeles and The Dalles, Oregon which is ~1,000 miles. 
# MAGIC
# MAGIC Bucket Names: 
# MAGIC - `rac_central`
# MAGIC - `rac_columbus`
# MAGIC
# MAGIC Service Account: 
# MAGIC - `racsa1@fe-dev-sandbox.iam.gserviceaccount.com`
# MAGIC - `fe-prod-general-sa@fe-prod-dbx.iam.gserviceaccount.com`

# COMMAND ----------

import uuid
import datetime 

# COMMAND ----------

uid = str(uuid.uuid4())
print(uid)

# COMMAND ----------

dbutils.widgets.text('bucket_name', '')

# COMMAND ----------

bucket_name = dbutils.widgets.get('bucket_name')
print(bucket_name)

# COMMAND ----------

start_time = datetime.datetime.utcnow()
print(start_time)

# COMMAND ----------

df = spark.read.load(f'gs://{bucket_name}/source/nyc_delta')

# COMMAND ----------

df.write.save(f'gs://{bucket_name}/output/{uid}')

# COMMAND ----------

end_time = datetime.datetime.utcnow()

# COMMAND ----------

print(f"TOTAL TIME: {end_time-start_time}")

# COMMAND ----------


