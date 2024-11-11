# Databricks notebook source
# MAGIC %md
# MAGIC # Image Processing with Databricks and GCP Document AI
# MAGIC
# MAGIC Databricks can serve as a general purpose ETL engine that can move data throughout your data ecosystem. Easily connect to any data source and push data as batch or streams. [GCP Document AI](https://cloud.google.com/document-ai?hl=en) is data extraction tool on extract, classify, and split documents. It is easy to check out, go to the link provide and upload an image to test it out. 
# MAGIC
# MAGIC The two services are excellent choices as Databricks provides cloud portability and GCP provides best in class document data extraction. 
# MAGIC
# MAGIC ## Instructions 
# MAGIC
# MAGIC This notebook takes three parameters to set up your data access. The expectation is that images are stored in the volume provided and have the following naming convention with the name of the image: `sea_salt_20241104_081427.jpg`. Additionally, you will need an access token and an existing Document AI deployed in GCP. 
# MAGIC
# MAGIC To generate an access token, go into the google cloud shell and run the following command. This is acceptable for development purposes, but you will want to use _<>recommendedation<>_.  
# MAGIC     ```
# MAGIC     gcloud auth application-default print-access-token
# MAGIC     ```
# MAGIC
# MAGIC Next you will want to deploy a custom extractor which can be found on this [GCP Page](https://cloud.google.com/document-ai). 

# COMMAND ----------

import requests
import json
import base64
import pandas as pd
from pyspark.sql.functions import pandas_udf, regexp_extract, input_file_name
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

# COMMAND ----------

# set and get notebook parameters 
dbutils.widgets.text('catalog_name', '')
dbutils.widgets.text('schema_name', '')
dbutils.widgets.text('volume_name', '')
dbutils.widgets.text('url', "https://us-documentai.googleapis.com/v1/projects/697856052963/locations/us/processors/5667d7e5c86460e2:process")

catalog_name = dbutils.widgets.get('catalog_name')
schema_name = dbutils.widgets.get('schema_name')
volume_name = dbutils.widgets.get('volume_name')

# url to the document ai api
url = dbutils.widgets.get('url')
# Retrieve the access token from Databricks secrets
access_token = dbutils.secrets.get('rac_scope', 'gcp_api_token')

print(f"{catalog_name} | {schema_name} | {volume_name}")

# COMMAND ----------

# create and use UC objects
spark.sql(f"create catalog if not exists {catalog_name}")
spark.sql(f"use catalog {catalog_name}")
spark.sql(f"create schema if not exists {schema_name}")
spark.sql(f"use schema {schema_name}")
spark.sql(f"create volume if not exists {volume_name}")

# COMMAND ----------

# volume path to save raw files
volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
dbutils.fs.ls(volume_path)[:3]

# COMMAND ----------

# Notice on the function use here 
## https://docs.databricks.com/en/sql/language-manual/functions/input_file_name.html
## https://docs.databricks.com/en/ingestion/file-metadata-column.html

df = (spark.read
      .format('image')
      .load(f"{volume_path}/*.jpg")
      .withColumn('file_path', input_file_name()) # get file path
      .withColumn('product_name', regexp_extract('file_path', r'([^/]+)_\d{8}_\d{6}\.jpg$', 1)) # parse out the product name
      )
display(df)

# COMMAND ----------

## UDF to call the API
## Loads image bytes via file path. But could also load via image column. 

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json; charset=utf-8"
}

# Define the UDF
@pandas_udf(StringType())
def process_image(image_path_series: pd.Series) -> pd.Series:
    results = []
    for image_path in image_path_series:
        image_path = image_path.replace('dbfs:','')
        try:
            # Read the image and encode it in base64
            with open(image_path, "rb") as image_file:
                image_data = image_file.read()
            image_bytes = base64.b64encode(image_data).decode("utf-8")

            # Define the JSON payload for the image
            payload = {
                "rawDocument": {
                    "content": image_bytes,
                    "mimeType": "image/png"
                }
            }

            # Send the request to the Document AI API
            response = requests.post(url, headers=headers, json=payload)

            # Parse the response and append the result
            if response.status_code == 200:
                results.append(json.loads(response.content.decode('utf-8')))
            else:
                results.append({"error": f"Request failed with status {response.status_code} | {json.loads(response.content.decode('utf-8'))}"})
        except Exception as e:
            results.append({"error": str(e)})

    return pd.Series([json.dumps(result) for result in results])

# COMMAND ----------

# Apply the UDF to the Spark DataFrame
# Assuming the Spark DataFrame has a column `file_path` with paths to images
df = df.withColumn("processed_result", process_image(df["file_path"]))
df.write.mode('overwrite').saveAsTable("processed_images", mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC # Misc Test Code Checking out the response values 

# COMMAND ----------


access_token = dbutils.secrets.get('rac_scope', 'gcp_api_token')

image_data = open("/Volumes/rac_demo_catalog/rac_demo_db/rac_volume/20241029_094616.jpg", "rb").read()
image_bytes = base64.b64encode(image_data).decode("utf-8")

url = "https://us-documentai.googleapis.com/v1/projects/697856052963/locations/us/processors/5667d7e5c86460e2:process"
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json; charset=utf-8"
}

# Define the JSON payload for the image
payload = {
    "rawDocument": {
        "content": image_bytes,
        "mimeType": "image/png"
    }
}

response = requests.post(url, headers=headers, json=payload)

# Print the response
print(response.status_code)
# return json.loads(response.content.decode('utf-8'))
json.loads(response.content.decode('utf-8'))

# COMMAND ----------

response = requests.post(url, headers=headers, json=payload)

# Print the response
print(response.status_code)

# COMMAND ----------

json.loads(response.content.decode('utf-8'))

# COMMAND ----------


