# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import * 
import json, os

# COMMAND ----------

sample_json = {
    "EnqueuedTimeUtc": "2021-11-12T16:50:08.6370000Z",
    "Properties": {},
    "SystemProperties": {
        "connectionDeviceId": "WGDillonUtilities",
        "connectionAuthMethod": "{\"scope\":\"device\",\"type\":\"sas\",\"issuer\":\"iothub\",\"acceptingIpFilterRule\":null}",
        "connectionDeviceGenerationId": "1111111111111111111111",
        "contentType": "application/json",
        "contentEncoding": "utf-8",
        "enqueuedTime": "2021-11-12T16:50:08.6370000Z"
    },
    "Body": {
        "timestamp": 1636735807870,
        "values": [
            {
                "id": "Box Furnace 1.Device1.PrevDay",
                "v": 68,
                "q": True,
                "t": 1636711619511
            },
            {
                "id": "Box Furnace 1.Device1.Rate",
                "v": 7787,
                "q": True,
                "t": 1636735807448
            },
            {
                "id": "Box Furnace 1.Device1.Today",
                "v": 36,
                "q": True,
                "t": 1636735800448
            },
            {
                "id": "Box Furnace 1.Device1.Total",
                "v": -22984,
                "q": True,
                "t": 1636735368427
            },
            {
                "id": "Box Furnace 2.Device1.PrevDay",
                "v": 112,
                "q": True,
                "t": 1636715144570
            },
            {
                "id": "Box Furnace 2.Device1.Rate",
                "v": 2714,
                "q": True,
                "t": 1636735806448
            },
            {
                "id": "Box Furnace 2.Device1.Today",
                "v": 20,
                "q": True,
                "t": 1636735131415
            },
            {
                "id": "Box Furnace 2.Device1.Total",
                "v": 26903,
                "q": True,
                "t": 1636735671439
            },
            {
                "id": "Box Furnace 3.Device1.PrevDay",
                "v": 80,
                "q": True,
                "t": 1636715369581
            },
            {
                "id": "Box Furnace 3.Device1.Rate",
                "v": 330,
                "q": True,
                "t": 1636735806448
            },
            {
                "id": "Box Furnace 3.Device1.Today",
                "v": 18,
                "q": True,
                "t": 1636734683343
            },
            {
                "id": "Box Furnace 3.Device1.Total",
                "v": 22383,
                "q": True,
                "t": 1636735146426
            },
            {
                "id": "Box Furnace 4.Device1.PrevDay",
                "v": 84,
                "q": True,
                "t": 1636715193581
            },
            {
                "id": "Box Furnace 4.Device1.Rate",
                "v": 3234,
                "q": True,
                "t": 1636735806448
            },
            {
                "id": "Box Furnace 4.Device1.Today",
                "v": 19,
                "q": True,
                "t": 1636734857370
            },
            {
                "id": "Box Furnace 4.Device1.Total",
                "v": 23601,
                "q": True,
                "t": 1636734686345
            },
            {
                "id": "Box Furnace 5.Device1.PrevDay",
                "v": 87,
                "q": True,
                "t": 1636715368581
            },
            {
                "id": "Box Furnace 5.Device1.Rate",
                "v": 3051,
                "q": True,
                "t": 1636735807448
            },
            {
                "id": "Box Furnace 5.Device1.Today",
                "v": 18,
                "q": True,
                "t": 1636734735358
            },
            {
                "id": "Box Furnace 5.Device1.Total",
                "v": 22553,
                "q": True,
                "t": 1636735109428
            },
            {
                "id": "Car Bottom 1.Device1.PrevDay",
                "v": 171,
                "q": True,
                "t": 1636714802564
            },
            {
                "id": "Car Bottom 1.Device1.Rate",
                "v": 14034,
                "q": True,
                "t": 1636735807448
            },
            {
                "id": "Car Bottom 1.Device1.Today",
                "v": 27,
                "q": True,
                "t": 1636735714449
            },
            {
                "id": "Car Bottom 1.Device1.Total",
                "v": 5396,
                "q": True,
                "t": 1636096394802
            },
            {
                "id": "Car Bottom 2.Device1.PrevDay",
                "v": 105,
                "q": True,
                "t": 1636715092577
            },
            {
                "id": "Car Bottom 2.Device1.Rate",
                "v": 942,
                "q": True,
                "t": 1636735807448
            },
            {
                "id": "Car Bottom 2.Device1.Today",
                "v": 6,
                "q": True,
                "t": 1636735124418
            },
            {
                "id": "Car Bottom 2.Device1.Total",
                "v": -20905,
                "q": True,
                "t": 1636734546328
            }
        ]
    }
}

print(sample_json)

# COMMAND ----------

app_json = json.dumps(sample_json)
print(app_json)

# COMMAND ----------

dbutils.fs.mkdirs('/tmp/ryan.chynoweth@databricks.com/')

# COMMAND ----------

with open('/dbfs/tmp/ryan.chynoweth@databricks.com/sample_json.json', 'w') as json_file:
  json.dump(sample_json, json_file)

# COMMAND ----------

display(spark.read.json('/tmp/ryan.chynoweth@databricks.com/sample_json.json'))

# COMMAND ----------

(spark.read.json('/tmp/ryan.chynoweth@databricks.com/sample_json.json')).printSchema()

# COMMAND ----------

(spark.read.json('/tmp/ryan.chynoweth@databricks.com/sample_json.json')).schema

# COMMAND ----------

# DBTITLE 1,DEFINE SCHEMA!!!!! 
# THIS IS THE IMPORTANT PART
sc = StructType([
  StructField("Body", StructType([
    StructField("timestamp", LongType()),
    StructField("values", ArrayType(StructType([
      StructField("id", StringType()),
      StructField("q", BooleanType()),
      StructField("t", LongType()),
      StructField("v", LongType())
      
    ])) )
  ])), 
  StructField("EnqueuedTimeUtc", StringType()),
  StructField("SystemProperties", StructType([
    StructField("connectionAuthMethod", StringType()),
    StructField("connectionDeviceGenerationId", StringType()),
    StructField("connectionDeviceId", StringType()),
    StructField("contentEncoding", StringType()),
    StructField("contentType", StringType()),
    StructField("enqueuedTime", StringType())
  ]))
  
])

# COMMAND ----------

# DBTITLE 1,PROVIDE SCHEMA ON READ
display(spark.read.format('json').schema(sc).load('/tmp/ryan.chynoweth@databricks.com/sample_json.json'))

# COMMAND ----------

(spark.read.format('json').schema(sc).load('/tmp/ryan.chynoweth@databricks.com/sample_json.json')).createOrReplaceTempView("json_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT explode(body:`values`) from json_vw

# COMMAND ----------

dbutils.fs.rm('/tmp/ryan.chynoweth@databricks.com/', True)
