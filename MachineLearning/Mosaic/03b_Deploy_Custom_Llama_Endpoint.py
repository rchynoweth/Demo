# Databricks notebook source
# MAGIC %md ##Introduction
# MAGIC
# MAGIC In this notebook, we will deploy the `sentence-transformers/all-MiniLM-L6-v2` model as an API on Databricks. 
# MAGIC
# MAGIC Please note this was developed on a Databricks ML 14.3 LTS cluster.

# COMMAND ----------

# MAGIC %pip install -U sentence-transformers
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text('catalog_name', '')
dbutils.widgets.text('schema_name', '')

catalog_name = dbutils.widgets.get('catalog_name')
schema_name = dbutils.widgets.get('schema_name')

# COMMAND ----------

# create and use UC objects
spark.sql(f"create catalog if not exists {catalog_name}")
spark.sql(f"use catalog {catalog_name}")
spark.sql(f"create schema if not exists {schema_name}")
spark.sql(f"use schema {schema_name}")

# COMMAND ----------

from sentence_transformers import SentenceTransformer
import mlflow
import mlflow.pyfunc
from mlflow.models.signature import infer_signature

mlflow.set_registry_uri('databricks-uc')

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Package solution for Model Serving

# COMMAND ----------

class EncodingModel(mlflow.pyfunc.PythonModel):

  def load_context(self, context): 
    from sentence_transformers import SentenceTransformer
    # Load the model
    embedding_model_name = 'sentence-transformers/all-MiniLM-L6-v2'
    model = SentenceTransformer(embedding_model_name)
  

  def predict(self, context, model_input):
    print(f"---------------- Get raw input {model_input}", flush=True)
    # data = model_input["data"][0] # get the encoded image
    data = model_input[0]
    print(f"---------------- Get individual row {data}", flush=True)

    try :
      print(f"---------------- Getting embeddings", flush=True)
      # embeddings = model_input.apply(lambda x: model.encode([x])[0])
      embeddings = model.encode(data)
      print(f"---------------- Got embeddings {embeddings}", flush=True)
      return embeddings
    except Exception as e:
      print(f"------------ An exception occurred: {str(e)}", flush=True)
      return str(e)




# COMMAND ----------

# Load the pre-trained model
embedding_model_name = 'sentence-transformers/all-MiniLM-L6-v2'
model = SentenceTransformer(embedding_model_name)

reqs = ["sentence-transformers==3.2.0"]
content_list = ["This is a string."]
# pdf = pd.DataFrame({'content': content_list})
# api_output = pdf['content'].apply(lambda x: model.encode([x])[0])
# {"dataframe_split":{"data":[["Welcome to databricks vector search"]],"columns":[0],"index":[0]}}
# pdf = pd.DataFrame({'data': content_list})
# pdf = [{"input":content_list}]
# api_output = [model.encode(p).tolist() for p in pdf[0]]
api_output = model.encode(content_list)
# api_output = pdf['data'].apply(lambda x: model.encode([x])[0])

# COMMAND ----------

# Log the model
with mlflow.start_run(run_name = "rac_embedding_model"):
  model_name = 'rac_embedding_model'
  run = mlflow.active_run()
  signature = infer_signature(content_list, api_output, None)
  mlflow.pyfunc.log_model(
      artifact_path=model_name,
      python_model=EncodingModel(),
      signature=signature, 
      pip_requirements=reqs
  )


    
  model_uri = f'runs:/{run.info.run_id}/{model_name}'
  reg = mlflow.register_model(model_uri=model_uri, name="rac_demo_catalog.rac_demo_db.rac_embedding_model", await_registration_for=600) 
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy to Model Serving 

# COMMAND ----------

from mlflow.deployments import get_deploy_client

client = get_deploy_client("databricks")

# COMMAND ----------

model_name = "rac_embedding_model"
full_model_name = f"{catalog_name}.{schema_name}.{model_name}"
model_version = reg.version
endpoint_name = "rac_embedding_model_endpoint"

# COMMAND ----------

try: 
  client.get_endpoint(endpoint=endpoint_name)
  endpoint_exists = True
except:
  endpoint_exists = False

# COMMAND ----------

endpoint_config = {
          "served_entities": [
              {
                  "name": f"{model_name}-{model_version}",
                  "entity_name": full_model_name,
                  "entity_version": model_version,
                  "workload_size": "Small",
                  "workload_type": "CPU",
                  "scale_to_zero_enabled": False,
              }
          ],
          "auto_capture_config": {
              "catalog_name": catalog_name,
              "schema_name": schema_name,
              "table_name_prefix": model_name,
          },
          "traffic_config": {
              "routes": [
                  {
                      "served_model_name": f"{model_name}-{model_version}",
                      "traffic_percentage": 100,
                  }
              ]
          },
      }

# COMMAND ----------

if endpoint_exists == False:
  endpoint = client.create_endpoint(
      name=endpoint_name,
      config=endpoint_config,
  )
else :
  del endpoint_config['auto_capture_config']
  endpoint = client.update_endpoint(
    endpoint=endpoint_name,
    config=endpoint_config,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC To test the API you can paste the following into the UI test console in Databricks. 
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   "dataframe_records": [
# MAGIC   {
# MAGIC     "content": "Embed this sentence as fast as possible. why is this only returning the number one."
# MAGIC   }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Output: 
# MAGIC ```
# MAGIC {
# MAGIC   "predictions": [
# MAGIC     0.043806903064250946,
# MAGIC     -0.006325399503111839,
# MAGIC     0.08014420419931412,
# MAGIC     ...
# MAGIC     ...
# MAGIC     -0.03335569053888321
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC It appears that when pinging the endpoint upon index creation we send several formats to determine which is the one to use. These were the 5 most recent requests from my last attempt: 
# MAGIC ```
# MAGIC {"dataframe_split":{"data":[["Welcome to databricks vector search"]],"columns":[0],"index":[0]}}	{"error_code": "BAD_REQUEST", "message": "Model is missing inputs ['content']. Note that there were extra inputs: [0]"}
# MAGIC {"dataframe_split":{"data":[["Welcome to databricks vector search"]],"columns":[0],"index":[0]}}	{"error_code": "BAD_REQUEST", "message": "Model is missing inputs ['content']. Note that there were extra inputs: [0]"}
# MAGIC {"input":["Vector Search probe query for endpoint type: EXTERNAL_MODEL"]}	{"error_code": "BAD_REQUEST", "message": "Model is missing inputs ['content']. Note that there were extra inputs: ['input']"}
# MAGIC {"input":["Vector Search probe query for endpoint type: FOUNDATION_MODEL_API"]}	{"error_code": "BAD_REQUEST", "message": "Model is missing inputs ['content']. Note that there were extra inputs: ['input']"}
# MAGIC {"dataframe_split":{"data":[["Vector Search probe query for endpoint type: MODELS"]],"columns":[0],"index":[0]}}	{"error_code": "BAD_REQUEST", "message": "Model is missing inputs ['content']. Note that there were extra inputs: [0]"}
# MAGIC ```
