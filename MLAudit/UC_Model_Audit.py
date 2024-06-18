# Databricks notebook source
import mlflow
import datetime 
mlflow.set_registry_uri("databricks-uc")
client = mlflow.MlflowClient()

# COMMAND ----------

registered_models = client.search_registered_models()

# COMMAND ----------

print(f"--- Number of registered Models: {len(registered_models)}")

# COMMAND ----------

model_names = ""
for n in registered_models:
  model_names += f"'{n.name}',"


names_list = model_names[:-1]
print(names_list)

# COMMAND ----------

spark.sql(f"""
          select service_name, action_name, request_params.full_name_arg as model_name, event_date
          from system.access.audit 
          where service_name = 'unityCatalog' 
              and action_name = 'getRegisteredModel' 
              and request_params.full_name_arg in ( {names_list} )
          """).createOrReplaceTempView("registered_models")

display(spark.sql("select * from registered_models"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select model_name, max(event_date) as last_touched 
# MAGIC from registered_models 
# MAGIC group by model_name

# COMMAND ----------

auto_delete = True
for m in registered_models:
    last_touched = spark.sql(f"select max(event_date) from registered_models where model_name = '{m.name}' ").collect()[0][0]

    print(f"Name: {m.name} | Created: {datetime.datetime.fromtimestamp(m.creation_timestamp/1000)} | Last Updated: {datetime.datetime.fromtimestamp(m.last_updated_timestamp/1000)} | Alias: {m.aliases} | Last Touched: {last_touched}")

if auto_delete: ## likely want to add some other logic here to check if the model is still in use
    client.delete_registered_model(name=m.name)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from system.access.audit 
# MAGIC where service_name = 'unityCatalog' 
# MAGIC               and action_name = 'getRegisteredModel' 
