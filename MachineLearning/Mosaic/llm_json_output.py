# Databricks notebook source
# MAGIC %pip install langchain

# COMMAND ----------

from langchain_core.messages import AIMessage
from langchain_core.output_parsers import JsonOutputParser

incomplete_json_string = '{"foo": "bar"'

output_parser = JsonOutputParser()
output_json = output_parser.parse(incomplete_json_string)
output_json

# COMMAND ----------

type(output_json)

# COMMAND ----------

output_json.get('foo')

# COMMAND ----------


