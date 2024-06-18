# Databricks notebook source
# MAGIC %md
# MAGIC Add the following environment variable to your cluster. 
# MAGIC ```
# MAGIC NLTK_DATA='/dbfs/nltk/punkt'
# MAGIC ```

# COMMAND ----------

import nltk
import os 
os.environ.get("NLTK_DATA")

# COMMAND ----------

nltk.data.path

# COMMAND ----------

import pandas as pd

# Create a list of strings
data = ['This is a sample sentence. It will be tokenized using the punkt tokenizer.', 'This is a sample sentence. It will be tokenized using the punkt tokenizer.', 'This is a sample sentence. It will be tokenized using the punkt tokenizer.', 'This is a sample sentence. It will be tokenized using the punkt tokenizer.']

# Create a DataFrame from the list
pdf = pd.DataFrame(data, columns=['strings'])

df = spark.createDataFrame(pdf)
display(df)

# COMMAND ----------

nltk.download('punkt', '/dbfs/nltk/punkt')

# COMMAND ----------

dbutils.fs.ls("/nltk/punkt/tokenizers/punkt")

# COMMAND ----------

## add the tokenizer that you need
## I will use english
# sc.addFile('/dbfs/nltk/punkt/tokenizers/punkt/english.pickle')

# COMMAND ----------

# Use the punkt tokenizer
text = "This is a sample sentence. It will be tokenized using the punkt tokenizer."
tokens = nltk.tokenize.word_tokenize(text)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType

# Define the UDF
def transform_string(string):
    # Apply any transformation logic you need
    return nltk.tokenize.word_tokenize(string)

# Register the UDF
transform_udf = F.udf(transform_string, StringType())

# COMMAND ----------

df = df.withColumn('transformed_strings', transform_udf(F.col('strings')))

# COMMAND ----------

display(df)

# COMMAND ----------


