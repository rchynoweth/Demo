# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import email
from email.header import decode_header
import pandas as pd
from pyspark.sql.functions import to_timestamp

from email_client import EmailClient 

# COMMAND ----------

# spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

email1 = '/Volumes/rac_demo_catalog/default/email_source/Sample Email with Attachement.eml'
email2 = '/Volumes/rac_demo_catalog/default/email_source/email - Getting mail attachment to python file object - Stack Overflow.eml' 
email3 = '/Volumes/rac_demo_catalog/default/email_source/email - Getting mail attachment to python file object - Stack Overflow - reply.eml' 
attachment_folder = '/Volumes/rac_demo_catalog/default/email_source'

# COMMAND ----------

client = EmailClient(spark=spark, attachement_folder=attachment_folder)

# COMMAND ----------

msg = client.read_email_file(email1)
df = client.load_email_as_spark_df(msg=msg)
display(df)

# COMMAND ----------

attachment_contents, save_path, file_name, attachment_id, attachment = client.get_email_attachement(msg)

# COMMAND ----------

client.save_attachment(attachment_contents=attachment_contents, save_path=save_path)
