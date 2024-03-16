# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingesting Email Data 

# COMMAND ----------

# import client lib
from email_client import EmailClient 

# COMMAND ----------

# assigns vars
email1 = '/Volumes/rac_demo_catalog/default/email_source/Test Email with CSV.eml'
email2 = '/Volumes/rac_demo_catalog/default/email_source/email - Getting mail attachment to python file object - Stack Overflow.eml' 
email3 = '/Volumes/rac_demo_catalog/default/email_source/email - Getting mail attachment to python file object - Stack Overflow - reply.eml' 
attachment_folder = '/Volumes/rac_demo_catalog/default/email_source'

# COMMAND ----------

# create client
client = EmailClient(spark=spark, attachment_folder=attachment_folder)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read email file from volume 

# COMMAND ----------

# load a single email as a DF
msg = client.read_email_file(email1)
df = client.load_email_as_spark_df(msg=msg)
display(df)

# COMMAND ----------

# get attachment details
attachment_contents, save_path, file_name, attachment_id, attachment = client.get_email_attachment(msg)

# COMMAND ----------

# save attachment 
client.save_attachment(attachment_contents=attachment_contents, save_path=save_path)

# COMMAND ----------

# save email metadata to table
client.save_email_data_as_table(df=df, table_name='rac_demo_catalog.default.email_dataset')

# COMMAND ----------

# view table of email data
display(spark.read.table('rac_demo_catalog.default.email_dataset'))

# COMMAND ----------

# view contents of attachment 
display(spark.read.csv(save_path, header=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Email Directly from Gmail

# COMMAND ----------

user = '<your username>'
password = '<your password>'
gmail_conn = client.gmail_client(username=user, password=password)

# COMMAND ----------

msg = client.search_gmail(gmail_conn)

# COMMAND ----------

# load a single email as a DF
msg = client.read_email_from_bytes(msg)
df = client.load_email_as_spark_df(msg=msg)
display(df)

# COMMAND ----------

# get attachment details
attachment_contents, save_path, file_name, attachment_id, attachment = client.get_email_attachment(msg)

# COMMAND ----------

# save attachment 
client.save_attachment(attachment_contents=attachment_contents, save_path=save_path)

# COMMAND ----------

# save email metadata to table
client.save_email_data_as_table(df=df, table_name='rac_demo_catalog.default.email_dataset')

# COMMAND ----------

# view table of email data
display(spark.read.table('rac_demo_catalog.default.email_dataset'))

# COMMAND ----------

# view contents of attachment 
display(spark.read.csv(save_path, header=True))

# COMMAND ----------


