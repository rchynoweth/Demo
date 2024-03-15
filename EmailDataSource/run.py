# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

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
client = EmailClient(spark=spark, attachement_folder=attachment_folder)

# COMMAND ----------

# load a single email as a DF
msg = client.read_email_file(email1)
df = client.load_email_as_spark_df(msg=msg)
display(df)

# COMMAND ----------

# get attachement details
attachment_contents, save_path, file_name, attachment_id, attachment = client.get_email_attachement(msg)

# COMMAND ----------

# save attachement 
client.save_attachment(attachment_contents=attachment_contents, save_path=save_path)

# COMMAND ----------

# save email metadata to table
client.save_email_data_as_table(df=df, table_name='rac_demo_catalog.default.email_dataset')

# COMMAND ----------

# view table of email data
display(spark.read.table('rac_demo_catalog.default.email_dataset'))

# COMMAND ----------

# view contents of attachement 
display(spark.read.csv(save_path, header=True))
