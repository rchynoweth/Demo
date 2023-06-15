# Databricks notebook source
# MAGIC %md
# MAGIC Parsing the cron schedule from the Databricks API data. Databricks uses the [Quartz Cron Schedule](https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html).

# COMMAND ----------

from pyspark.sql.functions import * 
from datetime import datetime
import json

# COMMAND ----------

# Define the JSON data
json_data = ['{"pause_status":"UNPAUSED","timezone_id":"America/Boise","quartz_cron_expression":"0 30 18 ? * Mon,Thu"}',
             '{"pause_status":"UNPAUSED","timezone_id":"America/Boise","quartz_cron_expression":"0 30 15 ? * Mon,Thu"}',
             '{"pause_status":"UNPAUSED","timezone_id":"America/Boise","quartz_cron_expression":"0 15 6 ? * Mon,Thu"}'
            ]

# Create a DataFrame from the JSON data
df = spark.read.json(spark.sparkContext.parallelize(json_data))
display(df)

# COMMAND ----------

character_lookup = {
  '*': 'all values',
  '?': 'no specific value'
}

# COMMAND ----------

# MAGIC %md
# MAGIC UDF to parse the schedule 

# COMMAND ----------

def parse_cron_schedule(cron_expression):

  # get the cron and split the values into a list
  schedule = cron_dict.get('quartz_cron_expression')
  schedule_list = schedule.split(' ')
  output_dict = {}

  output_dict['cron_second'] = schedule_list[0] # required field
  output_dict['cron_minute'] = schedule_list[1] # required field
  output_dict['cron_hour'] = schedule_list[2] # required field
  output_dict['cron_day_of_month'] = schedule_list[3] # required field
  output_dict['cron_month'] = schedule_list[4] # required field
  output_dict['cron_day_of_week'] = schedule_list[5] # required field
  output_dict['cron_year'] = '?' if len(schedule_list)<7 else schedule_list[6] # year is often not present because it is an optional field
  return json.dumps(output_dict) # must return as a proper json object 

parse_cron_schedule_udf = udf(parse_cron_schedule)

# COMMAND ----------

# MAGIC %md
# MAGIC Parse and convert the cron schedule into multiple columns

# COMMAND ----------

display(df.withColumn('cron_datetime', parse_cron_schedule_udf(col('quartz_cron_expression')))
          .withColumn('cron_second', get_json_object('cron_datetime', '$.cron_second'))
          .withColumn('cron_minute', get_json_object('cron_datetime', '$.cron_minute'))
          .withColumn('cron_hour', get_json_object('cron_datetime', '$.cron_hour'))
          .withColumn('cron_day_of_month', get_json_object('cron_datetime', '$.cron_day_of_month'))
          .withColumn('cron_day_of_week', get_json_object('cron_datetime', '$.cron_day_of_week'))
          .withColumn('cron_month', get_json_object('cron_datetime', '$.cron_month'))
          .withColumn('cron_year', get_json_object('cron_datetime', '$.cron_year'))
          .withColumn('pacific_cron_hour', when(col('cron_hour').cast('int').isNotNull(), col('cron_hour').cast('int') - 1).otherwise(col('cron_hour'))) # boise is 1 hour ahead
        )

# COMMAND ----------


