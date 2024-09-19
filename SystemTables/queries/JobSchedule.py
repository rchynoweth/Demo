# Databricks notebook source
df = spark.sql(
    """
  with jobs_w_schedules as (
    select request_params.job_id
      , workspace_id
      , event_time
      , user_identity.email
      , get_json_object(request_params.new_settings, '$.schedule') as cron_schedule
      , get_json_object(request_params.new_settings, '$.schedule.quartz_cron_expression') as quartz_cron_expression
      , get_json_object(request_params.new_settings, '$.schedule.timezone_id') as timezone_id
      , get_json_object(request_params.new_settings, '$.schedule.pause_status') as pause_status
      , get_json_object(request_params.new_settings, "$.name") as job_name
      , ROW_NUMBER() OVER (PARTITION BY request_params.job_id, workspace_id ORDER BY event_time DESC) AS rn


    from system.access.audit

    where service_name = 'jobs' and action_name in ('create', 'update')
  ) 

  select job_id
      , workspace_id
      , event_time
      , email
      , cron_schedule
      , quartz_cron_expression
      , timezone_id
      , pause_status
      , job_name
  from jobs_w_schedules
  where cron_schedule is not null
  and rn = 1
"""
)

display(df)

# COMMAND ----------

# df = df.limit(10)

# COMMAND ----------

# UDF to convert cron expression to human-readable format
def cron_to_human_readable(cron_expr):
    parts = cron_expr.split()
    
    second_part = parts[0]
    minute_part = parts[1]
    hour_part = parts[2]
    day_of_month_part = parts[3]
    month_part = parts[4]
    day_of_week_part = parts[5]
    
    human_readable = f"At {hour_part}:{minute_part}:{second_part}"

    if day_of_month_part == "*" and day_of_week_part == "?":
        human_readable += " every day"
    elif day_of_month_part == "*":
        human_readable += f" on every {day_of_week_part}"
    elif day_of_week_part == "?":
        human_readable += f" on day {day_of_month_part} of the month"

    return human_readable.strip()


# COMMAND ----------

# Register the UDF
from pyspark.sql.types import StringType
cron_to_human_readable_udf = udf(cron_to_human_readable, StringType())

# COMMAND ----------

# Apply the UDF to transform the DataFrame
df_transformed = df.withColumn("human_readable", cron_to_human_readable_udf(df.quartz_cron_expression))

# COMMAND ----------

display(df_transformed)

# COMMAND ----------


