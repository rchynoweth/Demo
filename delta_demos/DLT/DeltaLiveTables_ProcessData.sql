-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ETL Pipelines with Delta Live Tables
-- MAGIC 
-- MAGIC Why Delta live tables? 
-- MAGIC - Complex pipeline developemnt 
-- MAGIC   - Makes it easy to build and maintain dependencies 
-- MAGIC   - Easier to switch between batch and stream processing
-- MAGIC   
-- MAGIC - Solves Data Quality 
-- MAGIC   - Enforce data quality rules and monitor pipelines
-- MAGIC   - Data lineage traceability
-- MAGIC   
-- MAGIC - Enhances pipeline operations
-- MAGIC   - Observability at a granular level 
-- MAGIC   - Error handling and recovery made easy
-- MAGIC   
-- MAGIC 
-- MAGIC Noteworthy details: 
-- MAGIC - Live tables are managed by the pipeline but are still delta tables and can be incorporated outside of the delta live tables environment. 
-- MAGIC - Pipeline tables need to use the `live.table_name` to reference each other. 
-- MAGIC - Pipelines can be configured to register the live tables to the metastore using the `target` parameter in the pipeline json. 
-- MAGIC - INCREMENTAL live tables are processed as a stream, if incremental is not used we will process entire datasets each execution  
-- MAGIC 
-- MAGIC 
-- MAGIC Tables can be incremental or complete. Incremental tables support updates based on continually arriving data without having to recompute the entire table. A complete table is entirely recomputed with each update.

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE nyctaxi_bronze_live 
AS 
SELECT * FROM STREAM(rac_demo_db.nyctaxi_bronze)

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE stg_nyctaxi_daily_aggs
AS 
SELECT date(pickup_datetime) as pickup_date
, count(1) as pickup_counts
-- , lag(count(1)) OVER (ORDER BY date(pickup_datetime)) as yesterday_pickup_counts

from STREAM(live.nyctaxi_bronze_live) 
group by date(pickup_datetime)


-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE nyctaxi_daily_aggs
AS 
SELECT pickup_date, pickup_counts
--, yesterday_pickup_counts 
-- ,(pickup_counts - yesterday_pickup_counts) as daily_diff

from STREAM(live.stg_nyctaxi_daily_aggs)
