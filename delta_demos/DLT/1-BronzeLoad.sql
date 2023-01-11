-- Databricks notebook source
-- MAGIC %md
-- MAGIC Link to [DLT Pipeline](https://eastus2.azuredatabricks.net/?o=5206439413157315&owned-by-me=true&name-order=ascend#joblist/pipelines/5b71c79d-6d54-4bac-9c3c-b4f4bf1f3062)

-- COMMAND ----------

CREATE LIVE TABLE encounters_etl 
AS 
SELECT * FROM demo_omop_database.encounters


-- COMMAND ----------

CREATE LIVE TABLE T1_ETL
AS 
  SELECT
    CL1.id encounter_id,
    CL1.patient,
    CL1.encounterclass,
    CL1.start VISIT_START_DATE,
    CL1.stop VISIT_END_DATE
  FROM
    live.encounters_etl CL1
  WHERE
    CL1.encounterclass in ('emergency', 'urgent')
