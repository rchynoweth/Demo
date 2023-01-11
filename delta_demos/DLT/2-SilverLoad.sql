-- Databricks notebook source
CREATE LIVE TABLE T2_ETL
AS 
    SELECT
      MIN(encounter_id) encounter_id,
      patient,
      encounterclass,
      VISIT_START_DATE,
      MAX(VISIT_END_DATE) AS VISIT_END_DATE
    FROM live.T1_ETL

    GROUP BY
      patient,
      encounterclass,
      VISIT_START_DATE

-- COMMAND ----------

CREATE LIVE TABLE ER_VISITS_DLT
AS 
  SELECT
    encounter_id,
    patient,
    encounterclass,
    VISIT_START_DATE,
    VISIT_END_DATE
  FROM live.T2_ETL
