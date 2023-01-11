-- Databricks notebook source
-- MAGIC %md
-- MAGIC # BI Reporting 
-- MAGIC 
-- MAGIC Integrating machine learning results into a dashboard is an excellent way to analyze performance of a model and for production consumption for end users. With [Databricks SQL](https://databricks.com/product/databricks-sql) users are now able to take advantage of a SQL focused user interface to run analytics directly on top of a data lake. Users are able to run a specialized Databricks runtime that is optimized for SQL Workloads, [Photon](https://docs.databricks.com/runtime/photon.html).  
-- MAGIC 
-- MAGIC Photon is the native vectorized query engine on Databricks, written to be directly compatible with Apache Spark APIs so it works with your existing code. It is developed in C++ to take advantage of modern hardware, and uses the latest techniques in vectorized query processing to capitalize on data- and instruction-level parallelism in CPUs, enhancing performance on real-world data and applications-—all natively on your data lake. Photon is part of a high-performance runtime that runs your existing SQL and DataFrame API calls faster and reduces your total cost per workload.
-- MAGIC 
-- MAGIC In Databricks SQL users can write custom [queries](https://e2-demo-west.cloud.databricks.com/sql/queries/37674f0d-8892-4603-b7a9-c348be80d750/source?o=2556758628403379#23464) using Spark SQL, and then take those queries and build [Dashboards](https://e2-demo-west.cloud.databricks.com/sql/dashboards/65036e8a-9ffa-4ef7-949b-d26996d42021-rac_iot_dashboard?o=2556758628403379). 
-- MAGIC 
-- MAGIC Databricks SQL enables organizations to reduce data copying and eliminate the need for a traditional Data Warehouse. Databricks is now home to BI, ML, and AI use cases.  
-- MAGIC 
-- MAGIC <br></br>
-- MAGIC 
-- MAGIC 
-- MAGIC <img src="https://racadlsgen2.blob.core.windows.net/public/LakehouseIoTDashboard.png"/>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("DatabaseName", "rac_demo_db")
-- MAGIC dbutils.widgets.text("UserName", "ryan.chynoweth@databricks.com")
-- MAGIC database_name = dbutils.widgets.get("DatabaseName")
-- MAGIC user_name = dbutils.widgets.get("UserName")
-- MAGIC spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))
-- MAGIC spark.sql("USE {}".format(database_name))

-- COMMAND ----------

SELECT device_type,
    device_id, 
    reading_1,
    reading_2,
    reading_3,
    predicted_device_operational_status,
    1 AS device_count,
    CASE WHEN reading_1 > reading_2 AND reading_1 > reading_3 THEN reading_1
         WHEN reading_2 > reading_1 AND reading_2 > reading_3 THEN reading_2
         ELSE reading_3 END AS max_reading_value

FROM rac_demo_db.application_output



-- COMMAND ----------


