-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Live Table Pipeline 

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE raw_weather_dlt
COMMENT "The raw data from weather api"
AS SELECT * FROM json.`/Users/ryan.chynoweth@databricks.com/api_weather_demo/raw/*.json`


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE bronze_weather_dlt_recorded_data
COMMENT "BRONZE: recorded data table for weather dataset"
AS 
SELECT base, clouds.all as clouds, coord.lat as latitude, coord.lon as longitude, dt as timestamp, from_unixtime(dt) as datetime, to_date(from_unixtime(dt)) as date, id, main.feels_like as percieved_temp, main.humidity, main.pressure, main.temp, main.temp_max, main.temp_min, name as city_name  FROM live.raw_weather_dlt

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE silver_weather_dlt_daily_recorded_data_agg
COMMENT "SILVER: recorded data table for weather dataset aggregated at a daily level"
AS 
SELECT city_name, date, avg(percieved_temp) as avg_percieved_temp, avg(humidity) as avg_humidity, avg(pressure) as avg_pressure  FROM live.bronze_weather_dlt_recorded_data GROUP BY city_name, date

-- COMMAND ----------



-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE bronze_weather_dlt_sys
COMMENT "The raw data from weather api"
AS SELECT sys.country, sys.id, sys.sunrise, from_unixtime(sys.sunrise) as sunrise_datetime, sys.sunset, from_unixtime(sys.sunset) as sunset_datetime, sys.type FROM live.raw_weather_dlt


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE bronze_weather_dlt_atmosphere
COMMENT "The raw data from weather api"
AS 
SELECT weather[0].description as description, weather[0].icon as icon, weather[0].main as main, wind.deg, wind.gust, wind.speed
FROM live.raw_weather_dlt

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

-- DROP TABLE IF EXISTS weather_raw_dlt
-- DROP TABLE IF EXISTS bronze_weather_dlt_recorded_data
