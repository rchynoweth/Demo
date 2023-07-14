class DDLHelper():

  def __init__(self, spark):
    self.spark = spark # pass the spark object for DDL actions

  def create_cost_lookup_table(self, target_catalog, target_schema):
    """
    Table create for cost lookup table 
    """
    self.spark.sql(f"""
          create table if not exists {target_catalog}.{target_schema}.sku_cost_lookup 
          (
            sku string,
            list_price double
          )
    """                         
    )
  
  def create_consolidated_forecast_view(self, target_catalog, target_schema):
    """
    Forecast Reporting view for consolidated sku i.e. ALL_PURPOSE, SQL, DLT
    """
    self.spark.sql(f"""
        create view if not exists {target_catalog}.{target_schema}.vw_dbu_forecasts
        as 
        select 
        ds as date
        , workspace_id
        , sku
        , y as dbus
        , case when sku='ALL_PURPOSE' and y is not null then 0.55*y  
          when sku='DLT' and y is not null  then 0.36*y
          when sku='JOBS' and y is not null then 0.30*y
          when sku='SQL' and y is not null then 0.22*y 
          when sku='MODEL_INFERENCE' and y is not null then 0.079*y       
          else NULL end as ApproxListCost
        
        , case when sku='ALL_PURPOSE' and y is null then 0.55*yhat  
          when sku='DLT' and y is null  then 0.36*yhat
          when sku='JOBS' and y is null then 0.30*yhat
          when sku='SQL' and y is null then 0.22*yhat 
          when sku='MODEL_INFERENCE' and y is null then 0.079*yhat       
          else NULL end as ApproxForecastListCost

        , case when yhat < 0 then 0 else yhat end as dbus_predicted
        , case when yhat_upper < 0 then 0 else yhat_upper end as dbus_predicted_upper
        , case when yhat_lower < 0 then 0 else yhat_lower end as dbus_predicted_lower
        , case when y > yhat_upper then TRUE else FALSE end as upper_anomaly_alert
        , case when y < yhat_lower then TRUE else FALSE end as lower_anomaly_alert
        , case when y >= yhat_lower AND y <= yhat_upper THEN true ELSE false END AS on_trend
        , training_date
        
        from {target_catalog}.{target_schema}.output_dbu_forecasts_by_date_sku_workspace

    """)


  def create_granular_forecast_view(self, target_catalog, target_schema):
    """
    Forecast Reporting view for granular sku i.e. STANDARD_ALL_PURPOSE_COMPUTE, PREMIUM_ALL_PURPOSE_COMPUTE
    """
    self.spark.sql(f"""
          create view if not exists {target_catalog}.{target_schema}.vw_dbu_granular_forecasts
          as 
          with forecasts as (
            select 
            f.ds as date
            , f.workspace_id
            , f.sku
            , f.y as dbus
            , l.list_price
            , f.y*l.list_price as ApproxListCost
            , f.yhat*l.list_price as ApproxForecastListCost
            , case when f.yhat_lower*l.list_price < 0 then 0 else f.yhat_lower*l.list_price end as ApproxForecastListCostLower
            , f.yhat_upper*l.list_price as ApproxForecastListCostUpper
            , case when f.yhat < 0 then 0 else f.yhat end as dbus_predicted
            , case when f.yhat_upper < 0 then 0 else f.yhat_upper end as dbus_predicted_upper
            , case when f.yhat_lower < 0 then 0 else f.yhat_lower end as dbus_predicted_lower
            , case when f.y > f.yhat_upper then TRUE else FALSE end as upper_anomaly_alert
            , case when f.y < f.yhat_lower then TRUE else FALSE end as lower_anomaly_alert
            , case when f.y >= f.yhat_lower AND f.y <= f.yhat_upper THEN true ELSE false END AS on_trend
            , f.training_date

          from {target_catalog}.{target_schema}.output_dbu_forecasts_by_date_system_sku_workspace f
            inner join {target_catalog}.{target_schema}.sku_cost_lookup l on l.sku = f.sku
            )

            select 
            `date`
            , workspace_id
            , sku
            , dbus
            , list_price
            , ApproxListCost
            , avg(ApproxForecastListCost) OVER (PARTITION BY sku ORDER BY date ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS ApproxForecastListCost
            , avg(ApproxForecastListCostUpper) OVER (PARTITION BY sku ORDER BY date ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS ApproxForecastListCostUpper
            , avg(ApproxForecastListCostLower) OVER (PARTITION BY sku ORDER BY date ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS ApproxForecastListCostLower
            , avg(dbus_predicted) OVER (PARTITION BY sku ORDER BY date ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS dbus_predicted
            , avg(dbus_predicted_upper) OVER (PARTITION BY sku ORDER BY date ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS dbus_predicted_upper
            , avg(dbus_predicted_lower) OVER (PARTITION BY sku ORDER BY date ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS dbus_predicted_lower
            , upper_anomaly_alert
            , lower_anomaly_alert
            , on_trend
            , training_date
            from forecasts 
    """)



  def create_raw_granular_forecast_view(self, target_catalog, target_schema):
    """
    Forecast Reporting view for granular sku i.e. STANDARD_ALL_PURPOSE_COMPUTE, PREMIUM_ALL_PURPOSE_COMPUTE
    """
    self.spark.sql(f"""
          create view if not exists {target_catalog}.{target_schema}.vw_dbu_granular_forecasts
          as 
          select 
          f.ds as date
          , f.workspace_id
          , f.sku
          , f.y as dbus
          , l.list_price
          , f.y*l.list_price as ApproxListCost
          , f.yhat*l.list_price as ApproxForecastListCost
          , case when f.yhat_lower*l.list_price < 0 then 0 else f.yhat_lower*l.list_price end as ApproxForecastListCostLower
          , f.yhat_upper*l.list_price as ApproxForecastListCostUpper
          , case when f.yhat < 0 then 0 else f.yhat end as dbus_predicted
          , case when f.yhat_upper < 0 then 0 else f.yhat_upper end as dbus_predicted_upper
          , case when f.yhat_lower < 0 then 0 else f.yhat_lower end as dbus_predicted_lower
          , case when f.y > f.yhat_upper then TRUE else FALSE end as upper_anomaly_alert
          , case when f.y < f.yhat_lower then TRUE else FALSE end as lower_anomaly_alert
          , case when f.y >= f.yhat_lower AND f.y <= f.yhat_upper THEN true ELSE false END AS on_trend
          , f.training_date
          
          from {target_catalog}.{target_schema}.output_dbu_forecasts_by_date_system_sku_workspace f
            inner join {target_catalog}.{target_schema}.sku_cost_lookup l on l.sku = f.sku

    """)
  
  def insert_into_cost_lookup_table(self, target_catalog, target_schema):
    """
    Function to insert overwrite pricing data for $DBU Estimates on forecasts
    """
    self.spark.sql(f"""insert overwrite {target_catalog}.{target_schema}.sku_cost_lookup values ('PREMIUM_SQL_PRO_COMPUTE_US_CENTRAL', 0.33),
                  ('ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_EUROPE_IRELAND', 0.08),
                  ('ENTERPRISE_SQL_PRO_COMPUTE_AP_SEOUL', 0.74),
                  ('PREMIUM_SERVERLESS_SQL_COMPUTE_US_CENTRAL', 0.42),
                  ('ENTERPRISE_ALL_PURPOSE_COMPUTE', 0.65),
                  ('PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_OHIO', 0.7),
                  ('STANDARD_JOBS_LIGHT_COMPUTE', 0.07),
                  ('PREMIUM_SQL_PRO_COMPUTE_JAPAN_WEST', 0.39),
                  ('PREMIUM_SQL_PRO_COMPUTE_SWEDEN_CENTRAL', 0.36),
                  ('PREMIUM_JOBS_COMPUTE_(PHOTON)', 0.3),
                  ('PREMIUM_ALL_PURPOSE_COMPUTE', 0.55),
                  ('PREMIUM_SQL_PRO_COMPUTE_FRANCE_CENTRAL', 0.36),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_EU_NORTH', 0.08),
                  ('PREMIUM_ALL_PURPOSE_COMPUTE_(PHOTON)', 0.58),
                  ('PREMIUM_JOBS_COMPUTE_(PHOTON)', 0.375),
                  ('STANDARD_ALL_PURPOSE_COMPUTE', 0.4),
                  ('STANDARD_SQL_COMPUTE', 0.22),
                  ('ENTERPRISE_DLT_ADVANCED_COMPUTE', 0.36),
                  ('ENTERPRISE_SQL_PRO_COMPUTE_CANADA', 0.62),
                  ('ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_WEST_OREGON', 0.07),
                  ('ENTERPRISE_DLT_PRO_COMPUTE', 0.25),
                  ('PREMIUM_SERVERLESS_SQL_COMPUTE_EU_WEST', 0.46),
                  ('PREMIUM_SQL_PRO_COMPUTE_AUSTRALIA_CENTRAL', 0.74),
                  ('ENTERPRISE_JOBS_COMPUTE_(PHOTON)', 0.2),
                  ('ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_EUROPE_FRANKFURT', 0.08),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_AP_SINGAPORE', 0.09),
                  ('ENTERPRISE_DLT_ADVANCED_COMPUTE_(PHOTON)', 0.36),
                  ('PREMIUM_DLT_PRO_COMPUTE', 0.25),
                  ('ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_EAST_OHIO', 0.07),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_WEST', 0.33),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_EAST_OHIO', 0.07),
                  ('PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_N_VIRGINIA', 0.7),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_EAST_OHIO', 0.55),
                  ('PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST', 0.42),
                  ('STANDARD_ALL_PURPOSE_COMPUTE', 0.5),
                  ('PREMIUM_DLT_ADVANCED_COMPUTE', 0.54),
                  ('PREMIUM_SQL_PRO_COMPUTE_INDIA_SOUTH', 0.35),
                  ('STANDARD_JOBS_COMPUTE_(PHOTON)', 0.1),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_WEST_CENTRAL', 0.33),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_NORTH_CENTRAL', 0.07),
                  ('PREMIUM_SQL_PRO_COMPUTE_NORWAY_EAST', 0.36),
                  ('PREMIUM_SQL_PRO_COMPUTE_EUROPE_FRANKFURT', 0.72),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_CENTRAL', 0.08),
                  ('ENTERPRISE_SERVERLESS_SQL_COMPUTE_AP_SYDNEY', 0.95),
                  ('PREMIUM_JOBS_LIGHT_COMPUTE', 0.22),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_EAST', 0.33),
                  ('STANDARD_DLT_PRO_COMPUTE', 0.25),
                  ('ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_US_WEST_OREGON', 0.07),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_WEST', 0.08),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_EAST_N_VIRGINIA', 0.07),
                  ('PREMIUM_SQL_COMPUTE', 0.33),
                  ('PREMIUM_DLT_CORE_COMPUTE_(PHOTON)', 0.3),
                  ('ENTERPRISE_DLT_PRO_COMPUTE_(PHOTON)', 0.25),
                  ('PREMIUM_JOBS_SERVERLESS_COMPUTE_EU_WEST', 0.55),
                  ('PREMIUM_SQL_PRO_COMPUTE_AUSTRALIA_EAST', 0.37),
                  ('ENTERPRISE_SQL_PRO_COMPUTE_AP_SYDNEY', 0.74),
                  ('PREMIUM_SQL_PRO_COMPUTE_CANADA_CENTRAL', 0.37),
                  ('PREMIUM_DLT_PRO_COMPUTE_(PHOTON)', 0.38),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_EAST_N_VIRGINIA', 0.55),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_WEST_2', 0.33),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_NORTH_CENTRAL', 0.33),
                  ('STANDARD_DLT_ADVANCED_COMPUTE', 0.54),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_WEST', 0.08),
                  ('ENTERPRISE_SQL_PRO_COMPUTE_AP_MUMBAI', 0.61),
                  ('PREMIUM_SQL_PRO_COMPUTE_UAE_NORTH', 0.37),
                  ('PREMIUM_JOBS_COMPUTE_(PHOTON)', 0.32),
                  ('PREMIUM_SERVERLESS_SQL_COMPUTE_EU_NORTH', 0.46),
                  ('STANDARD_ALL_PURPOSE_COMPUTE_(PHOTON)', 0.4),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_AP_SYDNEY', 0.09),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_EUROPE_IRELAND', 0.08),
                  ('STANDARD_JOBS_COMPUTE', 0.15),
                  ('STANDARD_JOBS_COMPUTE_(PHOTON)', 0.16),
                  ('PREMIUM_SQL_PRO_COMPUTE_KOREA_CENTRAL', 0.37),
                  ('STANDARD_ALL_PURPOSE_COMPUTE_(PHOTON)', 0.42),
                  ('PREMIUM_SQL_PRO_COMPUTE_EUROPE_IRELAND', 0.72),
                  ('STANDARD_DLT_ADVANCED_COMPUTE_(PHOTON)', 0.54),
                  ('PREMIUM_DLT_ADVANCED_COMPUTE', 0.4),
                  ('PREMIUM_JOBS_COMPUTE', 0.22),
                  ('ENTERPRISE_JOBS_LIGHT_COMPUTE', 0.13),
                  ('PREMIUM_SQL_PRO_COMPUTE_ASIA_EAST', 0.74),
                  ('PREMIUM_SQL_PRO_COMPUTE_UAE_NORTH', 0.74),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_WEST', 0.55),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_EUROPE_FRANKFURT', 0.08),
                  ('PREMIUM_SERVERLESS_SQL_COMPUTE_EU_NORTH', 0.91),
                  ('PREMIUM_SQL_PRO_COMPUTE_EU_WEST', 0.72),
                  ('PREMIUM_SQL_PRO_COMPUTE_EU_NORTH', 0.72),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_CENTRAL', 0.55),
                  ('PREMIUM_SQL_PRO_COMPUTE_CANADA_EAST', 0.62),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_EUROPE_FRANKFURT', 0.08),
                  ('PREMIUM_SQL_PRO_COMPUTE_SOUTH_AFRICA_NORTH', 0.74),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_WEST_CENTRAL', 0.55),
                  ('ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_WEST_OREGON', 0.45),
                  ('ENTERPRISE_SQL_PRO_COMPUTE_SA_BRAZIL', 0.85),
                  ('PREMIUM_SQL_PRO_COMPUTE_EUROPE_BELGIUM', 0.72),
                  ('PREMIUM_DLT_CORE_COMPUTE_(PHOTON)', 0.22),
                  ('ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_EUROPE_FRANKFURT', 0.08),
                  ('ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_CANADA', 0.08),
                  ('PREMIUM_DLT_CORE_COMPUTE', 0.22),
                  ('PREMIUM_SQL_PRO_COMPUTE_AUSTRALIA_CENTRAL_2', 0.74),
                  ('PREMIUM_SQL_PRO_COMPUTE_CN_EAST_2', 0.61),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_VIRGINIA', 0.55),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_SOUTH_CAROLINA', 0.55),
                  ('PREMIUM_SQL_PRO_COMPUTE_AUSTRALIA_CENTRAL_2', 0.37),
                  ('PREMIUM_SQL_PRO_COMPUTE_SWITZERLAND_WEST', 0.48),
                  ('PREMIUM_DLT_ADVANCED_COMPUTE', 0.36),
                  ('ENTERPRISE_SQL_PRO_COMPUTE_US_WEST_CALIFORNIA', 0.55),
                  ('ENTERPRISE_SERVERLESS_SQL_COMPUTE_US_EAST_N_VIRGINIA', 0.7),
                  ('PREMIUM_SERVERLESS_SQL_COMPUTE_EUROPE_FRANKFURT', 0.91),
                  ('ENTERPRISE_DLT_CORE_COMPUTE', 0.2),
                  ('PREMIUM_SQL_COMPUTE', 0.22),
                  ('PREMIUM_DLT_PRO_COMPUTE', 0.38),
                  ('STANDARD_DLT_ADVANCED_COMPUTE', 0.36),
                  ('ENTERPRISE_SERVERLESS_SQL_COMPUTE_EUROPE_FRANKFURT', 0.91),
                  ('PREMIUM_SQL_PRO_COMPUTE_EU_WEST', 0.36),
                  ('STANDARD_JOBS_COMPUTE', 0.1875),
                  ('PREMIUM_SQL_PRO_COMPUTE_GERMANY_WEST_CENTRAL', 0.36),
                  ('PREMIUM_SQL_PRO_COMPUTE_AUSTRALIA_SOUTHEAST', 0.37),
                  ('PREMIUM_JOBS_COMPUTE', 0.375),
                  ('ENTERPRISE_SQL_PRO_COMPUTE_EUROPE_IRELAND', 0.72),
                  ('PREMIUM_SERVERLESS_SQL_COMPUTE_US_WEST', 0.42),
                  ('ENTERPRISE_SQL_PRO_COMPUTE_EUROPE_FRANKFURT', 0.72),
                  ('ENTERPRISE_SQL_PRO_COMPUTE_US_EAST_OHIO', 0.55),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_AP_SINGAPORE', 0.09),
                  ('ENTERPRISE_DLT_CORE_COMPUTE_(PHOTON)', 0.2),
                  ('STANDARD_DLT_ADVANCED_COMPUTE', 0.4),
                  ('PREMIUM_SERVERLESS_SQL_COMPUTE_US_WEST', 0.7),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_EU_WEST', 0.08),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_EAST_2', 0.55),
                  ('PREMIUM_JOBS_COMPUTE', 0.15),
                  ('PREMIUM_SQL_PRO_COMPUTE_BRAZIL_SOUTH', 0.43),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_EAST', 0.55),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_WEST_OREGON', 0.55),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_WEST_CALIFORNIA', 0.55),
                  ('ENTERPRISE_SERVERLESS_SQL_COMPUTE_US_EAST_OHIO', 0.7),
                  ('PREMIUM_JOBS_COMPUTE_(PHOTON)', 0.22),
                  ('PREMIUM_SQL_PRO_COMPUTE_JAPAN_EAST', 0.39),
                  ('PREMIUM_SQL_PRO_COMPUTE_INDIA_CENTRAL', 0.31),
                  ('PREMIUM_SERVERLESS_SQL_COMPUTE_EU_WEST', 0.91),
                  ('PREMIUM_SQL_PRO_COMPUTE_CANADA_EAST', 0.37),
                  ('PREMIUM_JOBS_COMPUTE', 0.3),
                  ('ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_US_EAST_OHIO', 0.07),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_WEST_OREGON', 0.07),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_EAST_2', 0.07),
                  ('ENTERPRISE_SERVERLESS_SQL_COMPUTE_US_WEST_OREGON', 0.7),
                  ('PREMIUM_SERVERLESS_SQL_COMPUTE_AP_SYDNEY', 0.95),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_EAST_N_VIRGINIA', 0.07),
                  ('PREMIUM_ALL_PURPOSE_COMPUTE_(PHOTON)', 0.55),
                  ('PREMIUM_SQL_PRO_COMPUTE_AP_TOKYO', 0.78),
                  ('PREMIUM_SQL_PRO_COMPUTE_INDIA_CENTRAL', 0.61),
                  ('PREMIUM_SQL_PRO_COMPUTE_UK_SOUTH', 0.37),
                  ('ENTERPRISE_SQL_COMPUTE', 0.22),
                  ('ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_US_EAST_N_VIRGINIA', 0.07),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_EAST_2', 0.07),
                  ('STANDARD_JOBS_COMPUTE_(PHOTON)', 0.15),
                  ('ENTERPRISE_SQL_PRO_COMPUTE_US_EAST_N_VIRGINIA', 0.55),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_EAST', 0.07),
                  ('PREMIUM_DLT_CORE_COMPUTE', 0.3),
                  ('PREMIUM_SQL_PRO_COMPUTE_ASIA_EAST', 0.37),
                  ('ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_EAST_N_VIRGINIA', 0.07),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_WEST_3', 0.33),
                  ('ENTERPRISE_SQL_PRO_COMPUTE_AP_SINGAPORE', 0.69),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_SOUTH_CENTRAL', 0.55),
                  ('PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST', 0.7),
                  ('PREMIUM_DLT_CORE_COMPUTE_(PHOTON)', 0.2),
                  ('PREMIUM_SQL_PRO_COMPUTE_UK_SOUTH', 0.74),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_AP_SYDNEY', 0.09),
                  ('PREMIUM_SERVERLESS_SQL_COMPUTE_US_CENTRAL', 0.7),
                  ('PREMIUM_DLT_CORE_COMPUTE', 0.2),
                  ('ENTERPRISE_SQL_PRO_COMPUTE_EUROPE_LONDON', 0.74),
                  ('ENTERPRISE_SERVERLESS_SQL_COMPUTE_EUROPE_IRELAND', 0.91),
                  ('STANDARD_JOBS_COMPUTE', 0.1),
                  ('PREMIUM_JOBS_COMPUTE_(PHOTON)', 0.15),
                  ('PREMIUM_SERVERLESS_SQL_COMPUTE_EUROPE_IRELAND', 0.91),
                  ('ENTERPRISE_ALL_PURPOSE_COMPUTE_(PHOTON)', 0.65),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_SOUTH_CENTRAL', 0.33),
                  ('PREMIUM_DLT_ADVANCED_COMPUTE_(PHOTON)', 0.4),
                  ('PREMIUM_ALL_PURPOSE_COMPUTE', 0.6875),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_EUROPE_IRELAND', 0.08),
                  ('PREMIUM_SQL_PRO_COMPUTE_AP_MUMBAI', 0.61),
                  ('PREMIUM_SQL_PRO_COMPUTE_ASIA_SOUTHEAST', 0.35),
                  ('PREMIUM_SQL_PRO_COMPUTE_EU_NORTH', 0.36),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_EAST', 0.07),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_EAST_2', 0.33),
                  ('ENTERPRISE_JOBS_COMPUTE', 0.2),
                  ('PREMIUM_DLT_PRO_COMPUTE_(PHOTON)', 0.25),
                  ('ENTERPRISE_SQL_PRO_COMPUTE_US_WEST_OREGON', 0.55),
                  ('PREMIUM_SQL_PRO_COMPUTE_AP_SINGAPORE', 0.69),
                  ('ENTERPRISE_SQL_PRO_COMPUTE_AP_TOKYO', 0.78),
                  ('PREMIUM_DLT_ADVANCED_COMPUTE_(PHOTON)', 0.54),
                  ('PREMIUM_SERVERLESS_SQL_COMPUTE_US_WEST_OREGON', 0.7),
                  ('PREMIUM_SQL_PRO_COMPUTE_AP_SYDNEY', 0.74),
                  ('PREMIUM_SQL_PRO_COMPUTE_SWITZERLAND_NORTH', 0.43),
                  ('PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_2', 0.42),
                  ('STANDARD_DLT_ADVANCED_COMPUTE_(PHOTON)', 0.4),
                  ('PREMIUM_ALL_PURPOSE_COMPUTE_(PHOTON)', 0.6875),
                  ('PREMIUM_SQL_PRO_COMPUTE_ASIA_SOUTHEAST', 0.69),
                  ('PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_2', 0.7),
                  ('PREMIUM_DLT_ADVANCED_COMPUTE_(PHOTON)', 0.36),
                  ('ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_AP_SYDNEY', 0.09),
                  ('PREMIUM_SQL_PRO_COMPUTE_EUROPE_LONDON', 0.74),
                  ('ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_EUROPE_IRELAND', 0.08),
                  ('PREMIUM_JOBS_LIGHT_COMPUTE', 0.1),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_NORTH_CENTRAL', 0.07),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_EU_WEST', 0.08),
                  ('PREMIUM_SQL_PRO_COMPUTE_UK_WEST', 0.37),
                  ('STANDARD_DLT_CORE_COMPUTE', 0.2),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_WEST_OREGON', 0.07),
                  ('PREMIUM_SQL_PRO_COMPUTE_SOUTH_AFRICA_NORTH', 0.37),
                  ('STANDARD_DLT_ADVANCED_COMPUTE_(PHOTON)', 0.36),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_CENTRAL', 0.08),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_EU_NORTH', 0.08),
                  ('PREMIUM_SQL_PRO_COMPUTE_AP_SEOUL', 0.74),
                  ('PREMIUM_SQL_PRO_COMPUTE_CANADA', 0.62),
                  ('ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_CANADA', 0.08),
                  ('STANDARD_DLT_CORE_COMPUTE_(PHOTON)', 0.3),
                  ('PREMIUM_SQL_PRO_COMPUTE_CANADA_CENTRAL', 0.62),
                  ('ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_AP_SINGAPORE', 0.09),
                  ('PREMIUM_SQL_PRO_COMPUTE_INDIA_SOUTH', 0.69),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_WEST_3', 0.55),
                  ('PREMIUM_SQL_PRO_COMPUTE_BRAZIL_SOUTH', 0.85),
                  ('PREMIUM_SQL_COMPUTE', 0.23),
                  ('PREMIUM_SQL_PRO_COMPUTE_KOREA_CENTRAL', 0.74),
                  ('ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_AP_SINGAPORE', 0.09),
                  ('PREMIUM_SQL_PRO_COMPUTE_AUSTRALIA_SOUTHEAST', 0.74),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_NORTH_CENTRAL', 0.55),
                  ('PREMIUM_SQL_PRO_COMPUTE_JAPAN_EAST', 0.78),
                  ('PREMIUM_SQL_PRO_COMPUTE_GERMANY_WEST_CENTRAL', 0.72),
                  ('PREMIUM_SQL_PRO_COMPUTE_SWEDEN_CENTRAL', 0.72),
                  ('PREMIUM_SQL_PRO_COMPUTE_AUSTRALIA_EAST', 0.74),
                  ('PREMIUM_SQL_PRO_COMPUTE_FRANCE_CENTRAL', 0.72),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_WEST_2', 0.55),
                  ('PREMIUM_SQL_PRO_COMPUTE_SWITZERLAND_NORTH', 0.85),
                  ('ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_AP_SYDNEY', 0.09),
                  ('PREMIUM_SQL_PRO_COMPUTE_NORWAY_EAST', 0.72),
                  ('PREMIUM_JOBS_SERVERLESS_COMPUTE_US_WEST', 0.52),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_EAST_OHIO', 0.07),
                  ('PREMIUM_SQL_PRO_COMPUTE_JAPAN_WEST', 0.78),
                  ('PREMIUM_SQL_PRO_COMPUTE_UK_WEST', 0.74),
                  ('ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_EAST_N_VIRGINIA', 0.45),
                  ('PREMIUM_SQL_PRO_COMPUTE_EUROPE_FRANCE', 0.72),
                  ('PREMIUM_SQL_PRO_COMPUTE_CN_NORTH_3', 0.61),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_IOWA', 0.55),
                  ('PREMIUM_SQL_PRO_COMPUTE_ASIA_SINGAPORE', 0.69),
                  ('PREMIUM_SQL_PRO_COMPUTE_US_OREGON', 0.55),
                  ('STANDARD_SERVERLESS_SQL_COMPUTE', 0.55),
                  ('PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_CANADA', 0.08),
                  ('PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_2', 0.5),
                  ('PREMIUM_SQL_PRO_COMPUTE_INDIA_WEST', 0.35),
                  ('ENTERPRISE_SQL_PRO_COMPUTE_EUROPE_FRANCE', 0.72),
                  ('STANDARD_ALL_PURPOSE_COMPUTE_(PHOTON)', 0.5),
                  ('STANDARD_DLT_CORE_COMPUTE', 0.3),
                  ('STANDARD_DLT_CORE_COMPUTE_(PHOTON)', 0.2),
                  ('PREMIUM_SQL_PRO_COMPUTE_SA_BRAZIL', 0.85),
                  ('PREMIUM_SQL_PRO_COMPUTE_CN_NORTH_2', 0.61);
    """)