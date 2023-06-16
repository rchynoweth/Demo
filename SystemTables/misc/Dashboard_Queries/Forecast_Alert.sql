select * 
from main.prophet_forecast_schema.vw_dbu_granular_forecasts
where on_trend=False
order by date desc