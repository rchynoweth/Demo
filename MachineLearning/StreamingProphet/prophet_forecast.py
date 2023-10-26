from prophet import Prophet
from pyspark.sql.functions import *
from pyspark.sql.types import * 
import pandas as pd



class ProphetForecast():
  """
  Class for DBU Forecasting
  """
  
  def __init__(self, forecast_periods=90, interval_width=0.85, forecast_frequency='d', include_history=True):
    """
    Initilization function

    
    :param forecast_periods: Periods to forecast. Default is 7. 
    :param interval_width: confidence level of min/max thresholds. Default is 0.85
    :param forecast_frequency: frequency of the ds column. Default is daily i.e. 'd'
    :param include_history: whether or not to include history in the output dataframe. Default is True. 
    """
    self.forecast_periods=forecast_periods
    self.forecast_frequency=forecast_frequency
    self.include_history=include_history
    self.interval_width=interval_width
    
    
    # Training output schema 
    self.forecast_result_schema = StructType([
      StructField('ds',DateType()),
      StructField('sku',StringType()),
      StructField('sendTime', TimestampType()),
      StructField('y',FloatType()),
      StructField('yhat',FloatType()),
      StructField('yhat_upper',FloatType()),
      StructField('yhat_lower',FloatType())
      ])
    
    self.generate_forecast_udf = udf(self.generate_forecast)


  def generate_forecast(self, history_pd):
    """
    Function to generate forecasts 

    NOTE: Pandas UDFs inside classes must be static 
    """
    # remove missing values (more likely at day-store-item level)
    history_pd = history_pd.dropna()
    
    # train and configure the model
    model = Prophet( interval_width=self.interval_width )
    model.fit( history_pd )

    # make predictions
    future_pd = model.make_future_dataframe(
      periods=self.forecast_periods, 
      freq=self.forecast_frequency, 
      include_history=self.include_history
      )
    forecast_pd = model.predict( future_pd )  
    
    # ASSEMBLE EXPECTED RESULT SET
    # --------------------------------------
    # get relevant fields from forecast
    f_pd = forecast_pd[ ['ds','yhat', 'yhat_upper', 'yhat_lower'] ].set_index('ds')
    
    # get relevant fields from history
    h_pd = history_pd[['ds', 'sendTime','sku','y']].set_index('ds')
    
    # join history and forecast
    results_pd = f_pd.join( h_pd, how='left' )
    results_pd.reset_index(level=0, inplace=True)
    
    # get sku & workspace id from incoming data set
    results_pd['sku'] = history_pd['sku'].iloc[0]
    results_pd['sendTime'] = history_pd['sendTime'].iloc[0]

    return results_pd[ ['ds', 'sku', 'sendTime', 'y', 'yhat', 'yhat_upper', 'yhat_lower'] ]  

