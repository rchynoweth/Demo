import requests
import json



class WeatherAiClient():
  
  def __init__(self, api_key):
    self.api_key = api_key

  def request_data(self, lat, long):
    api_url = "https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&exclude=hourly,daily&appid={}".format(lat, long, self.api_key)
    data = requests.get(api_url)

    return json.loads(data.content.decode("utf-8"))
  

