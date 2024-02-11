import requests
import json
from pyspark.sql.functions import udf

class APIExtract():
  """
  Class used for transformations requiring an API call. 
  """

  def __init__(self):
    self.api_udf = udf(self.call_simple_rest_api)

  def call_simple_rest_api(self, url="https://cat-fact.herokuapp.com/facts/"):
    """ Example Rest API call to open API from Postman """
    # public REST API from PostMan: https://documenter.getpostman.com/view/8854915/Szf7znEe
    response = requests.get(url)
    return json.loads(response.text)


