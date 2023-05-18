import logging
import os
import azure.functions as func
import requests
import json
import pandas as pd
from azure.storage.blob import BlobServiceClient

hourlist=[]
timestamp=[]
temp_c=[]
wind_kph=[]
humidity=[]
wind_dir=[]
condition=[]
humidity=[]
feelslike_c=[]
date=[]
time=[]


def extractdate(timestring):
  return timestring.split(" ")[0]

def extracttime(timestring):
  return timestring.split(" ")[1]

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function got a request')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')
# Calling API
    url='https://api.weatherapi.com/v1/forecast.json'
    params={
        'key': os.environ['key'],
        'q':name ,
        'days':'10'
            }
    response=requests.get(url,params=params)
    json_response=response.json()

    #parsing of the data
    forecastday=json_response['forecast']['forecastday']
    for i in range(len(forecastday)):
        hourlist.append(forecastday[i]['hour'])
    logging.info('Parsing the JSON')
    for i in range(len(hourlist)):
        templist=hourlist[i]
        for j in range(len(templist)):
            timestamp.append(hourlist[i][j]['time'])
            temp_c.append(hourlist[i][j]['temp_c'])
            wind_kph.append(hourlist[i][j]['wind_kph'])
            wind_dir.append(hourlist[i][j]['wind_dir'])
            condition.append(hourlist[i][j]['condition']['text'])
            humidity.append(hourlist[i][j]['humidity'])
            feelslike_c.append(hourlist[i][j]['feelslike_c'])
            date.append(extractdate(hourlist[i][j]['time']))
            time.append(extracttime(hourlist[i][j]['time']))

    logging.info('Creating the dataframe')
    weatherdata=pd.DataFrame(zip(timestamp,temp_c,wind_kph,wind_dir,condition,humidity,feelslike_c,date,time),columns=['Timestamp','TemperatureInCelcius','WindinKph','WindDirection','Condition','Humidity','FeelsLikeinCelcius','Date','Time'])
    weatherdata_csv=weatherdata.to_csv()
    
    logging.info('Uploading to Blob')
    connection_string=os.environ['connection_string']
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client('dataengineercontainer')
    blob_client = container_client.get_blob_client("output.csv")
    blob_client.upload_blob(weatherdata_csv, blob_type="BlockBlob",overwrite=True)
    logging.info('Clearing the lists')
    hourlist.clear()
    timestamp.clear()
    temp_c.clear()
    wind_kph.clear()
    humidity.clear()
    wind_dir.clear()
    condition.clear()
    humidity.clear()
    feelslike_c.clear()
    date.clear()
    time.clear()


    if name:
        return func.HttpResponse(f"Hello, function triggered for {name}, with {weatherdata.shape[0]} records")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
