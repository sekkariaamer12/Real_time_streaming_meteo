import pandas as pd
import json
from datetime import datetime
import requests

ville = 'rabat'
base_url = 'https://api.openweathermap.org/data/2.5/weather?q='


with open ('key.txt','r') as f :
    api_key = f.read()
   # print(api_key)

def convert_kelvin(temp):
  temp_kelv = (temp - 273.15) * (9/5) + 32
  return temp_kelv



full_url = base_url+ville+'&appid='+api_key
#print(full_url)
r = requests.get(full_url)
data = r.json()
print(data)

def etl_weather_data(url):
    r = requests.get(url)
    data = r.json()
    #return(data)

city = data["name"]
weather_description = data["weather"][0]['description']
temp_farenheit = convert_kelvin(data["main"]["temp"])
feels_like_farenheit= convert_kelvin(data["main"]["feels_like"])
min_temp_farenheit = convert_kelvin(data["main"]["temp_min"])
max_temp_farenheit = convert_kelvin(data["main"]["temp_max"])
pressure = data["main"]["pressure"]
humidity = data["main"]["humidity"]
wind_speed = data["wind"]["speed"]
time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (F)": temp_farenheit,
                        "Feels Like (F)": feels_like_farenheit,
                        "Minimun Temp (F)":min_temp_farenheit,
                        "Maximum Temp (F)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time
                        }

transformed_data_list = [transformed_data]
df_data = pd.DataFrame(transformed_data_list)
df_data.to_csv('weather.csv',index =False)


if __name__ == '__main__':
    etl_weather_data(full_url)