from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
import json
from datetime import datetime
from kafka import KafkaProducer
import time
import logging

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 4, 22, 15, 00)
}


def convert_kelvin(temp):
    return (temp - 273.15) * 9/5 + 32

def fetch_weather_data():
    ville = 'rabat'
    base_url = 'https://api.openweathermap.org/data/2.5/weather?q='


    full_url = f"{base_url}{ville}&appid=cc921c2d1b638aa383a72a4e64c2a836"
    r = requests.get(full_url)
    data = r.json()

    return data

import datetime

def transform_weather_data(data):
    transformed_data = {
        "City": data["name"],
        "Description": data["weather"][0]['description'],
        "Temperature (F)": convert_kelvin(data["main"]["temp"]),
        "Feels Like (F)": convert_kelvin(data["main"]["feels_like"]),
        "Minimum Temp (F)": convert_kelvin(data["main"]["temp_min"]),
        "Maximum Temp (F)": convert_kelvin(data["main"]["temp_max"]),
        "Pressure": data["main"]["pressure"],
        "Humidity": data["main"]["humidity"],
        "Wind Speed": data["wind"]["speed"],
        "Time of Record": data['dt'] + data['timezone'],
        "Sunrise (Local Time)": data['sys']['sunrise'] + data['timezone'],
        "Sunset (Local Time)": data['sys']['sunset'] + data['timezone']
    }

    # Convertir les objets datetime en chaînes de caractères
    transformed_data["Time of Record"] = datetime.datetime.utcfromtimestamp(transformed_data["Time of Record"]).isoformat()
    transformed_data["Sunrise (Local Time)"] = datetime.datetime.utcfromtimestamp(transformed_data["Sunrise (Local Time)"]).isoformat()
    transformed_data["Sunset (Local Time)"] = datetime.datetime.utcfromtimestamp(transformed_data["Sunset (Local Time)"]).isoformat()

    return transformed_data


def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            data = fetch_weather_data()
            data = transform_weather_data(data)
            print(data)

            producer.send('users_created', json.dumps(data).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue


with DAG('weather_data_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
