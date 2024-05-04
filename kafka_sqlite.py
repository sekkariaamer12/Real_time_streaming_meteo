import requests
import json
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
import sqlite3
import time
import logging



# Fonction pour convertir la température de Kelvin à Fahrenheit
def convert_kelvin(temp):
    return (temp - 273.15) * 9/5 + 32

# Fonction pour récupérer les données météorologiques depuis l'API
def fetch_weather_data():
    ville = 'rabat'
    base_url = 'https://api.openweathermap.org/data/2.5/weather?q='
    api_key = 'cc921c2d1b638aa383a72a4e64c2a836'

    full_url = f"{base_url}{ville}&appid={api_key}"
    r = requests.get(full_url)
    data = r.json()

    return data

# Fonction pour transformer les données météorologiques
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
    transformed_data["Time of Record"] = datetime.utcfromtimestamp(transformed_data["Time of Record"]).isoformat()
    transformed_data["Sunrise (Local Time)"] = datetime.utcfromtimestamp(transformed_data["Sunrise (Local Time)"]).isoformat()
    transformed_data["Sunset (Local Time)"] = datetime.utcfromtimestamp(transformed_data["Sunset (Local Time)"]).isoformat()

    return transformed_data

# Fonction pour envoyer les données transformées à Kafka
def stream_data(max_messages=10):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    message_count = 0

    while message_count < max_messages:
        try:
            data = fetch_weather_data()
            data = transform_weather_data(data)
            print(data)

            producer.send('weather_data', json.dumps(data).encode('utf-8'))
            message_count += 1

            if message_count == max_messages:
                break

        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue


# Fonction pour consommer les données de Kafka et les insérer dans SQLite
def consume_data():
    try:
        consumer = KafkaConsumer('weather_data', bootstrap_servers=['localhost:9092'], group_id='weather_group')
        conn = sqlite3.connect('dags/weather_database/weather.sqlite3')
        cur = conn.cursor()

        cur.execute('''
            CREATE TABLE IF NOT EXISTS weather (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city TEXT,
                description TEXT,
                temperature_f REAL,
                feels_like_f REAL,
                min_temp_f REAL,
                max_temp_f REAL,
                pressure INTEGER,
                humidity INTEGER,
                wind_speed REAL,
                time_of_record TEXT,
                sunrise_local_time TEXT,
                sunset_local_time TEXT
            )
        ''')
        conn.commit()

        for message in consumer:
            try:
                data = json.loads(message.value.decode('utf-8'))
                cur.execute('''
                    INSERT INTO weather (city, description, temperature_f, feels_like_f,
                    min_temp_f, max_temp_f, pressure, humidity, wind_speed, time_of_record,
                    sunrise_local_time, sunset_local_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (data["City"], data["Description"], data["Temperature (F)"], data["Feels Like (F)"],
                      data["Minimum Temp (F)"], data["Maximum Temp (F)"], data["Pressure"],
                      data["Humidity"], data["Wind Speed"], data["Time of Record"],
                      data["Sunrise (Local Time)"], data["Sunset (Local Time)"]))
                conn.commit()
                logging.info(f"Inserted data for city {data['City']} into SQLite.")
            except Exception as e:
                logging.error(f'An error occurred while processing data from Kafka: {e}')
                continue

        conn.close()
    except Exception as e:
        logging.error(f'An error occurred while consuming data from Kafka: {e}')


# Exécution des fonctions
stream_data()
consume_data()  # Arrête la consommation de données après 60 secondes


