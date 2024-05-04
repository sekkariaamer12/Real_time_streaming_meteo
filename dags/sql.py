import sqlite3

def create_weather_table():
    try:
        # Connexion à la base de données SQLite
        conn = sqlite3.connect(r"C:\Users\amese\Desktop\Weather\dags\weather_database\weather.sqlite3")

        cur = conn.cursor()
        
        # Exécution de la requête de création de table
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
        
        # Validation des changements dans la base de données
        conn.commit()
        
        # Fermeture de la connexion
        conn.close()
        
        print("Table 'weather' created successfully!")
    except Exception as e:
        print(f"An error occurred while creating the table: {e}")


def select_weather_data():
    try:
        # Connexion à la base de données SQLite
        conn = sqlite3.connect(r"C:\Users\amese\Desktop\Weather\dags\weather_database\weather.sqlite3")
        cur = conn.cursor()
        
        # Exécution de la requête SELECT
        cur.execute("SELECT * FROM weather")
        
        # Récupération des résultats
        rows = cur.fetchall()
        
        # Affichage des résultats
        for row in rows:
            print(row)
        
        # Fermeture de la connexion
        conn.close()
    except Exception as e:
        print(f"An error occurred while selecting data: {e}")



# Exécution de la fonction pour sélectionner les données de la table weather
select_weather_data()


