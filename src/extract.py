import os
import json
import requests
from datetime import datetime
from google.cloud import storage


# Votre clé API OpenWeatherMap. On la lira depuis une variable d'environnement.
API_KEY = os.environ.get("OPENWEATHER_API_KEY") 

# Le nom du bucket que nous avons créé avec Terraform.
BUCKET_NAME = "tr-weather-pipeline-datalake-2025" 

# La ville pour laquelle nous voulons les données.
CITY_NAME = "Casablanca"


def fetch_weather_data(api_key, city):
    print(f"Récupération des données météo pour {city}...")
    
    url = f"https://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}"
    
    try:
        response = requests.get(url)
        # Cette ligne est une sécurité : elle lèvera une exception si la requête  a échoué
        response.raise_for_status() 
        print("Données récupérées avec succès.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de l'appel à l'API : {e}")
        # On retourne None si l'appel échoue pour que le script s'arrête proprement.
        return None

def upload_to_gcs(data, bucket_name, city):
    """
    Charge les données (un dictionnaire Python) dans un fichier JSON sur GCS.
    """
    if not data:
        print("Aucune donnée à charger. Arrêt.")
        return

    print(f"Chargement des données dans le bucket GCS: {bucket_name}...")
    
    # Initialise le client pour communiquer avec Google Cloud Storage.
    
    client = storage.Client()
    
    # Sélectionne notre bucket.
    bucket = client.bucket(bucket_name)

    # Crée un nom de fichier unique basé sur la date et l'heure actuelles
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    file_name = f"raw_weather_{city.lower()}_{timestamp}.json"
    
    # Crée une "référence" (blob) à notre futur fichier dans le bucket.
    blob = bucket.blob(file_name)
    
    # Convertit le dictionnaire Python en une chaîne de caractères JSON
    # et l'upload dans le fichier. 'indent=2' le rend plus lisible.
    blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
    
    print(f"Fichier '{file_name}' chargé avec succès dans GCS.")




if __name__ == "__main__":
    
    print("Démarrage du pipeline d'extraction.")

    # Vérification que la clé d'API est bien définie.
    if not API_KEY:
        raise ValueError("La variable d'environnement OPENWEATHER_API_KEY n'est pas définie.")

    # 1. Extraire les données de l'API
    weather_data = fetch_weather_data(API_KEY, CITY_NAME)
    
    # 2. Charger les données dans GCS
    # Cette étape ne s'exécutera que si 'weather_data' n'est pas None.
    upload_to_gcs(weather_data, BUCKET_NAME, CITY_NAME)
    
    print("Pipeline d'extraction terminé.")