"""
import os           # Pour interagir avec le système d'exploitation (lire les variables d'environnement).
import json         # Pour manipuler le format de données JSON.
import requests     # Pour faire des appels sur internet (à l'API).
from datetime import datetime # Pour travailler avec les dates et heures (pour nommer notre fichier).
from google.cloud import storage 

# On définit nos paramètres.
API_KEY = os.environ.get("OPENWEATHER_API_KEY") 
# os.environ.get(...) est la manière propre et sécurisée de lire une information sensible.
# Plutôt que d'écrire la clé ici, on dit au script : "Va chercher une variable
# dans le système qui s'appelle OPENWEATHER_API_KEY". C'est ce qu'on fait
# avec la commande 'export' ou 'set'.

BUCKET_NAME = "tr-weather-pipeline-datalake-2025" 
# Ici, on peut écrire le nom en dur, car ce n'est pas un secret.

CITY_NAME = "Casablanca"
# Le nom de la ville qui nous intéresse.

def fetch_weather_data(api_key, city):
    # On lui donne en entrée les deux choses dont elle a besoin : la clé et la ville.
    
    print(f"Récupération des données météo pour {city}...")
    
    # On construit l'URL exacte en assemblant les morceaux.
    url = f"https://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}"
    
    # On fait l'appel. C'est comme taper l'URL dans un navigateur.
    response = requests.get(url)
    
    # C'est une ligne de sécurité TRÈS importante.
    # Si le site répond "Erreur 404 (Not Found)" ou "Erreur 401 (Unauthorized)",
    # cette ligne va planter le script, ce qui est une bonne chose !
    # Ça évite de continuer avec des données invalides.
    response.raise_for_status() 
    
    # Si tout s'est bien passé, la réponse de l'API est du texte au format JSON.
    # .json() le transforme en un dictionnaire Python, beaucoup plus facile à manipuler.
    return response.json()

def upload_to_gcs(data, bucket_name, city):
    # On lui donne les données à charger, le nom du bucket où les mettre, et la ville (pour le nom du fichier).
    
    print(f"Chargement des données dans le bucket GCS: {bucket_name}...")
    
    # 1. On se connecte à Google Cloud.
    client = storage.Client()
    # C'est ici que la magie opère. Parce qu'on a fait "gcloud auth application-default login",
    # cette simple ligne trouve toute seule les identifiants et se connecte.
    
    # 2. On sélectionne le seau (bucket) qui nous intéresse.
    bucket = client.bucket(bucket_name)

    # 3. On invente un nom de fichier qui ne risque pas d'exister déjà.
    # On prend la date et l'heure actuelles, on les formate en texte.
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S") # Ex: "2023-10-27_163000"
    file_name = f"raw_weather_{city.lower()}_{timestamp}.json" # Ex: "raw_weather_casablanca_2023-10-27_163000.json"
    
    # 4. On dit au bucket : "Je vais créer un fichier qui s'appellera comme ça".
    blob = bucket.blob(file_name)
    
    # 5. On prend nos données (qui sont un dictionnaire Python), on les re-transforme
    # en texte au format JSON, et on les envoie dans le fichier.
    blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
    # json.dumps() fait l'inverse de .json() : Python -> Texte JSON.
    
    print(f"Fichier '{file_name}' chargé avec succès dans GCS.")


if __name__ == "__main__":
    
        print("Démarrage du pipeline d'extraction.")
        
        # Avant tout, vérifions que la clé d'API a bien été fournie.
        # C'est une sécurité pour éviter une erreur plus tard.
        if not API_KEY:
            raise ValueError("La variable d'environnement OPENWEATHER_API_KEY n'est pas définie.")

        # Étape 1 : Appeler notre premier ouvrier pour aller chercher la donnée.
        weather_data = fetch_weather_data(API_KEY, CITY_NAME)
        
        # Étape 2 : Si l'ouvrier n°1 est revenu avec quelque chose...
        if weather_data:
            # ...alors on appelle l'ouvrier n°2 pour déposer la donnée.
            upload_to_gcs(weather_data, BUCKET_NAME, CITY_NAME)
        
        print("Pipeline d'extraction terminé.")
        """

# =================================================================
# Script d'Extraction (E) du pipeline ELT Météo
# =================================================================

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
    """
    Appelle l'API OpenWeatherMap pour une ville donnée et retourne les données JSON.
    """
    print(f"Récupération des données météo pour {city}...")
    
    # L'URL de l'API. On utilise une f-string pour insérer la ville et la clé d'API.
    # L'endpoint "forecast" nous donne des prévisions sur 5 jours toutes les 3 heures.
    url = f"https://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}"
    
    try:
        response = requests.get(url)
        # Cette ligne est une sécurité : elle lèvera une exception si la requête
        # a échoué (ex: erreur 401, 404, 500).
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
    # Comme on a fait "gcloud auth application-default login", le client
    # trouvera automatiquement les identifiants pour se connecter.
    client = storage.Client()
    
    # Sélectionne notre bucket.
    bucket = client.bucket(bucket_name)

    # Crée un nom de fichier unique basé sur la date et l'heure actuelles
    # pour ne pas écraser les fichiers précédents.
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    file_name = f"raw_weather_{city.lower()}_{timestamp}.json"
    
    # Crée une "référence" (blob) à notre futur fichier dans le bucket.
    blob = bucket.blob(file_name)
    
    # Convertit le dictionnaire Python en une chaîne de caractères JSON
    # et l'upload dans le fichier. 'indent=2' le rend plus lisible.
    blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
    
    print(f"Fichier '{file_name}' chargé avec succès dans GCS.")


# --- Point d'entrée du script ---
# C'est la partie qui s'exécute quand on lance "python extract.py".
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