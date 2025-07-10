import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, explode, round

BUCKET_NAME = "tr-weather-pipeline-datalake-2025" 
BIGQUERY_DATASET = "weather_data"
BIGQUERY_TABLE = "casablanca_forecasts"
BIGQUERY_TEMP_BUCKET = "tr-weather-pipeline-bq-temp-2025"

def get_latest_file_from_gcs(bucket_name):
    from google.cloud import storage
    print("Recherche du dernier fichier dans le Data Lake...")
    # Le projet sera automatiquement détecté depuis les credentials
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs())
    if not blobs:
        raise FileNotFoundError(f"Aucun fichier trouvé dans le bucket GCS '{bucket_name}'.")
    latest_blob = sorted(blobs, key=lambda b: b.time_created, reverse=True)[0]
    print(f"Fichier à traiter trouvé : {latest_blob.name}")
    return f"gs://{bucket_name}/{latest_blob.name}"

if __name__ == "__main__":
    
    print("Création de la session Spark...")
    
    # Configuration Spark avec les connecteurs nécessaires
    spark = SparkSession.builder \
        .appName("WeatherTransformCompose") \
        .config("spark.jars", "/app/spark-jars/gcs-connector-hadoop3-latest.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.type", "APPLICATION_DEFAULT") \
        .getOrCreate()

    # Configuration additionnelle pour GCS
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if project_id:
        spark.conf.set("spark.hadoop.google.cloud.project.id", project_id)
    
    print("Session Spark créée et configurée.")

    try:
        input_path = get_latest_file_from_gcs(BUCKET_NAME)
        print(f"Lecture du fichier JSON depuis {input_path}...")
        raw_df = spark.read.option("multiline", "true").json(input_path)
        
        print("Schéma des données brutes lues depuis le JSON :")
        raw_df.printSchema()
        
        print("Application des transformations sur les données...")
        exploded_df = raw_df.select(explode("list").alias("forecast"), "city")
        transformed_df = exploded_df.select(
            col("city.name").alias("city_name"),
            from_unixtime(col("forecast.dt")).alias("forecast_time"),
            (round(col("forecast.main.temp") - 273.15, 2)).alias("temperature_celsius"),
            col("forecast.main.pressure").alias("pressure"),
            col("forecast.wind.speed").alias("wind_speed"),
            col("forecast.weather")[0].getItem("description").alias("description")
        )
        
        print("Transformation terminée. Affichage du résultat :")
        transformed_df.show(truncate=False)
        
        print("Schéma final des données transformées :")
        transformed_df.printSchema()
        
        # Optionnel : Sauvegarde dans BigQuery si les credentials sont configurés
        if project_id and os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            print("Chargement des données dans BigQuery...")
            # Ajout du connecteur BigQuery
            transformed_df.write \
                .format("bigquery") \
                .option("table", f"{project_id}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}") \
                .option("temporaryGcsBucket", BIGQUERY_TEMP_BUCKET) \
                .mode("append") \
                .save()
            print("Données chargées avec succès dans BigQuery.")
        else:
            print("Configuration BigQuery manquante, données transformées uniquement.")

    except Exception as e:
        print(f"Une erreur est survenue pendant le traitement Spark : {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'spark' in locals() and spark:
            spark.stop()
        print("Session Spark arrêtée.")