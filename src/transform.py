import os
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, explode, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

BUCKET_NAME = "tr-weather-pipeline-datalake-2025" 
BIGQUERY_DATASET = "weather_data"
BIGQUERY_TABLE = "casablanca_forecasts"
BIGQUERY_TEMP_BUCKET = "tr-weather-pipeline-bq-temp-2025"

def get_latest_file_from_gcs(bucket_name):
    from google.cloud import storage
    print("Recherche du dernier fichier dans le Data Lake...")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs())
    if not blobs:
        raise FileNotFoundError(f"Aucun fichier trouvé dans le bucket GCS '{bucket_name}'.")
    latest_blob = sorted(blobs, key=lambda b: b.time_created, reverse=True)[0]
    print(f"Fichier à traiter trouvé : {latest_blob.name}")
    return f"gs://{bucket_name}/{latest_blob.name}"

def get_project_id():
    try:
        # Méthode 1: Variable d'environnement
        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
        if project_id:
            return project_id
        
        # Méthode 2: Metadata service (pour les VMs GCP)
        result = subprocess.run([
            "gcloud", "config", "get-value", "project"], capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
        
        # Méthode 3: Via la bibliothèque google-cloud
        from google.cloud import storage
        client = storage.Client()
        return client.project
        
    except Exception as e:
        print(f"Erreur lors de la récupération du project ID: {e}")
        return None

def verify_bigquery_data(project_id, dataset_id, table_id):
    """Vérifie si les données ont été correctement insérées dans BigQuery"""
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=project_id)
        
        # Vérifier si la table existe
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        try:
            table = client.get_table(table_ref)
            print(f"Table {table_ref} existe avec {table.num_rows} lignes")
            
            # Exécuter une requête pour vérifier les données
            query = f"""
            SELECT COUNT(*) as total_rows,
                   MIN(forecast_time) as earliest_forecast,
                   MAX(forecast_time) as latest_forecast,
                   COUNT(DISTINCT city_name) as unique_cities
            FROM `{table_ref}`
            """
            
            query_job = client.query(query)
            results = query_job.result()
            
            for row in results:
                print(f"Données dans BigQuery:")
                print(f"  - Total des lignes: {row.total_rows}")
                print(f"  - Première prévision: {row.earliest_forecast}")
                print(f"  - Dernière prévision: {row.latest_forecast}")
                print(f"  - Villes uniques: {row.unique_cities}")
                
            return True
            
        except Exception as e:
            print(f"Erreur lors de la vérification de la table: {e}")
            return False
            
    except Exception as e:
        print(f"Erreur lors de la vérification BigQuery: {e}")
        return False

if __name__ == "__main__":
    
    print("Création de la session Spark...")

    gcs_connector = "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.0"
    bq_connector = "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0"

    # Configuration Spark avec les connecteurs nécessaires
    spark = SparkSession.builder \
        .appName("WeatherTransformCompose") \
        .config("spark.jars", "/app/spark-jars/gcs-connector-hadoop3-latest.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.jars.packages", f"{gcs_connector},{bq_connector}") \
        .config("spark.hadoop.google.cloud.auth.type", "APPLICATION_DEFAULT") \
        .getOrCreate()

    # Récupération du project ID
    project_id = get_project_id()
    print(f"Project ID détecté: {project_id}")
    
    # Configuration additionnelle pour GCS si project_id est disponible
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
        
        # Vérification des données avant insertion
        row_count = transformed_df.count()
        print(f"Nombre de lignes à insérer: {row_count}")
        
        # Vérification des valeurs nulles
        null_counts = []
        for column in transformed_df.columns:
            null_count = transformed_df.filter(col(column).isNull()).count()
            null_counts.append(f"{column}: {null_count} nulls")
        print("Valeurs nulles par colonne:")
        for null_info in null_counts:
            print(f"  - {null_info}")
        
        print("Schéma final des données transformées :")
        transformed_df.printSchema()
        
        # Sauvegarde dans BigQuery si le project_id est disponible
        if project_id:
            print("Chargement des données dans BigQuery...")
            try:
                # Vérification que le dataset existe
                from google.cloud import bigquery
                bq_client = bigquery.Client(project=project_id)
                
                # Création du dataset s'il n'existe pas
                dataset_id = f"{project_id}.{BIGQUERY_DATASET}"
                try:
                    bq_client.get_dataset(dataset_id)
                    print(f"Dataset {dataset_id} existe déjà")
                except:
                    dataset = bigquery.Dataset(dataset_id)
                    dataset.location = "US"  # ou votre région préférée
                    dataset = bq_client.create_dataset(dataset, timeout=30)
                    print(f"Dataset {dataset_id} créé")
                
                # Vérification de l'état initial de la table
                table_ref = f"{project_id}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
                initial_row_count = 0
                try:
                    table = bq_client.get_table(table_ref)
                    initial_row_count = table.num_rows
                    print(f"Table existante avec {initial_row_count} lignes")
                except:
                    print("Table n'existe pas encore, elle sera créée")

                transformed_df = transformed_df.dropDuplicates(["forecast_time"])
  
                # Chargement des données avec plus d'options de debug
                print("Début du chargement des données...")
                transformed_df.write \
                    .format("bigquery") \
                    .option("table", table_ref) \
                    .option("temporaryGcsBucket", BIGQUERY_TEMP_BUCKET) \
                    .option("writeMethod", "direct") \
                    .option("createDisposition", "CREATE_IF_NEEDED") \
                    .option("writeDisposition", "WRITE_APPEND") \
                    .mode("append") \
                    .save()
                    
                print("Chargement terminé. Vérification des données...")
                
                # Attendre un peu pour que BigQuery soit à jour
                import time
                time.sleep(5)
                
                # Vérification des données après insertion
                verify_bigquery_data(project_id, BIGQUERY_DATASET, BIGQUERY_TABLE)
                
                print("Données chargées avec succès dans BigQuery.")
                
            except Exception as bq_error:
                print(f"Erreur lors du chargement dans BigQuery: {bq_error}")
                print("Données transformées uniquement (pas de chargement BigQuery).")
                import traceback
                traceback.print_exc()
        else:
            print("Project ID non disponible, données transformées uniquement.")

    except Exception as e:
        print(f"Une erreur est survenue pendant le traitement Spark : {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'spark' in locals() and spark:
            spark.stop()
        print("Session Spark arrêtée.")
