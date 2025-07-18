
# le fournisseur de services cloud.

provider "google" {
  
  project = "tr-weather-pipeline-2025"
  region  = "europe-west9"
}


resource "google_storage_bucket" "data_lake" {
  name     = "tr-weather-pipeline-datalake-2025"
  location = "EU"
  storage_class = "STANDARD"
}
resource "google_bigquery_dataset" "data_warehouse" {
  dataset_id = "weather_data"
  description = "Dataset pour stocker les données météo transformées pour Casablanca."
  location    = "EU"
}

# bucket temporaire pour écrire les données
# avant de les charger dans BigQuery.
resource "google_storage_bucket" "bq_temp_bucket" {
  name     = "tr-weather-pipeline-bq-temp-2025" 
  location = "EU"
  storage_class = "STANDARD"

  # Ajoute une règle pour que les fichiers de plus d'un jour soient supprimés
  # automatiquement, pour ne pas accumuler de coûts.
  lifecycle_rule {
    condition {
      age = 1 # en jours
    }
    action {
      type = "Delete"
    }
  }
}