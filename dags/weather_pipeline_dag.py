from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
#from docker.types import Mount

# On définit le chemin absolu une seule fois.
# C'est VOTRE solution, elle est parfaite pour votre environnement Windows.
#HOST_GCP_PATH = 'D:/Academique/Projet/pipeline ETL/gcp'

# On crée l'objet Mount une seule fois pour le réutiliser.
#gcp_credentials_mount = Mount(
#    source=HOST_GCP_PATH,
#    target='/gcp',
#    type='bind',
#    read_only=True
#)

# On définit les variables d'environnement une seule fois pour les réutiliser.
#common_env_vars = {
    # Ces macros lisent les Variables Airflow que vous créerez dans l'UI.
#    "OPENWEATHER_API_KEY": "{{ var.value.openweather_api_key }}",
#    "GOOGLE_CLOUD_PROJECT": "{{ var.value.google_cloud_project }}",
    # Le chemin des credentials A L'INTERIEUR du conteneur de la tâche.
#    "GOOGLE_APPLICATION_CREDENTIALS": "/gcp/credentials.json",
#}

with DAG(
    dag_id="weather_elt_pipeline_docker_cloud",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["weather", "elt", "docker", "cloud"],
) as dag:

    env_vars = {
            "OPENWEATHER_API_KEY": "{{ var.value.openweather_api_key }}",
            "GOOGLE_CLOUD_PROJECT": "{{ var.value.google_cloud_project }}",
  }
    
    task_extract = DockerOperator(
        task_id="extract_from_api_to_gcs",
        image="weather-pipeline/extractor",
       # pull_policy="NEVER",
        command="python extract.py",
        auto_remove=True,
        environment=env_vars,
        #mounts=[gcp_credentials_mount],
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",

    )

    task_transform = DockerOperator(
        task_id="transform_in_spark_and_load_to_bq",
        image="weather-pipeline/transformer",
       # pull_policy="NEVER",
       # mount_tmp_dir=False,
       # command="spark-submit transform.py",
        auto_remove=True,
        environment=env_vars,
        #mounts=[gcp_credentials_mount],
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    task_extract >> task_transform
