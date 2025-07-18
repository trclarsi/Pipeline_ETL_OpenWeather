from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator


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
