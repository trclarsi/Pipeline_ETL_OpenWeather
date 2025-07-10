from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id="hello_airflow",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
) as dag:
    hello_task = BashOperator(
        task_id="hello_task",
        bash_command="echo 'Hello from Airflow!'",
    )