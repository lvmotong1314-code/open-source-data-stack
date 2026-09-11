from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="project1_taxi_pipeline",
    description="Chicago Taxi Project 1 pipeline",
    start_date=datetime(2026, 9, 1),
    schedule_interval=None,
    catchup=False,
    tags=["project1", "chicago-taxi"],
) as dag:

    check_environment = BashOperator(
        task_id="check_environment",
        bash_command=(
            "python --version && "
            "dbt --version && "
            "test -f /opt/db/scripts/ingest_taxi_trips.py && "
            "echo 'Project 1 Airflow environment OK'"
        ),
    )

    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command="python /opt/db/scripts/ingest_taxi_trips.py",
    )


    dbt_transform = BashOperator(
    task_id="dbt_transform",
    bash_command=(
        "cd /opt/dbt/chicago_taxi && "
        "dbt run --profiles-dir /opt/dbt/chicago_taxi"
    ),
)

    check_environment >> ingest_raw >> dbt_transform