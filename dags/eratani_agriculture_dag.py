from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


from include.agriculture.staging_ingestion import ingest_csv_to_staging

CSV_PATH = "/opt/airflow/data/agriculture_dataset.csv"
POSTGRES_CONN_ID = "postgres_default"

default_args = {
    "owner": "eratani",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="eratani_agriculture_etl",
    default_args=default_args,
    description="Ingest agriculture CSV into staging table",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["eratani", "etl", "staging"],
) as dag:

    ingest_staging = PythonOperator(
        task_id="ingest_csv_to_staging",
        python_callable=ingest_csv_to_staging,
        op_kwargs={
            "csv_path": CSV_PATH,
            "postgres_conn_id": POSTGRES_CONN_ID,
        },
    )
    run_dbt = BashOperator(
    task_id="run_dbt_models",
    bash_command="""
    cd /opt/dbt_projects &&
    dbt run
    """,
)
ingest_staging >> run_dbt

