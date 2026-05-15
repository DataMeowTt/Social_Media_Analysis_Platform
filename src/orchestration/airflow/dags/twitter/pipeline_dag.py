from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

from src.orchestration.jobs.twitter.ingestion_job import run_ingestion_task
from src.orchestration.airflow.dags._spark import spark_cmd as _spark_cmd


_default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="social_media_pipeline",
    default_args=_default_args,
    description="Social Media Analysis Pipeline: Ingest tweets, process data, and run analytics",
    # TODO: change to daily schedule after testing
    # schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["social-media"],
) as dag:

    ingestion = PythonOperator(
        task_id="ingestion_tweets",
        python_callable=run_ingestion_task,
    )

    processing = BashOperator(
        task_id="processing_silver",
        bash_command=_spark_cmd(
            "/opt/workspace/src/orchestration/jobs/twitter/processing_job.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    analytics = BashOperator(
        task_id="analytics_gold",
        bash_command=_spark_cmd(
            "/opt/workspace/src/orchestration/jobs/twitter/analytic_job.py"
        ),
        execution_timeout=timedelta(hours=3),
    )

    ingestion >> processing >> analytics