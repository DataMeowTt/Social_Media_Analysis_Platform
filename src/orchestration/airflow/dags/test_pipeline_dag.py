from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

from src.orchestration.jobs.ingestion_job import run_ingestion_task
from src.utils.deleter_duplicate import delete_raw_duplicates, delete_processed_duplicates
from src.orchestration.airflow.dags._spark import spark_cmd as _spark_cmd


_default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="social_media_pipeline_test",
    default_args=_default_args,
    description="Test pipeline — safe to run multiple times a day (deduplicates between runs)",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["social-media", "test"],
) as dag:

    ingestion = PythonOperator(
        task_id="ingestion_tweets",
        python_callable=run_ingestion_task,
    )

    delete_raw = PythonOperator(
        task_id="delete_raw_duplicates",
        python_callable=delete_raw_duplicates,
    )

    processing = BashOperator(
        task_id="processing_silver",
        bash_command=_spark_cmd(
            "/opt/workspace/src/orchestration/jobs/processing_job.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    delete_processed = PythonOperator(
        task_id="delete_processed_duplicates",
        python_callable=delete_processed_duplicates,
    )

    analytics = BashOperator(
        task_id="analytics_gold",
        bash_command=_spark_cmd(
            "/opt/workspace/src/orchestration/jobs/analytic_job.py"
        ),
        execution_timeout=timedelta(hours=3),
    )

    ingestion >> delete_raw >> processing >> delete_processed >> analytics
