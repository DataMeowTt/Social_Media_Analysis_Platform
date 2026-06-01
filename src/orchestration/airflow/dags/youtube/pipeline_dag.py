from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

from src.orchestration.jobs.youtube.ingestion_job import run_ingestion_task
from src.orchestration.airflow.dags._spark import spark_cmd as _spark_cmd, local_spark_cmd as _local_spark_cmd

_default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="youtube_pipeline",
    default_args=_default_args,
    description="YouTube Pipeline: Ingest comments, process to silver, run analytics",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["youtube"],
) as dag:

    ingestion = PythonOperator(
        task_id="ingestion_comments",
        python_callable=run_ingestion_task,
    )

    processing = BashOperator(
        task_id="processing_silver",
        bash_command=_spark_cmd(
            "/opt/workspace/src/orchestration/jobs/youtube/processing_job.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    analytics_sentiment = BashOperator(
        task_id="analytics_sentiment",
        bash_command=_local_spark_cmd(
            "/opt/workspace/src/orchestration/jobs/youtube/sentiment_job.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    analytics_stance = BashOperator(
        task_id="analytics_stance",
        bash_command=_local_spark_cmd(
            "/opt/workspace/src/orchestration/jobs/youtube/stance_job.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    build_conversations = BashOperator(
        task_id="build_conversation_threads",
        bash_command=_local_spark_cmd(
            "/opt/workspace/src/orchestration/jobs/youtube/conversation_job.py"
        ),
        execution_timeout=timedelta(hours=1),
    )

    gemini_analysis = BashOperator(
        task_id="gemini_analysis_and_save",
        bash_command=_local_spark_cmd(
            "/opt/workspace/src/orchestration/jobs/youtube/gemini_analysis_job.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    ingestion >> processing >> analytics_sentiment >> analytics_stance >> build_conversations >> gemini_analysis

