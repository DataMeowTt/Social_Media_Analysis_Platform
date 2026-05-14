from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from src.orchestration.jobs.ingestion_job import run_ingestion_task

_COMPOSE = "/opt/workspace/infra/docker/docker-compose.yaml"

_SPARK_ARGS = (
    "--master spark://spark-master:7077 "
    "--total-executor-cores 6 "
    "--executor-cores 2 "
    "--executor-memory 4G "
    "--packages org.apache.hadoop:hadoop-aws:3.3.4 "
    "--conf spark.pyspark.python=/opt/venv/bin/python3 "
    "--conf spark.executorEnv.PYTHONPATH=/opt/workspace "
    "--conf spark.executorEnv.ENV=prod "
    "--conf spark.executorEnv.PYSPARK_PYTHON=/opt/venv/bin/python3 "
    "--conf spark.executorEnv.TRANSFORMERS_OFFLINE=1 "
    "--conf spark.executorEnv.HF_HUB_OFFLINE=1 "
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
    "--conf spark.hadoop.fs.s3a.aws.credentials.provider="
    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
)


def _spark_cmd(script: str) -> str:
    inner = f"/opt/spark/bin/spark-submit {_SPARK_ARGS} {script}"
    return (
        f"docker compose -f {_COMPOSE} run --rm "
        f'-e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" '
        f'-e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" '
        f'spark-submit bash -c "{inner}"'
    )


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
            "/opt/workspace/src/orchestration/jobs/processing_job.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    analytics = BashOperator(
        task_id="analytics_gold",
        bash_command=_spark_cmd(
            "/opt/workspace/src/orchestration/jobs/analytic_job.py"
        ),
        execution_timeout=timedelta(hours=3),
    )

    ingestion >> processing >> analytics