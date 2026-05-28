from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

from src.orchestration.airflow.dags._spark import spark_cmd as _spark_cmd, local_spark_cmd as _local_spark_cmd

_default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="facebook_pipeline",
    default_args=_default_args,
    description="Facebook Pipeline: Ingest posts/comments, process to silver, run ML analytics and Gemini analysis",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["facebook", "vinfast"],
) as dag:

    ingestion = BashOperator(
        task_id="ingestion_posts",
        bash_command=(
            "docker compose -f /opt/workspace/infra/docker/docker-compose.yaml run --rm "
            '-e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" '
            '-e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" '
            '-e FB_COOKIES="$FB_COOKIES" '
            '-e FB_DTSG="$FB_DTSG" '
            '-e PROXY="$PROXY" '
            '-e STATIC_PROXY="$STATIC_PROXY" '
            '-e FB_INGESTION_MODE="$FB_INGESTION_MODE" '
            '-e FB_GROUP_URL="$FB_GROUP_URL" '
            '-e FB_POST_URLS="$FB_POST_URLS" '
            '-e FB_PAGE_NAME="$FB_PAGE_NAME" '
            '-e FB_LIMIT="$FB_LIMIT" '
            '-e FB_MIN_COMMENTS="$FB_MIN_COMMENTS" '
            "spark-submit python /opt/workspace/src/orchestration/jobs/facebook/ingestion_job.py"
        ),
        execution_timeout=timedelta(hours=3),
    )

    processing = BashOperator(
        task_id="processing_silver",
        bash_command=_spark_cmd(
            "/opt/workspace/src/orchestration/jobs/facebook/processing_job.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    analytics = BashOperator(
        task_id="analytics_gold",
        bash_command=_spark_cmd(
            "/opt/workspace/src/orchestration/jobs/facebook/analytics_job.py"
        ),
        execution_timeout=timedelta(hours=3),
    )

    gemini_analysis = BashOperator(
        task_id="gemini_analysis",
        bash_command=_local_spark_cmd(
            "/opt/workspace/src/orchestration/jobs/facebook/gemini_analysis_job.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    stance_analysis = BashOperator(
        task_id="stance_analysis",
        bash_command=_spark_cmd(
            "/opt/workspace/src/orchestration/jobs/facebook/stance_job.py"
        ),
        execution_timeout=timedelta(hours=3),
    )

    ingestion >> processing >> analytics >> [gemini_analysis, stance_analysis]

