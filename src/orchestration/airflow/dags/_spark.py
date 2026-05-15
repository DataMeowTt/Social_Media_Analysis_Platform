import os

COMPOSE = "/opt/workspace/infra/docker/docker-compose.yaml"

_SPARK_ARGS = (
    "--master spark://spark-master:7077 "
    "--total-executor-cores 6 "
    "--executor-cores 2 "
    "--executor-memory 4G "
    "--packages org.apache.hadoop:hadoop-aws:3.3.4 "
    "--conf spark.pyspark.python=/opt/venv/bin/python3 "
    "--conf spark.executorEnv.PYTHONPATH=/opt/workspace "
    f"--conf spark.executorEnv.ENV={os.environ.get('ENV', 'prod')} "
    "--conf spark.executorEnv.PYSPARK_PYTHON=/opt/venv/bin/python3 "
    "--conf spark.executorEnv.TRANSFORMERS_OFFLINE=1 "
    "--conf spark.executorEnv.HF_HUB_OFFLINE=1 "
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
    "--conf spark.hadoop.fs.s3a.aws.credentials.provider="
    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
)


def spark_cmd(script: str) -> str:
    inner = f"/opt/spark/bin/spark-submit {_SPARK_ARGS} {script}"
    return (
        f"docker compose -f {COMPOSE} run --rm "
        f'-e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" '
        f'-e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" '
        f'spark-submit bash -c "{inner}"'
    )
