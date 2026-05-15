import os

import boto3
from functools import lru_cache

from src.utils.config_loader import load_config


@lru_cache(maxsize=None)
def _region() -> str:
    return load_config()["aws"]["region"]


@lru_cache(maxsize=None)
def get_s3_client():
    return boto3.client("s3", region_name=_region())


@lru_cache(maxsize=None)
def get_glue_client():
    return boto3.client("glue", region_name=_region())


def create_spark_session(app_name: str):
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )
        .config("spark.pyspark.python", "/opt/venv/bin/python3")
        .config("spark.executorEnv.PYSPARK_PYTHON", "/opt/venv/bin/python3")
        .config("spark.executorEnv.PYTHONPATH", "/opt/workspace")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
        .config("spark.executorEnv.ENV", os.environ.get("ENV", "prod"))
        .config("spark.executorEnv.AWS_ACCESS_KEY_ID", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.executorEnv.AWS_SECRET_ACCESS_KEY", os.environ["AWS_SECRET_ACCESS_KEY"])
        .getOrCreate()
    )
