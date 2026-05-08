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
