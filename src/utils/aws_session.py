import boto3
from functools import lru_cache
from src.utils.config_loader import load_config

@lru_cache(maxsize=None)
def get_s3_client():
    config = load_config()
    
    # Local dev: boto3 đọc từ env vars AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
    # TODO: Databricks/ECS: boto3 tự dùng IAM Role attached, không cần credentials
    return boto3.client("s3", region_name=config["aws"]["region"])
