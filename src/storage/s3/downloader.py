import boto3
import json
from src.utils.config_loader import load_config

def download_from_s3(s3_key: str) -> list[dict]:
    config = load_config()
    bucket = config["s3"]["bucket_name"]
    region = config["aws"]["region"]

    s3 = boto3.client("s3", region_name=region)
    response = s3.get_object(Bucket=bucket, Key=s3_key)
    content = response["Body"].read().decode("utf-8")
    
    return json.loads(content)