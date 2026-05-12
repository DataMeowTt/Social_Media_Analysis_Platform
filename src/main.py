from pyspark.sql import SparkSession
from src.processing.processing_job import historical_processing, incremental_processing
from src.analytics.analytics_job import gold_processing
from src.utils.logger import get_logger

logger = get_logger(__name__)

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SocialMediaAnalysis")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        .getOrCreate()
    )

if __name__ == "__main__":
    spark = create_spark_session()
    try:
        gold_processing(spark)
    finally:
        spark.stop()
