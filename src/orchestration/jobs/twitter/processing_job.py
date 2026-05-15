from src.twitter.processing.processing_job import incremental_processing
from src.utils.logger import get_logger
from src.utils.session import create_spark_session

logger = get_logger(__name__)

if __name__ == "__main__":
    spark = create_spark_session("SocialMedia-Processing")
    try:
        incremental_processing(spark)
    finally:
        spark.stop()
