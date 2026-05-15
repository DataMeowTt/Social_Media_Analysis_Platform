from src.utils.session import create_spark_session
from src.twitter.analytics.analytics_job import gold_processing
from src.twitter.processing.processing_job import historical_processing
from src.utils.logger import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    spark = create_spark_session("SocialMedia-Analytics")
    try:
        gold_processing(spark)
    finally:
        spark.stop()
