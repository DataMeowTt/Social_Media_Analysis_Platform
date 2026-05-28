from src.facebook.analytics.analytics_job import incremental_gold_processing
from src.utils.logger import get_logger
from src.utils.session import create_spark_session

logger = get_logger(__name__)

if __name__ == "__main__":
    spark = create_spark_session("Facebook-Analytics")
    try:
        incremental_gold_processing(spark)
    finally:
        spark.stop()
