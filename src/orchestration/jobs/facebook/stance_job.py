from src.facebook.analytics.stance_job import run_stance_analysis
from src.utils.logger import get_logger
from src.utils.session import create_spark_session

logger = get_logger(__name__)

if __name__ == "__main__":
    spark = create_spark_session("Facebook-Stance-Analysis")
    try:
        run_stance_analysis(spark)
    finally:
        spark.stop()
