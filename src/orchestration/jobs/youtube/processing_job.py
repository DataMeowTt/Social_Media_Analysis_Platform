import os

from src.youtube.processing.processing_job import processing
from src.utils.session import create_spark_session

YOUTUBE_ID = os.getenv("YOUTUBE_VIDEO_ID", "nHkKJ87FS6s")

if __name__ == "__main__":
    spark = create_spark_session("SocialMedia-YouTube-Processing")
    try:
        processing(spark, youtube_id=YOUTUBE_ID)
    finally:
        spark.stop()
