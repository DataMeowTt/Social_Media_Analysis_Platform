from src.utils.session import create_spark_session
from src.youtube.analytics.transformations.sentiment import run_sentiment
from src.youtube.analytics.transformations.stance import run_stance


def run_sentiment_analysis(youtube_id: str) -> None:
    spark = create_spark_session("YouTube-Analytics")
    try:
        run_sentiment(spark, youtube_id)
    finally:
        spark.stop()


def run_stance_analysis(youtube_id: str) -> None:
    spark = create_spark_session("YouTube-Analytics")
    try:
        run_stance(spark, youtube_id)
    finally:
        spark.stop()


def analysis_local(youtube_id: str) -> None:
    run_sentiment_analysis(youtube_id)
    run_stance_analysis(youtube_id)
