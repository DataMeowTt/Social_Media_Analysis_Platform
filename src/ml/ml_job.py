from pyspark.sql import DataFrame
from src.ml.inference.batch_inference import add_sentiment

def run_ml_pipeline(df: DataFrame) -> DataFrame:
    # Adding sentiment analysis results to the DataFrame
    return add_sentiment(df)