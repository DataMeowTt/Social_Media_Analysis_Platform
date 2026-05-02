from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, FloatType, StructType, StructField
import pandas as pd

_schema = StructType([
    StructField("sentiment_label", StringType(), True),
    StructField("sentiment_score", FloatType(), True)
])

@pandas_udf(_schema)
def predict_sentiment_udf(texts: pd.Series) -> pd.DataFrame:
    from src.ml.inference.predictor import SentimentPredictor
    
    predictor = SentimentPredictor()
    results = [predictor.predict(t) for t in texts]
    
    return pd.DataFrame({
        "sentiment_label": [r["label"] for r in results],
        "sentiment_score": [r["score"] for r in results]
    })
    
def add_sentiment(df: DataFrame) -> DataFrame:
    tmp = predict_sentiment_udf(df["text"])
    
    return (
        df
        .withColumn("sentiment_label", tmp["sentiment_label"])
        .withColumn("sentiment_score", tmp["sentiment_score"])
    )