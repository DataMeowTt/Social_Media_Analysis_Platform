from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def add_engagement_score(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "engagement_score",
        F.col("retweetCount") * 2 + F.col("likeCount") + F.col("replyCount") * 1.5 + F.col("quoteCount") * 1.5
    )
    
def add_is_viral(df: DataFrame, threshold: int = 1000) -> DataFrame:
    return df.withColumn(
        "is_viral",
        F.col("engagement_score") >= threshold
    )
    
def add_created_at_ts(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "created_at_ts",
        F.to_timestamp(F.col("createdAt"))
        )
    
def add_temporal_features(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("tweet_hour", F.hour(F.col("created_at_ts")))
        .withColumn("tweet_day_of_week", F.dayofweek(F.col("created_at_ts")))
        .withColumn("year", F.year(F.col("created_at_ts")))
        .withColumn("month", F.month(F.col("created_at_ts")))
        .withColumn("day", F.dayofmonth(F.col("created_at_ts")))
    )
    
def add_tweet_type_flags(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("is_reply", F.col("inReplyToId").isNotNull())
        .withColumn("is_retweet", F.col("type") == "retweet")
    )
    
def add_entity_count(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("hashtag_count", F.size(F.coalesce(F.col("hashtags"), F.array())))
        .withColumn("mention_count", F.size(F.coalesce(F.col("mentioned_user_ids"), F.array())))
    )
    
def add_author_influence(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "author_influence_ratio",
        F.when(F.col("author_following") > 0,
               F.col("author_followers") / F.col("author_following"))
         .otherwise(None)
    )
    
def enrich_tweets(df: DataFrame) -> DataFrame:
    df = add_engagement_score(df)
    df = add_is_viral(df)
    df = add_created_at_ts(df)
    df = add_temporal_features(df)
    df = add_tweet_type_flags(df)
    df = add_entity_count(df)
    df = add_author_influence(df)
    
    return df