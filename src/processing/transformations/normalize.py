from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def flatten_tweets(df: DataFrame) -> DataFrame:
    return df.select(
        "id", "type", "url", "text", "source", "lang",
        "retweetCount", "replyCount", "likeCount", "quoteCount",
        "createdAt", "conversationId",
        "inReplyToId", "inReplyToUserId", "inReplyToUsername",
        "ingestion_date",
        
        # Flatten Author Fields
        F.col("author.id").alias("author_id"),
        F.col("author.userName").alias("author_username"),
        F.col("author.isBlueVerified").alias("author_is_blue_verified"),
        F.col("author.followers").alias("author_followers"),
        F.col("author.following").alias("author_following"),
        F.col("author.statusesCount").alias("author_statuses_count"),
        
        # Flatten Entity Fields
        F.col("entities.hashtags").alias("hashtags"),
        F.col("entities.user_mentions.id_str").alias("mentioned_user_ids"),
    )
