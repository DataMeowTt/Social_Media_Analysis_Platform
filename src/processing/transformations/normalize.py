from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _select_tweet_fields(prefix: str, ingestion_date_col: str = "ingestion_date") -> list:
    p = f"{prefix}." if prefix else ""
    return [
        F.col(f"{p}id").alias("id"),
        F.col(f"{p}type").alias("type"),
        F.col(f"{p}url").alias("url"),
        F.col(f"{p}text").alias("text"),
        F.col(f"{p}source").alias("source"),
        F.col(f"{p}lang").alias("lang"),
        F.col(f"{p}retweetCount").alias("retweetCount"),
        F.col(f"{p}replyCount").alias("replyCount"),
        F.col(f"{p}likeCount").alias("likeCount"),
        F.col(f"{p}quoteCount").alias("quoteCount"),
        F.col(f"{p}createdAt").alias("createdAt"),
        F.col(f"{p}conversationId").alias("conversationId"),
        F.col(f"{p}inReplyToId").alias("inReplyToId"),
        F.col(f"{p}inReplyToUserId").alias("inReplyToUserId"),
        F.col(f"{p}inReplyToUsername").alias("inReplyToUsername"),
        F.col(ingestion_date_col).alias("ingestion_date"),
        F.col(f"{p}author.id").alias("author_id"),
        F.col(f"{p}author.userName").alias("author_username"),
        F.col(f"{p}author.isBlueVerified").alias("author_is_blue_verified"),
        F.col(f"{p}author.followers").alias("author_followers"),
        F.col(f"{p}author.following").alias("author_following"),
        F.col(f"{p}author.statusesCount").alias("author_statuses_count"),
        F.col(f"{p}author.createdAt").alias("author_createdAt"),
        F.col(f"{p}entities.hashtags.text").alias("hashtags"),
        F.col(f"{p}entities.user_mentions.id_str").alias("mentioned_user_ids"),
    ]


def flatten_tweets(df: DataFrame) -> DataFrame:
    main_tweets = df.select(_select_tweet_fields(prefix=""))

    quoted_tweets = (
        df.filter(F.col("quoted_tweet").isNotNull())
        .select(_select_tweet_fields(prefix="quoted_tweet", ingestion_date_col="ingestion_date"))
    )

    return main_tweets.unionByName(quoted_tweets).dropDuplicates(["id"])
