from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType

AUTHOR_SCHEMA = StructType([
    StructField("userName",         StringType(), True),
    StructField("id",               StringType(), True),   
    StructField("isBlueVerified",   BooleanType(), True), 
    StructField("followers",        IntegerType(), True),
    StructField("following",        IntegerType(), True),
    StructField("statusesCount",    IntegerType(), True)
])

ENTITY_SCHEMA = StructType([
    StructField("user_mentions", ArrayType(
        StructType([
            StructField("id_str", StringType(), True)
        ])
    ), True),
    
    StructField("hashtags", ArrayType(StringType()), True)
])

TWEET_SCHEMA = StructType([
    StructField("type",              StringType(), True),
    StructField("id",                StringType(), True),
    StructField("url",               StringType(), True),
    StructField("text",              StringType(), True),
    StructField("source",            StringType(), True),
    StructField("retweetCount",      IntegerType(), True),
    StructField("replyCount",        IntegerType(), True),
    StructField("likeCount",         IntegerType(), True),
    StructField("quoteCount",        IntegerType(), True),
    StructField("createdAt",         StringType(), True),
    StructField("lang",              StringType(), True),
    StructField("inReplyToId",       StringType(), True),
    StructField("conversationId",    StringType(), True),
    StructField("inReplyToUserId",   StringType(), True),
    StructField("inReplyToUsername", StringType(), True),
    StructField("author",            AUTHOR_SCHEMA, True),
    StructField("entities",          ENTITY_SCHEMA, True),
    StructField("ingestion_date",    StringType(), True)
])