import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    ArrayType, IntegerType, StringType, StructField, StructType,
)

from src.facebook.processing.transformations.normalize import flatten_comments, flatten_posts

_REPLY_SCHEMA = StructType([
    StructField("author_id",      StringType(), True),
    StructField("author_name",    StringType(), True),
    StructField("author_url",     StringType(), True),
    StructField("text",           StringType(), True),
    StructField("reaction_count", StringType(), True),
])

_COMMENT_SCHEMA = StructType([
    StructField("author_id",         StringType(),             True),
    StructField("author_name",       StringType(),             True),
    StructField("author_url",        StringType(),             True),
    StructField("text",              StringType(),             True),
    StructField("reaction_count",    StringType(),             True),
    StructField("parent_comment_id", StringType(),             True),
    StructField("replies",           ArrayType(_REPLY_SCHEMA), True),
])

RAW_SCHEMA = StructType([
    StructField("post_id",        StringType(),               True),
    StructField("source_type",    StringType(),               True),
    StructField("page_name",      StringType(),               True),
    StructField("text",           StringType(),               True),
    StructField("feedback_id",    StringType(),               True),
    StructField("comment_count",  IntegerType(),              True),
    StructField("permalink",      StringType(),               True),
    StructField("ingestion_date", StringType(),               True),
    StructField("comments",       ArrayType(_COMMENT_SCHEMA), True),
])


def _post(post_id="p1", text="normal post", page_name="VinFast", comments=None, ingestion_date="2026-05-01T00:00:00Z"):
    return Row(
        post_id=post_id,
        source_type="group",
        page_name=page_name,
        text=text,
        feedback_id=None,
        comment_count=0,
        permalink=None,
        ingestion_date=ingestion_date,
        comments=comments or [],
    )


def _comment(author_id="u1", author_name="User", text="hello", reaction_count="0", replies=None):
    return Row(
        author_id=author_id,
        author_name=author_name,
        author_url=None,
        text=text,
        reaction_count=reaction_count,
        parent_comment_id=None,
        replies=replies or [],
    )


def _reply(author_id="u2", author_name="Replier", text="reply text"):
    return Row(author_id=author_id, author_name=author_name, author_url=None, text=text, reaction_count="1")


def _df(spark, rows):
    return spark.createDataFrame(rows, RAW_SCHEMA)


# ── flatten_posts ─────────────────────────────────────────────────────────────

def test_flatten_posts_keeps_normal_post(spark):
    df = _df(spark, [_post(text="xe VinFast chạy ngon")])
    assert flatten_posts(df).count() == 1


def test_flatten_posts_filters_sale_thanh_ly(spark):
    df = _df(spark, [_post(text="thanh lý xe VinFast giá tốt")])
    assert flatten_posts(df).count() == 0


def test_flatten_posts_filters_sale_can_ban(spark):
    df = _df(spark, [_post(text="cần bán xe VinFast VF8")])
    assert flatten_posts(df).count() == 0


def test_flatten_posts_filters_sale_sang_nhuong(spark):
    df = _df(spark, [_post(text="sang nhượng xe VF9 giá rẻ")])
    assert flatten_posts(df).count() == 0


def test_flatten_posts_dedup_by_post_id(spark):
    df = _df(spark, [_post(post_id="p1"), _post(post_id="p1"), _post(post_id="p2")])
    assert flatten_posts(df).count() == 2


def test_flatten_posts_ingestion_date_cast_to_date(spark):
    from pyspark.sql.types import DateType
    df = _df(spark, [_post(ingestion_date="2026-05-01T00:00:00Z")])
    result = flatten_posts(df)
    assert result.schema["ingestion_date"].dataType == DateType()


def test_flatten_posts_null_text_kept(spark):
    df = _df(spark, [_post(text=None)])
    assert flatten_posts(df).count() == 1


# ── flatten_comments ──────────────────────────────────────────────────────────

def test_flatten_comments_extracts_top_level(spark):
    row = _post(post_id="p1", comments=[_comment(text="great car")])
    df = _df(spark, [row])
    result = flatten_comments(df)
    assert result.count() == 1
    assert result.collect()[0]["is_reply"] is False


def test_flatten_comments_is_reply_false_for_root(spark):
    row = _post(comments=[_comment()])
    df = _df(spark, [row])
    result = flatten_comments(df)
    assert result.collect()[0]["parent_comment_id"] is None


def test_flatten_comments_extracts_replies(spark):
    reply = _reply(author_id="u2", text="agree!")
    comment = _comment(author_id="u1", replies=[reply])
    df = _df(spark, [_post(post_id="p1", comments=[comment])])
    result = flatten_comments(df)
    assert result.count() == 2
    replies = [r for r in result.collect() if r["is_reply"]]
    assert len(replies) == 1
    assert replies[0]["text"] == "agree!"


def test_flatten_comments_reply_has_parent_comment_id(spark):
    reply = _reply()
    comment = _comment(replies=[reply])
    df = _df(spark, [_post(comments=[comment])])
    result = flatten_comments(df)
    root = next(r for r in result.collect() if not r["is_reply"])
    child = next(r for r in result.collect() if r["is_reply"])
    assert child["parent_comment_id"] == root["comment_id"]


def test_flatten_comments_comment_id_deterministic(spark):
    c = _comment(author_id="u1", text="hello")
    df1 = _df(spark, [_post(post_id="p1", comments=[c])])
    df2 = _df(spark, [_post(post_id="p1", comments=[c])])
    id1 = flatten_comments(df1).collect()[0]["comment_id"]
    id2 = flatten_comments(df2).collect()[0]["comment_id"]
    assert id1 == id2


def test_flatten_comments_dedup_by_comment_id(spark):
    c = _comment(author_id="u1", text="hello")
    df = _df(spark, [_post(post_id="p1", comments=[c]), _post(post_id="p1", comments=[c])])
    result = flatten_comments(df)
    ids = [r["comment_id"] for r in result.collect()]
    assert len(ids) == len(set(ids))


def test_flatten_comments_no_comments_empty_result(spark):
    df = _df(spark, [_post(comments=[])])
    assert flatten_comments(df).count() == 0
