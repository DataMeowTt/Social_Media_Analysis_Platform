import pandas as pd

from app.s3_reader import read_parquet_from_s3


def controversial_posts() -> list[dict]:
    posts_table = read_parquet_from_s3("analytics/facebook/dim_posts")
    stance_table = read_parquet_from_s3("analytics/facebook/fact_comment_stance")

    if posts_table is None or stance_table is None:
        return []

    posts = posts_table.to_pandas()
    stance = stance_table.to_pandas()

    agg = (
        stance.groupby("post_id")
        .agg(
            agree_count=("stance", lambda x: (x == "agree").sum()),
            disagree_count=("stance", lambda x: (x == "disagree").sum()),
            total_comments=("comment_id", "count"),
        )
        .reset_index()
    )

    post_cols = ["post_id", "text_preview", "sentiment_label", "page_name"]
    if "permalink" in posts.columns:
        post_cols.append("permalink")

    merged = agg.merge(posts[post_cols], on="post_id", how="inner")

    result = (
        merged.sort_values("total_comments", ascending=False)
        .head(2)
        .drop(columns=["post_id"])
    )
    return result.where(pd.notnull(result), None).to_dict(orient="records")
