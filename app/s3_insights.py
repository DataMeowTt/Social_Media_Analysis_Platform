from app.s3_reader import read_parquet_from_s3

_FRONTEND_COLS = {
    "thread_id", "video_id", "entity", "aspect", "sentiment",
    "intensity", "mention_count", "evidence", "brand_comment_count", "analyzed_at",
}


def read_thread_insights() -> list[dict]:
    table = read_parquet_from_s3("analytics/youtube/thread_insights")
    if table is None:
        return []

    df = table.to_pandas()
    df = df[[c for c in df.columns if c in _FRONTEND_COLS]]

    ts_cols = df.select_dtypes(include=["datetime64[ns, UTC]", "datetime64[ns]"]).columns
    for col in ts_cols:
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    return df.to_dict(orient="records")
