import gzip
import io
import json
from datetime import date

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.config_loader import load_config as _load_config

_config = _load_config()
BUCKET = _config["s3"]["bucket_name"]
REGION = _config["aws"]["region"]


def _s3():
    return boto3.client("s3", region_name=REGION)


# ── Raw layer (JSON.GZ) ───────────────────────────────────────────────────────

def delete_raw_duplicates() -> None:
    s3 = _s3()
    today = date.today()
    prefix = f"raw/tweets/year={today.year}/month={today.month:02d}/day={today.day:02d}/"

    keys = [
        obj["Key"]
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".json.gz")
    ]

    if not keys:
        print(f"[raw] No files found under {prefix}")
        return

    all_tweets: list[tuple[str, dict]] = []
    for key in keys:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        for tweet in json.loads(gzip.decompress(body)):
            all_tweets.append((key, tweet))

    total = len(all_tweets)
    seen: set[str] = set()
    unique_by_file: dict[str, list[dict]] = {k: [] for k in keys}
    duplicates = 0

    for key, tweet in all_tweets:
        tid = tweet.get("id")
        if tid and tid in seen:
            duplicates += 1
        else:
            if tid:
                seen.add(tid)
            unique_by_file[key].append(tweet)

    for key, tweets in unique_by_file.items():
        body = gzip.compress(json.dumps(tweets, ensure_ascii=False).encode("utf-8"))
        s3.put_object(Bucket=BUCKET, Key=key, Body=body,
                      ContentType="application/json", ContentEncoding="gzip")

    print(f"[raw] total={total}  duplicates={duplicates}  remaining={total - duplicates}")


# ── Processed layer (Parquet) ─────────────────────────────────────────────────

def delete_processed_duplicates() -> None:
    s3 = _s3()
    today = date.today()

    today_partition = f"year={today.year}/month={today.month}/day={today.day}"
    keys = [
        obj["Key"]
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET, Prefix=f"processed/tweets/{today_partition}/"
        )
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    if not keys:
        print(f"[processed] No files found for today ({today})")
        return

    tables = []
    for key in keys:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        tables.append(pq.read_table(io.BytesIO(body)))

    combined = pa.concat_tables(tables)
    total = len(combined)

    seen: set = set()
    mask = []
    for tweet_id in combined.column("id").to_pylist():
        if tweet_id in seen:
            mask.append(False)
        else:
            seen.add(tweet_id)
            mask.append(True)

    deduped = combined.filter(pa.array(mask))
    duplicates = total - len(deduped)

    for key in keys:
        s3.delete_object(Bucket=BUCKET, Key=key)

    buf = io.BytesIO()
    pq.write_table(deduped, buf, use_deprecated_int96_timestamps=True)
    buf.seek(0)
    s3.put_object(Bucket=BUCKET, Key=keys[0], Body=buf.read(),
                  ContentType="application/octet-stream")

    print(f"[processed] total={total}  duplicates={duplicates}  remaining={len(deduped)}")
