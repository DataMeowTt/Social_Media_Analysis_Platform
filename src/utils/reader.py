import gzip
import io
import json
import re
import boto3
import pyarrow.parquet as pq

from src.utils.config_loader import load_config

_config = load_config()
BUCKET = _config["s3"]["bucket_name"]
REGION = _config["aws"]["region"]

# ── Helpers ──────────────────────────────────────────────────────────────────

def s3_client():
    return boto3.client("s3", region_name=REGION)

def list_objects(prefix):
    s3 = s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        yield from page.get("Contents", [])

# ── Step 1: collect raw tweets ────────────────────────────────────────────────

def load_raw_tweets() -> list[dict]:
    s3 = s3_client()
    tweets = []
    for obj in list_objects("raw/tweets/"):
        if not obj["Key"].endswith(".json.gz"):
            continue
        body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
        tweets.extend(json.loads(gzip.decompress(body)))
    print(f"Raw total       : {len(tweets)}")
    return tweets

# ── Step 2: simulate pipeline filters ────────────────────────────────────────

MONTH_MAP = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}

TIMESTAMP_RE = re.compile(r'^\w+\s+(\w+)\s+(\d+)\s+(\d+:\d+:\d+)\s+\S+\s+(\d+)$')

def parse_twitter_ts(s: str | None) -> bool:
    """Returns True if our regex+format would produce a non-null timestamp."""
    if not s:
        return False
    m = TIMESTAMP_RE.match(s)
    if not m:
        return False
    month_name = m.group(1)
    return month_name in MONTH_MAP

def simulate_pipeline(tweets: list[dict]) -> None:
    total = len(tweets)

    # Deduplicate (flatten_tweets)
    seen = {}
    for t in tweets:
        tid = t.get("id")
        if tid and tid not in seen:
            seen[tid] = t
    after_dedup = list(seen.values())
    print(f"After dedup     : {len(after_dedup)}  (lost {total - len(after_dedup)})")

    # drop_missing_critical_fields (clean_tweets)
    after_critical = []
    dropped_critical = []
    for t in after_dedup:
        author = t.get("author") or {}
        missing = (
            not t.get("id") or
            not t.get("text") or
            not t.get("createdAt") or
            not author.get("id") or
            not author.get("createdAt")
        )
        if missing:
            dropped_critical.append(t)
        else:
            after_critical.append(t)
    print(f"After critical  : {len(after_critical)}  (lost {len(dropped_critical)} — missing critical fields)")

    # drop_negative_engagement (clean_tweets)
    after_engagement = []
    dropped_engagement = []
    for t in after_critical:
        negative = (
            (t.get("retweetCount") or 0) < 0 or
            (t.get("likeCount")    or 0) < 0 or
            (t.get("replyCount")   or 0) < 0 or
            (t.get("quoteCount")   or 0) < 0
        )
        if negative:
            dropped_engagement.append(t)
        else:
            after_engagement.append(t)
    print(f"After engagement: {len(after_engagement)}  (lost {len(dropped_engagement)} — negative engagement)")

    # drop_unparseable_timestamps (enrich_tweets)
    after_ts = []
    dropped_ts = []
    for t in after_engagement:
        if not parse_twitter_ts(t.get("createdAt")):
            dropped_ts.append(t)
        else:
            after_ts.append(t)
    print(f"After timestamp : {len(after_ts)}  (lost {len(dropped_ts)} — unparseable createdAt)")

    # Sample unparseable timestamps
    if dropped_ts:
        print("\nSample unparseable createdAt values:")
        for t in dropped_ts[:5]:
            print(f"  id={t.get('id')}  createdAt={t.get('createdAt')!r}")

    # Sample dropped critical fields
    if dropped_critical:
        print("\nSample missing critical fields:")
        for t in dropped_critical[:5]:
            author = t.get("author") or {}
            print(f"  id={t.get('id')}  createdAt={t.get('createdAt')!r}  author.id={author.get('id')}  author.createdAt={author.get('createdAt')!r}")

# ── Step 3: compare with processed ───────────────────────────────────────────

def load_processed_ids() -> set:
    s3 = s3_client()
    ids = set()
    for obj in list_objects("processed/tweets/"):
        if not obj["Key"].endswith(".parquet"):
            continue
        body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
        table = pq.read_table(io.BytesIO(body), columns=["id"])
        ids.update(table.column("id").to_pylist())
    return ids


def check_processed_duplicates() -> None:
    from collections import Counter
    s3 = s3_client()
    ids = []
    for obj in list_objects("processed/tweets/"):
        if not obj["Key"].endswith(".parquet"):
            continue
        body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
        table = pq.read_table(io.BytesIO(body), columns=["id"])
        ids.extend(table.column("id").to_pylist())

    total = len(ids)
    unique = len(set(ids))
    print(f"Processed total : {total}")
    print(f"Processed unique: {unique}  (lost {total - unique} duplicates)")

    if total != unique:
        counts = Counter(ids)
        dups = [(id_, cnt) for id_, cnt in counts.items() if cnt > 1]
        print(f"\nSample duplicate IDs in processed:")
        for id_, cnt in dups[:5]:
            print(f"  id={id_}  count={cnt}")


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Pipeline simulation ===")
    raw_tweets = load_raw_tweets()
    simulate_pipeline(raw_tweets)

    print("\n=== Processed layer ===")
    check_processed_duplicates()
