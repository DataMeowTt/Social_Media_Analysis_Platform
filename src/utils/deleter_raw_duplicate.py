import gzip
import json
import boto3

BUCKET = "social-analysis-prod-bucket"
PREFIX = "raw/tweets/year=2026/month=05/day=11/"
REGION = "ap-southeast-1"


def s3():
    return boto3.client("s3", region_name=REGION)


def list_files() -> list[str]:
    paginator = s3().get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json.gz"):
                keys.append(obj["Key"])
    return keys


def load_tweets(key: str) -> list[dict]:
    body = s3().get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return json.loads(gzip.decompress(body))


def write_tweets(key: str, tweets: list[dict]) -> None:
    body = gzip.compress(json.dumps(tweets, ensure_ascii=False).encode("utf-8"))
    s3().put_object(
        Bucket=BUCKET, Key=key, Body=body,
        ContentType="application/json", ContentEncoding="gzip",
    )


def main() -> None:
    keys = list_files()
    if not keys:
        print(f"No .json.gz files found under {PREFIX}")
        return

    # Load all tweets across all files in the prefix
    all_tweets: list[tuple[str, dict]] = []   # (file_key, tweet)
    for key in keys:
        for tweet in load_tweets(key):
            all_tweets.append((key, tweet))

    total = len(all_tweets)

    # Deduplicate: keep first occurrence of each ID
    seen_ids: set[str] = set()
    unique_by_file: dict[str, list[dict]] = {k: [] for k in keys}
    duplicates = 0

    for key, tweet in all_tweets:
        tid = tweet.get("id")
        if tid and tid in seen_ids:
            duplicates += 1
        else:
            if tid:
                seen_ids.add(tid)
            unique_by_file[key].append(tweet)

    remaining = total - duplicates

    # Rewrite each file with only unique tweets
    for key, tweets in unique_by_file.items():
        write_tweets(key, tweets)

    print(f"Tổng tweets ban đầu : {total}")
    print(f"Tweets duplicate    : {duplicates}")
    print(f"Tweets còn lại      : {remaining}")


if __name__ == "__main__":
    main()
