from datetime import datetime

from src.storage.s3.partitioning import get_partition_prefix, get_s3_key

_DT = datetime(2026, 5, 9, 8, 0, 0)


# ── get_partition_prefix ──────────────────────────────────────────────────────

def test_partition_prefix_all_cols():
    result = get_partition_prefix(_DT, ["year", "month", "day"])
    assert result == "year=2026/month=05/day=09/"


def test_partition_prefix_year_only():
    result = get_partition_prefix(_DT, ["year"])
    assert result == "year=2026/"


def test_partition_prefix_year_month():
    result = get_partition_prefix(_DT, ["year", "month"])
    assert result == "year=2026/month=05/"


def test_partition_prefix_empty_cols():
    result = get_partition_prefix(_DT, [])
    assert result == "/"


# ── get_s3_key ────────────────────────────────────────────────────────────────

def test_get_s3_key_full_path():
    key = get_s3_key("bronze", "facebook", "data.json", _DT, ["year", "month", "day"])
    assert key == "bronze/facebook/year=2026/month=05/day=09/data.json"


def test_get_s3_key_no_partition():
    key = get_s3_key("processed", "twitter", "file.parquet", _DT, [])
    assert key == "processed/twitter/file.parquet"
