from datetime import date, datetime, timezone


from src.facebook.analytics.conversation.gemini_extractor import _parse_response
from src.facebook.analytics.conversation.insight_merger import merge_insights

_POST_ID   = "post_abc"
_PAGE      = "VinFast"
_DATE      = date(2026, 5, 1)
_ANALYZED  = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)

_VALID_ITEM = {
    "model_entity":  "VF8",
    "aspect":        "pin",
    "sentiment":     "negative",
    "intensity":     "strong",
    "mention_count": 3,
    "evidence":      "Pin xuống nhanh quá",
}


# ── merge_insights ────────────────────────────────────────────────────────────

def test_merge_insights_valid_row():
    rows = merge_insights([_VALID_ITEM], _POST_ID, _PAGE, _DATE, _ANALYZED)
    assert len(rows) == 1
    r = rows[0]
    assert r["post_id"]       == _POST_ID
    assert r["page_name"]     == _PAGE
    assert r["model_entity"]  == "VF8"
    assert r["aspect"]        == "pin"
    assert r["sentiment"]     == "negative"
    assert r["intensity"]     == "strong"
    assert r["mention_count"] == 3
    assert r["ingestion_date"] == _DATE
    assert r["analyzed_at"]   == _ANALYZED


def test_merge_insights_invalid_model_entity_skipped():
    item = {**_VALID_ITEM, "model_entity": "VF99"}
    rows = merge_insights([item], _POST_ID, _PAGE, _DATE, _ANALYZED)
    assert rows == []


def test_merge_insights_invalid_aspect_skipped():
    item = {**_VALID_ITEM, "aspect": "unknown_aspect"}
    rows = merge_insights([item], _POST_ID, _PAGE, _DATE, _ANALYZED)
    assert rows == []


def test_merge_insights_invalid_sentiment_defaults_to_neutral():
    item = {**_VALID_ITEM, "sentiment": "dunno"}
    rows = merge_insights([item], _POST_ID, _PAGE, _DATE, _ANALYZED)
    assert rows[0]["sentiment"] == "neutral"


def test_merge_insights_invalid_intensity_defaults_to_mild():
    item = {**_VALID_ITEM, "intensity": "extreme"}
    rows = merge_insights([item], _POST_ID, _PAGE, _DATE, _ANALYZED)
    assert rows[0]["intensity"] == "mild"


def test_merge_insights_mention_count_clamped_to_1():
    item = {**_VALID_ITEM, "mention_count": 0}
    rows = merge_insights([item], _POST_ID, _PAGE, _DATE, _ANALYZED)
    assert rows[0]["mention_count"] == 1


def test_merge_insights_evidence_truncated_to_200():
    item = {**_VALID_ITEM, "evidence": "x" * 300}
    rows = merge_insights([item], _POST_ID, _PAGE, _DATE, _ANALYZED)
    assert len(rows[0]["evidence"]) == 200


def test_merge_insights_empty_input():
    assert merge_insights([], _POST_ID, _PAGE, _DATE, _ANALYZED) == []


# ── _parse_response ───────────────────────────────────────────────────────────

def test_parse_response_valid_json():
    raw = '[{"model_entity":"VF8","aspect":"pin","sentiment":"negative","intensity":"strong","mention_count":2,"evidence":"slow charge"}]'
    result = _parse_response(raw)
    assert len(result) == 1
    assert result[0]["model_entity"] == "VF8"
    assert result[0]["aspect"] == "pin"


def test_parse_response_invalid_json_returns_empty():
    assert _parse_response("not json at all") == []


def test_parse_response_malformed_json_returns_empty():
    assert _parse_response("[{broken json}]") == []


def test_parse_response_invalid_model_entity_filtered():
    raw = '[{"model_entity":"VF99","aspect":"pin","sentiment":"negative","intensity":"mild","mention_count":1,"evidence":"x"}]'
    assert _parse_response(raw) == []


def test_parse_response_invalid_aspect_filtered():
    raw = '[{"model_entity":"VF8","aspect":"invalid","sentiment":"negative","intensity":"mild","mention_count":1,"evidence":"x"}]'
    assert _parse_response(raw) == []


def test_parse_response_multiple_items_mixed_valid():
    raw = (
        '[{"model_entity":"VF8","aspect":"pin","sentiment":"positive","intensity":"mild","mention_count":1,"evidence":"ok"},'
        '{"model_entity":"INVALID","aspect":"pin","sentiment":"positive","intensity":"mild","mention_count":1,"evidence":"bad"}]'
    )
    result = _parse_response(raw)
    assert len(result) == 1
    assert result[0]["model_entity"] == "VF8"
