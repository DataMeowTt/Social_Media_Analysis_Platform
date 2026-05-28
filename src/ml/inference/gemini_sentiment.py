from __future__ import annotations

import json
import os
import re
import urllib.request

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType, StructField, StructType

from src.utils.logger import get_logger

logger = get_logger(__name__)

_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"

_PROMPT = """\
Phân tích sentiment của {n} văn bản tiếng Việt sau. Nhãn hợp lệ: "positive", "negative", "neutral".
Trả về ONLY một JSON array, mỗi phần tử: {{"index": <int>, "label": "<nhãn>"}}.
Không markdown, không giải thích.

{texts}

JSON array:"""


def _call_gemini(prompt: str) -> str:
    api_key = os.environ.get("GEMINI_API_KEY", "")
    model   = os.environ.get("MODEL", "gemini-2.5-flash-lite")
    url     = _API_URL.format(model=model, key=api_key)
    body    = json.dumps({"contents": [{"parts": [{"text": prompt}]}]}).encode()
    req     = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = json.loads(resp.read().decode())
    return data["candidates"][0]["content"]["parts"][0]["text"]


def add_sentiment(df: DataFrame) -> DataFrame:
    pdf = df.toPandas()
    if pdf.empty:
        return (
            df.withColumn("sentiment_label", F.lit(None).cast(StringType()))
              .withColumn("sentiment_score",  F.lit(None).cast(FloatType()))
        )

    texts    = pdf["text"].fillna("").tolist()
    numbered = "\n".join(f"{i}. {(t or '')[:200]}" for i, t in enumerate(texts))
    prompt   = _PROMPT.format(n=len(texts), texts=numbered)

    try:
        raw    = _call_gemini(prompt)
        match  = re.search(r"\[.*\]", raw, re.DOTALL)
        items  = json.loads(match.group()) if match else []
        labels = ["neutral"] * len(texts)
        valid  = {"positive", "negative", "neutral"}
        for item in items:
            idx   = item.get("index")
            label = item.get("label", "neutral")
            if isinstance(idx, int) and 0 <= idx < len(texts) and label in valid:
                labels[idx] = label
    except Exception as exc:
        logger.warning(f"[gemini_sentiment] Gemini failed: {exc} — defaulting to neutral")
        labels = ["neutral"] * len(texts)

    logger.info(f"[gemini_sentiment] {len(labels)} sentiment labels from Gemini")
    pdf["sentiment_label"] = labels
    pdf["sentiment_score"] = 1.0

    new_schema = StructType(df.schema.fields + [
        StructField("sentiment_label", StringType(), True),
        StructField("sentiment_score", FloatType(),  True),
    ])
    return df.sparkSession.createDataFrame(pdf, schema=new_schema)
