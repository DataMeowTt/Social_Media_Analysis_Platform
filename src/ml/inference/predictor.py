from __future__ import annotations
from transformers import pipeline

_MODEL_PATH = "/opt/models/sentiment"


class SentimentPredictor:
    def __init__(self):
        self.pipe = pipeline(
            "text-classification",
            model=_MODEL_PATH,
            truncation=True,
            max_length=128,
        )

    def predict(self, texts: list[str], batch_size: int = 64) -> list[dict]:
        results = self.pipe(texts, batch_size=batch_size)
        return [{"label": r["label"], "score": r["score"]} for r in results]
