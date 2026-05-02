from transformers import pipeline

class SentimentPredictor:
    def __init__(self):
        self.pipe = pipeline(
            "text-classification",
            model="cardiffnlp/twitter-roberta-base-sentiment-latest",
            truncation=True,
            max_length=512,
        )

    def predict(self, text: str) -> dict:
        result = self.pipe(text)[0]
        return {"label": result["label"], "score": result["score"]}
