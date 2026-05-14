from src.ml.inference.predictor import SentimentPredictor

predictor = SentimentPredictor()

texts = [
    "I love this product, it's amazing!",
    "This is absolutely terrible, worst experience ever.",
    "The weather is okay today.",
    "@nytpolitics Finally some great news, hope they lose",  # tweet thực từ data
]

results = predictor.predict(texts)

for text, result in zip(texts, results):
    label = result["label"]
    score = result["score"]
    print(f"[{label:8s} {score:.2f}] {text[:60]}")

