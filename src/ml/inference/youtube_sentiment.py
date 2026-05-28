import gc
import os

_WORKSPACE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
_MODEL_PATH = os.path.join(_WORKSPACE, "src/ml/models/ytb_sentiment")
_LABELS = {0: "Negative", 1: "Neutral", 2: "Positive"}
_INFER_BATCH_SIZE = 32


def _batch_predict(tokenizer, model, texts: list, aspect: str) -> list:
    import torch
    all_preds = []
    total = len(texts)
    for i in range(0, total, _INFER_BATCH_SIZE):
        chunk = texts[i: i + _INFER_BATCH_SIZE]
        inputs = tokenizer(chunk, [aspect] * len(chunk), return_tensors="pt", truncation=True, max_length=128, padding=True)
        with torch.inference_mode():
            logits = model(**inputs).logits
        all_preds.extend([_LABELS[idx] for idx in logits.argmax(dim=-1).tolist()])
        print(f"[predict_sentiment:{aspect}] {min(i + _INFER_BATCH_SIZE, total)}/{total}")
    return all_preds


def predict_sentiment(texts: list, brand_lists: list) -> tuple:
    import torch
    from transformers import AutoTokenizer, AutoModelForSequenceClassification

    torch.set_num_threads(os.cpu_count())
    tokenizer = AutoTokenizer.from_pretrained(_MODEL_PATH)
    model = AutoModelForSequenceClassification.from_pretrained(_MODEL_PATH)
    model.eval()

    iphone_results = ["Neutral"] * len(texts)
    android_results = ["Neutral"] * len(texts)
    iphone_idx, iphone_texts = [], []
    android_idx, android_texts = [], []

    for i, (text, brand_list) in enumerate(zip(texts, brand_lists)):
        brand_set = set(brand_list) if brand_list else set()
        if "iPhone" in brand_set:
            iphone_idx.append(i); iphone_texts.append(text)
        if "Android" in brand_set:
            android_idx.append(i); android_texts.append(text)

    print(f"[predict_sentiment] model={_MODEL_PATH}")
    print(f"[predict_sentiment] total={len(texts)} | iphone={len(iphone_texts)} | android={len(android_texts)}")

    if iphone_texts:
        for idx, pred in zip(iphone_idx, _batch_predict(tokenizer, model, iphone_texts, "iPhone")):
            iphone_results[idx] = pred
    if android_texts:
        for idx, pred in zip(android_idx, _batch_predict(tokenizer, model, android_texts, "Android")):
            android_results[idx] = pred

    del tokenizer, model
    gc.collect()
    return iphone_results, android_results
