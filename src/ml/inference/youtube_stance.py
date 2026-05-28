import gc
import os

_WORKSPACE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
_MODEL_PATH = os.path.join(_WORKSPACE, "src/ml/models/zeroshot")
_LABELS = ["agreement", "disagreement", "neutral"]
_INFER_BATCH_SIZE = 32


def predict_stance(reply_texts: list, parent_texts: list) -> list:
    import torch
    from transformers import AutoTokenizer, AutoModelForSequenceClassification

    torch.set_num_threads(os.cpu_count())
    tokenizer = AutoTokenizer.from_pretrained(_MODEL_PATH)
    model = AutoModelForSequenceClassification.from_pretrained(_MODEL_PATH)
    model.eval()

    print(f"[predict_stance] model={_MODEL_PATH}")
    total = len(reply_texts)
    results = []
    for batch_start in range(0, total, _INFER_BATCH_SIZE):
        batch_replies = reply_texts[batch_start: batch_start + _INFER_BATCH_SIZE]
        batch_parents = parent_texts[batch_start: batch_start + _INFER_BATCH_SIZE]

        premises, hypotheses = [], []
        for reply, parent in zip(batch_replies, batch_parents):
            for label in _LABELS:
                premises.append(reply)
                hypotheses.append(f"This response expresses {label} with: {parent}")

        inputs = tokenizer(premises, hypotheses, return_tensors="pt", truncation=True, max_length=128, padding=True)
        with torch.inference_mode():
            logits = model(**inputs).logits

        entail_scores = torch.softmax(logits, dim=-1)[:, -1]
        for i in range(len(batch_replies)):
            scores = entail_scores[i * len(_LABELS): (i + 1) * len(_LABELS)]
            results.append(_LABELS[scores.argmax().item()])

        done = min(batch_start + _INFER_BATCH_SIZE, total)
        print(f"[predict_stance] {done}/{total}")

    del tokenizer, model
    gc.collect()
    return results
