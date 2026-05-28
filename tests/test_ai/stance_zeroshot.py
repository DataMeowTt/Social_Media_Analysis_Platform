from transformers import pipeline

MODEL_PATH = "src/ml/models/zeroshot"

classifier = pipeline("zero-shot-classification", model=MODEL_PATH)

test_pairs = [
    ["iPhone is better for video.", "Totally agree"],
    ["Android is for geeks.", "That's not true. Android is easy now."],
    ["The S24 looks great.", "I'm waiting for the MacBook Air."]
]

print("--- STANCE RESULTS (zero-shot) ---\n")
for i, (text_a, text_b) in enumerate(test_pairs, start=1):
    result = classifier(
        text_b,
        candidate_labels=["agreement", "disagreement", "neutral"],
        hypothesis_template=f"This response expresses {{}} with: {text_a}",
    )
    label  = result["labels"][0]
    score  = result["scores"][0]
    all_scores = {l: f"{s:.3f}" for l, s in zip(result["labels"], result["scores"])}
    print(f"Pair {i}:")
    print(f"  A: {text_a}")
    print(f"  B: {text_b}")
    print(f"  → {label} (score: {score:.3f}) | all: {all_scores}")
    print()
