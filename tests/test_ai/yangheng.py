import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch.nn.functional as F

model_name = "src/ml/models/ml_ytb"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name)

labels_map = {0: "Negative", 1: "Neutral", 2: "Positive"}

texts = [
    "The iPhone's screen is beautiful, but the operating system feels way too restrictive compared to Android.",
    "Android has terrible battery optimization, making the iPhone look like a lifesaver.",
    "I don't think iPhone is a bad phone, but Android just offers so much more freedom for power users.",
]
aspects = ["iPhone", "Android"]

print("--- ABSA ANALYSIS RESULTS ---")

for text in texts:
    print(f"\nContext: {text}")
    
    for aspect in aspects:
        inputs = tokenizer(text, aspect, return_tensors="pt", truncation=True, max_length=512)
        
        with torch.no_grad():
            outputs = model(**inputs)
        
        probs = F.softmax(outputs.logits, dim=-1)
        pred_label_id = torch.argmax(probs, dim=-1).item()
        
        label = labels_map[pred_label_id]
        score = probs[0][pred_label_id].item()
        
        print(f"  └─ Aspect: {aspect:7s} -> [{label:8s} Score: {score:.3f}]")