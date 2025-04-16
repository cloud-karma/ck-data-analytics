from transformers import pipeline

# Load TinyLlama (or fallback smaller model)
qa_pipeline = pipeline("text-generation", model="TinyLlama/TinyLlama-1.1B-Chat-v1.0")

def generate_answer(question, context):
    prompt = f"Context: {context}\nQuestion: {question}\nAnswer:"
    result = qa_pipeline(prompt, max_new_tokens=100)[0]["generated_text"]
    return result.split("Answer:")[-1].strip()
