from loaders import load_wikipedia_summary
from model import generate_answer
from memory import SessionMemory

memory = SessionMemory()

print("Welcome to Personal Research Assistant!")
while True:
    query = input("Ask me anything (or type 'exit'): ")
    if query.lower() == 'exit':
        break

    context = load_wikipedia_summary(query)
    answer = generate_answer(query, context)
    
    memory.store(query, answer)
    print(f"\nAnswer: {answer}\n")
