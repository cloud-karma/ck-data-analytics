import wikipedia

def load_wikipedia_summary(query):
    try:
        return wikipedia.summary(query, sentences=3)
    except Exception as e:
        return f"Could not fetch info: {str(e)}"
