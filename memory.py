class SessionMemory:
    def __init__(self):
        self.qa_pairs = []

    def store(self, q, a):
        self.qa_pairs.append((q, a))

    def get_history(self):
        return self.qa_pairs
