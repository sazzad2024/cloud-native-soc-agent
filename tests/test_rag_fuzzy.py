from rag_engine import RAGEngine

def test_fuzzy_matching():
    rag = RAGEngine() # Loads the AI Brain (all-MiniLM-L6-v2)
    
    print("\n--- 🧠 TESTING SEMANTIC UNDERSTANDING ---")
    print("We will ask 'Vague' questions, and see if it finds the 'Formal' MITRE term.\n")

    test_cases = [
        "The hacker stole all our files",          # Should map to Exfiltration
        " Someone is guessing passwords endlessly", # Should map to Brute Force
        "A bad program is hiding in the system",    # Should map to Persistence / Defense Evasion
        "They are listening to our network traffic" # Should map to Sniffing / Discovery
    ]

    for user_query in test_cases:
        print(f"❓ Alert/Query: '{user_query}'")
        hits = rag.search_mitre(user_query, k=1)
        for hit in hits:
            print(f"   ✅ Mapped to: {hit['name']} ({hit['id']})")
        print("-" * 50)

if __name__ == "__main__":
    test_fuzzy_matching()
