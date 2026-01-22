import json
import os
from langchain_community.vectorstores import FAISS
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_core.documents import Document

# Configuration
DATA_PATH = os.path.join("data", "enterprise-attack.json")
VECTOR_DB_PATH = os.path.join("data", "mitre_vector_db")

class RAGEngine:
    def __init__(self):
        self.embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
        self.vector_store = None
        
        # Load or Create Index
        if os.path.exists(VECTOR_DB_PATH):
            print("[RAG] Loading existing Vector Database...")
            self.vector_store = FAISS.load_local(VECTOR_DB_PATH, self.embeddings, allow_dangerous_deserialization=True)
        else:
            print("[RAG] Creating new Vector Database from MITRE data...")
            self.build_index()

    def build_index(self):
        """
        Parses MITRE STIX data, extracts techniques, and builds FAISS index.
        """
        if not os.path.exists(DATA_PATH):
            raise FileNotFoundError(f"MITRE Data not found at {DATA_PATH}")

        with open(DATA_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)

        documents = []
        
        # STIX objects are in the 'objects' list
        for obj in data.get('objects', []):
            if obj.get('type') == 'attack-pattern':
                name = obj.get('name', 'Unknown')
                description = obj.get('description', '')
                mitre_id = "Unknown"
                
                # Extract external ID (e.g., T1566)
                for ref in obj.get('external_references', []):
                    if ref.get('source_name') == 'mitre-attack':
                        mitre_id = ref.get('external_id')
                        break
                
                # Create a clear text for embedding
                page_content = f"Technique: {name} ({mitre_id})\nDescription: {description}"
                
                metadata = {
                    "name": name,
                    "id": mitre_id,
                    "url": f"https://attack.mitre.org/techniques/{mitre_id}"
                }
                
                documents.append(Document(page_content=page_content, metadata=metadata))

        print(f"[RAG] Indexing {len(documents)} techniques...")
        self.vector_store = FAISS.from_documents(documents, self.embeddings)
        self.vector_store.save_local(VECTOR_DB_PATH)
        print("[RAG] Index saved.")

    def search_mitre(self, query: str, k: int = 2):
        """
        Searches the knowledge base for techniques relevant to the query.
        """
        if not self.vector_store:
            raise Exception("Vector Store not initialized.")
            
        print(f"[RAG] Searching for: '{query}'")
        results = self.vector_store.similarity_search(query, k=k)
        
        formatted_results = []
        for doc in results:
            formatted_results.append({
                "name": doc.metadata['name'],
                "id": doc.metadata['id'],
                "description_snippet": doc.page_content[:300] + "..."
            })
            
        return formatted_results

def main():
    rag = RAGEngine()
    
    # Test Queries
    queries = [
        "How do attackers steal passwords?",
        "What is Phishing?",
        "Explain SQL Injection"
    ]
    
    for q in queries:
        print(f"\n--- Query: {q} ---")
        hits = rag.search_mitre(q)
        for hit in hits:
            print(f"found: {hit['name']} ({hit['id']})")

if __name__ == "__main__":
    main()
