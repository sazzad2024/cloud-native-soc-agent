import unittest
import os
import shutil
# We import the RAGEngine class to test it
from rag_engine import RAGEngine

class TestRAGEngine(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """
        Initialize the RAG engine once for all tests.
        This will trigger the index building if it doesn't exist.
        """
        print("[Test] Initializing RAG Engine (this may take a moment)...")
        cls.engine = RAGEngine()

    def test_search_phishing(self):
        """Test that 'phishing' returns T1566 (Phishing)."""
        results = self.engine.search_mitre("User clicked a link in a suspicious email")
        
        # Check if we got results
        self.assertTrue(len(results) > 0, "No results returned for phishing query")
        
        # Check if one of the results is Phishing
        # Note: Depending on the embedding model, it might return 'Spearphishing Link' etc.
        # We check for the word 'Phishing' or the ID T1566
        found = False
        for res in results:
            if "Phishing" in res['name'] or "T1566" in res['id']:
                found = True
                break
        
        self.assertTrue(found, f"Phishing technique not found in top results: {results}")
        print("[PASS] Phishing detected successfully.")

    def test_search_bruteforce(self):
        """Test that 'guessing passwords' returns Brute Force."""
        results = self.engine.search_mitre("Attacker is trying many passwords to login")
        
        found = False
        for res in results:
            # T1110 is Brute Force
            if "Brute Force" in res['name'] or "T1110" in res['id']:
                found = True
                break
        
        self.assertTrue(found, f"Brute Force not found in results: {results}")
        print("[PASS] Brute Force detected successfully.")

if __name__ == '__main__':
    unittest.main()
