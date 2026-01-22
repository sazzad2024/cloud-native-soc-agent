from guardrails import SQLGuardrail, verified_query, SecurityException
import unittest

class TestSQLGuardrails(unittest.TestCase):

    def test_valid_select(self):
        """Test that a standard SELECT query is allowed."""
        query = "SELECT * FROM active_alerts WHERE severity = 'High'"
        self.assertTrue(SQLGuardrail.validate_query(query))
        print(f"[PASS] Valid Query Allowed: {query}")

    def test_blocked_drop(self):
        """Test that DROP commands are blocked."""
        query = "DROP TABLE active_alerts"
        with self.assertRaises(SecurityException) as cm:
            SQLGuardrail.validate_query(query)
        print(f"[PASS] Blocked Query Caught: {query} -> {cm.exception}")

    def test_blocked_delete(self):
        """Test that DELETE commands are blocked."""
        query = "DELETE FROM user_directory WHERE id = 1"
        with self.assertRaises(SecurityException) as cm:
            SQLGuardrail.validate_query(query)
        print(f"[PASS] Blocked Query Caught: {query} -> {cm.exception}")

    def test_injection_or(self):
        """Test detection of 'OR 1=1' injection patterns."""
        query = "SELECT * FROM users WHERE id = 1 OR 1=1"
        with self.assertRaises(SecurityException) as cm:
            SQLGuardrail.validate_query(query)
        print(f"[PASS] Injection Caught: {query} -> {cm.exception}")

    def test_injection_comment(self):
        """Test detection of comment-based injection."""
        query = "SELECT * FROM users; --"
        with self.assertRaises(SecurityException) as cm:
            SQLGuardrail.validate_query(query)
        print(f"[PASS] Injection Caught: {query} -> {cm.exception}")
        
    def test_multiple_statements(self):
        """Test that multiple statements (;) are blocked."""
        query = "SELECT * FROM users; SELECT * FROM admins"
        with self.assertRaises(SecurityException) as cm:
            SQLGuardrail.validate_query(query)
        print(f"[PASS] Multiple Statements Caught: {query} -> {cm.exception}")

    def test_decorator(self):
        """Test the @verified_query decorator."""
        
        @verified_query
        def run_db_query(query):
            return "Query Executed"

        # Valid call
        self.assertEqual(run_db_query("SELECT * FROM table"), "Query Executed")
        
        # Invalid call
        with self.assertRaises(SecurityException):
            run_db_query("DROP TABLE table")
            
        print("[PASS] Decorator Verification Successful")

if __name__ == '__main__':
    unittest.main()
