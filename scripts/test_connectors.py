import unittest
from unittest.mock import MagicMock, patch
from connectors import AegisConnector
from guardrails import SecurityException

class TestAegisConnector(unittest.TestCase):

    @patch('connectors.snowflake.connector.connect')
    def test_fetch_user_context(self, mock_sf_connect):
        """Test fetching user context from Snowflake (Mocked)."""
        # Configure the mock to return a cursor with specific data
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_sf_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (1, "alice@company.com", "HR", 10)
        
        connector = AegisConnector()
        result = connector.fetch_user_context(1)
        
        # Expecting (1, "alice@company.com", "HR", 10)
        self.assertIsNotNone(result)
        self.assertEqual(result[1], "alice@company.com")
        print("[PASS] Snowflake User Context (Mock) Success")

    @patch('connectors.sql.connect')
    def test_databricks_traffic(self, mock_db_connect):
        """Test fetching traffic volume from Databricks."""
        # Configure Mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_db_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Side effect to return different results based on execution
        # Or simpler: just return the success case and verify call structure
        mock_cursor.fetchall.return_value = [(500000,)]
        
        connector = AegisConnector()
        
        # Test known IP
        result = connector.get_traffic_volume("192.168.1.105")
        
        # We need to verify what we got. 
        # Since I hardcoded the return value of the mock to 500k, checking it is trivial but validates the flow.
        self.assertEqual(result[0][0], 500000)
        
        print("[PASS] Databricks Traffic Volume (Mock) Success")

    def test_guardrail_enforcement(self):
        """Test that guardrails still work on the cloud connectors."""
        connector = AegisConnector()
        
        # Attempt Injection works same as before
        with self.assertRaises(SecurityException):
            # Pass malicious string to an integer field (weak test but checks decorator presence)
            # A better test would be if get_traffic_volume took a raw SQL string.
            # But relying on types:
            connector.fetch_user_context("1 OR 1=1")
            
        print("[PASS] Security Guardrails Enforced on Cloud Connectors")

if __name__ == '__main__':
    unittest.main()
