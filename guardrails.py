import sqlparse
import re
from functools import wraps

class SecurityException(Exception):
    """Exception raised for security violations."""
    pass

class SQLGuardrail:
    """
    Validates SQL queries to ensure they are safe and strictly read-only.
    """
    
    BLOCKED_KEYWORDS = {
        'DROP', 'DELETE', 'UPDATE', 'INSERT', 'TRUNCATE', 
        'ALTER', 'GRANT', 'REVOKE', 'CREATE', 'REPLACE'
    }
    
    # Simple regex for common SQL injection patterns like ' OR 1=1
    # This catches basic tautologies and comment injections
    SQL_INJECTION_PATTERNS = [
        r"(?i)\bOR\s+[\d\w]+\s*=\s*[\d\w]+",  # OR 1=1
        r"--",                               # Comments
        r";",                                # Multiple statements
        r"/\*",                              # Block comments
    ]

    @staticmethod
    def validate_query(query: str) -> bool:
        """
        Validates the given SQL query.
        Returns True if safe, raises SecurityException if unsafe.
        """
        if not query:
            raise SecurityException("Empty query provided.")

        # 1. Parse content using sqlparse
        parsed = sqlparse.parse(query)
        if not parsed:
            raise SecurityException("Failed to parse query.")
            
        # Ensure only one statement
        if len(parsed) > 1:
            raise SecurityException("Multiple statements are not allowed.")
            
        statement = parsed[0]
        
        # 2. Check statement type (Must be SELECT)
        if statement.get_type().upper() != 'SELECT':
            raise SecurityException(f"Only SELECT statements are allowed. Found: {statement.get_type().upper()}")

        # 3. Check for Blocked Keywords in the tokens
        # We transform to string to check generalized keywords, 
        # but iterating tokens is safer for specific context. 
        # For simplicity/robustness against obfuscation, we check the normalized string upper.
        normalized_query = str(statement).upper()
        
        # Token-based check for keywords to avoid matching substrings in data
        for token in statement.flatten():
            if token.ttype in (sqlparse.tokens.Keyword, sqlparse.tokens.Keyword.DML, sqlparse.tokens.Keyword.DDL):
                if token.value.upper() in SQLGuardrail.BLOCKED_KEYWORDS:
                     raise SecurityException(f"Blocked keyword detected: {token.value.upper()}")

        # 4. Regex checks for Injection Patterns
        for pattern in SQLGuardrail.SQL_INJECTION_PATTERNS:
            if re.search(pattern, query):
                raise SecurityException(f"Potential SQL Injection detected pattern: {pattern}")
                
        return True

def verified_query(func):
    """
    Decorator to validate the first argument (assumed to be the query) of a function.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Assume the query is the first argument or 'query' kwarg
        query = kwargs.get('query')
        if not query and args:
            # If method call (self, query), query is args[1]
            # If function call (query), query is args[0]
            # Heuristic: The first string argument is the query
            for arg in args:
                if isinstance(arg, str):
                    query = arg
                    break
        
        if query:
            SQLGuardrail.validate_query(query)
            
        return func(*args, **kwargs)
    return wrapper
