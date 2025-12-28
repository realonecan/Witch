"""
SQL Validator Service
Validates generated SQL by running on a sample before full execution.
SQL validation and quality checks.
"""

import logging
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class ValidationResult:
    """Result of SQL validation."""
    
    def __init__(
        self,
        is_valid: bool,
        sample_rows: list[dict] = None,
        row_count: int = 0,
        column_names: list[str] = None,
        error_message: Optional[str] = None,
        error_type: Optional[str] = None,
    ):
        self.is_valid = is_valid
        self.sample_rows = sample_rows or []
        self.row_count = row_count
        self.column_names = column_names or []
        self.error_message = error_message
        self.error_type = error_type
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "is_valid": self.is_valid,
            "sample_rows": self.sample_rows,
            "row_count": self.row_count,
            "column_names": self.column_names,
            "error_message": self.error_message,
            "error_type": self.error_type,
        }


class SQLValidator:
    """
    Service for validating SQL queries before full execution.
    
    Catches errors early by running SQL on a limited sample.
    """
    
    @staticmethod
    def validate_sql_on_sample(
        engine: Engine,
        sql: str,
        limit: int = 1000,
        timeout_seconds: int = 30,
    ) -> ValidationResult:
        """
        Validate SQL by running on a limited sample.
        
        Args:
            engine: SQLAlchemy engine
            sql: SQL query to validate
            limit: Maximum rows to return (default 1000)
            timeout_seconds: Query timeout in seconds
            
        Returns:
            ValidationResult with success/failure and sample data
        """
        # Wrap SQL with LIMIT to prevent full table scans
        # Handle CTEs by wrapping the entire query
        sql_clean = sql.strip().rstrip(';')
        
        # Check if SQL is a CTE (starts with WITH)
        if sql_clean.upper().startswith("WITH"):
            # Find the final SELECT and wrap it
            # Wrap entire query in a subquery
            sample_sql = f"""
SELECT * FROM (
    {sql_clean}
) AS sample_query
LIMIT {limit}
"""
        else:
            # Simple query - just add LIMIT
            sample_sql = f"{sql_clean}\nLIMIT {limit}"
        
        try:
            with engine.connect() as conn:
                # Set query timeout (Postgres specific)
                try:
                    conn.execute(text(f"SET statement_timeout = {timeout_seconds * 1000}"))
                except Exception:
                    pass  # Skip if not Postgres
                
                result = conn.execute(text(sample_sql))
                rows = result.fetchall()
                columns = list(result.keys())
                
                # Convert rows to dicts
                sample_rows = [dict(zip(columns, row)) for row in rows]
                
                # Reset timeout
                try:
                    conn.execute(text("SET statement_timeout = 0"))
                except Exception:
                    pass
                
                return ValidationResult(
                    is_valid=True,
                    sample_rows=sample_rows[:100],  # Return max 100 for response
                    row_count=len(rows),
                    column_names=columns,
                )
                
        except Exception as e:
            error_str = str(e)
            
            # Categorize error type
            if "syntax" in error_str.lower():
                error_type = "SYNTAX_ERROR"
            elif "column" in error_str.lower() and "not" in error_str.lower():
                error_type = "COLUMN_NOT_FOUND"
            elif "relation" in error_str.lower() or "table" in error_str.lower():
                error_type = "TABLE_NOT_FOUND"
            elif "timeout" in error_str.lower() or "cancel" in error_str.lower():
                error_type = "TIMEOUT"
            else:
                error_type = "UNKNOWN"
            
            logger.warning(f"SQL validation failed: {error_str}")
            
            return ValidationResult(
                is_valid=False,
                error_message=error_str,
                error_type=error_type,
            )
    
    @staticmethod
    def check_leakage_prevention(sql: str) -> list[str]:
        """
        Check SQL for potential data leakage patterns.
        
        Returns list of warnings if potential leakage detected.
        """
        warnings = []
        sql_upper = sql.upper()
        
        # Check for missing time constraints
        if "OBSERVATION_DATE" not in sql_upper:
            warnings.append("No observation_date reference found. May cause data leakage.")
        
        # Check for proper inequality (< not <=)
        if "OBSERVATION_DATE" in sql_upper:
            # Good patterns: event_date < observation_date
            # Bad patterns: event_date <= observation_date (includes same day)
            if "<= G.OBSERVATION_DATE" in sql_upper or "<=G.OBSERVATION_DATE" in sql_upper:
                # This is actually OK for windowed features
                pass
            if "> G.OBSERVATION_DATE" in sql_upper:
                warnings.append("Event date > observation_date found. This causes leakage.")
        
        return warnings



sql_validator = SQLValidator()
