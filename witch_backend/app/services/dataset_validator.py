"""
Dataset Validator
Pre-export validation checks for ML datasets.
Ensures dataset integrity before export.
"""

import logging
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


@dataclass
class ValidationCheck:
    """Single validation check result."""
    name: str
    passed: bool
    message: str
    severity: str = "ERROR"  # ERROR, WARNING, INFO


class DatasetValidator:
    """
    Validates ML dataset before export.
    
    Checks:
    1. Target column present
    2. Entity + observation_date present
    3. Feature names unique
    4. Train < Valid < Test dates
    5. No duplicate (entity_id, observation_date)
    """
    
    @staticmethod
    def validate(
        engine: Engine,
        sql: str,
        expected_columns: list[str],
        entity_column: str = "entity_id",
        observation_column: str = "observation_date",
        target_column: str | None = None,
        train_end: str | None = None,
        valid_end: str | None = None,
    ) -> dict[str, Any]:
        """
        Run all validation checks.
        
        Args:
            engine: SQLAlchemy engine
            sql: Final dataset SQL
            expected_columns: Expected column names
            entity_column: Entity ID column name
            observation_column: Observation date column name
            target_column: Target column name (optional)
            train_end: Training end date (optional)
            valid_end: Validation end date (optional)
            
        Returns:
            Validation report with checks and overall status
        """
        checks = []
        
        with engine.connect() as conn:
            # Get actual columns from SQL
            actual_cols = DatasetValidator._get_sql_columns(conn, sql)
            
            # Check 1: Entity column present
            checks.append(ValidationCheck(
                name="entity_column",
                passed=entity_column in actual_cols,
                message=f"Entity column '{entity_column}' present" if entity_column in actual_cols 
                        else f"MISSING: Entity column '{entity_column}'",
                severity="ERROR",
            ))
            
            # Check 2: Observation date present
            checks.append(ValidationCheck(
                name="observation_column",
                passed=observation_column in actual_cols,
                message=f"Observation column '{observation_column}' present" if observation_column in actual_cols
                        else f"MISSING: Observation column '{observation_column}'",
                severity="ERROR",
            ))
            
            # Check 3: Target column present (if specified)
            if target_column:
                checks.append(ValidationCheck(
                    name="target_column",
                    passed=target_column in actual_cols,
                    message=f"Target column '{target_column}' present" if target_column in actual_cols
                            else f"MISSING: Target column '{target_column}'",
                    severity="ERROR",
                ))
            
            # Check 4: All expected columns present
            missing = set(expected_columns) - set(actual_cols)
            checks.append(ValidationCheck(
                name="expected_columns",
                passed=len(missing) == 0,
                message=f"All {len(expected_columns)} expected columns present" if not missing
                        else f"MISSING columns: {list(missing)[:5]}",
                severity="ERROR" if missing else "INFO",
            ))
            
            # Check 5: Unique column names
            duplicates = [c for c in actual_cols if actual_cols.count(c) > 1]
            checks.append(ValidationCheck(
                name="unique_columns",
                passed=len(duplicates) == 0,
                message="All column names unique" if not duplicates
                        else f"DUPLICATE columns: {set(duplicates)}",
                severity="ERROR",
            ))
            
            # Check 6: No duplicate (entity, observation) pairs
            if entity_column in actual_cols and observation_column in actual_cols:
                has_dupes = DatasetValidator._check_duplicates(
                    conn, sql, entity_column, observation_column
                )
                checks.append(ValidationCheck(
                    name="no_duplicates",
                    passed=not has_dupes,
                    message="No duplicate (entity, observation) pairs" if not has_dupes
                            else "DUPLICATES found in (entity_id, observation_date)",
                    severity="ERROR",
                ))
            
            # Check 7: Split dates valid (if provided)
            if train_end and valid_end:
                valid_splits = train_end < valid_end
                checks.append(ValidationCheck(
                    name="split_dates",
                    passed=valid_splits,
                    message=f"Split dates valid: train <= {train_end} < valid <= {valid_end}" if valid_splits
                            else f"INVALID: train_end ({train_end}) >= valid_end ({valid_end})",
                    severity="ERROR",
                ))
            
            # Check 8: Row count sanity
            row_count = DatasetValidator._get_row_count(conn, sql)
            checks.append(ValidationCheck(
                name="row_count",
                passed=row_count > 0,
                message=f"Dataset has {row_count:,} rows" if row_count > 0
                        else "EMPTY: Dataset has 0 rows",
                severity="ERROR" if row_count == 0 else "INFO",
            ))
        
        # Summarize
        passed_checks = [c for c in checks if c.passed]
        failed_checks = [c for c in checks if not c.passed and c.severity == "ERROR"]
        
        return {
            "is_valid": len(failed_checks) == 0,
            "checks": [
                {"name": c.name, "passed": c.passed, "message": c.message, "severity": c.severity}
                for c in checks
            ],
            "passed_count": len(passed_checks),
            "failed_count": len(failed_checks),
            "total_checks": len(checks),
            "status": "success" if len(failed_checks) == 0 else "failed",
        }
    
    @staticmethod
    def _get_sql_columns(conn, sql: str) -> list[str]:
        """Get column names from SQL query."""
        try:
            sample_sql = f"SELECT * FROM ({sql.strip().rstrip(';')}) s LIMIT 0"
            result = conn.execute(text(sample_sql))
            return list(result.keys())
        except Exception as e:
            logger.warning(f"Failed to get columns: {e}")
            return []
    
    @staticmethod
    def _check_duplicates(
        conn,
        sql: str,
        entity_col: str,
        obs_col: str,
    ) -> bool:
        """Check for duplicate (entity, observation) pairs."""
        try:
            dupe_sql = f"""
                SELECT COUNT(*) as dupe_count
                FROM (
                    SELECT "{entity_col}", "{obs_col}", COUNT(*) as cnt
                    FROM ({sql.strip().rstrip(';')}) s
                    GROUP BY "{entity_col}", "{obs_col}"
                    HAVING COUNT(*) > 1
                ) dupes
            """
            result = conn.execute(text(dupe_sql))
            row = result.fetchone()
            return row[0] > 0 if row else False
        except Exception as e:
            logger.warning(f"Duplicate check failed: {e}")
            return False
    
    @staticmethod
    def _get_row_count(conn, sql: str) -> int:
        """Get approximate row count."""
        try:
            count_sql = f"SELECT COUNT(*) FROM ({sql.strip().rstrip(';')}) s LIMIT 1"
            result = conn.execute(text(count_sql))
            row = result.fetchone()
            return int(row[0]) if row else 0
        except Exception as e:
            logger.warning(f"Row count failed: {e}")
            return 0



dataset_validator = DatasetValidator()
