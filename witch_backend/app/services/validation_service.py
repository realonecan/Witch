"""
Validation Service


Validation layers:
A) SQL syntax validation via EXPLAIN
B) Forbidden keyword detection (security)
C) Contract validation (required output columns)
D) Feature column existence check
E) Mean imputation type compatibility
"""

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.services.grain_service import validate_identifier


# =============================================================================
# Validation Types
# =============================================================================


class ValidationSeverity(str, Enum):
    """Severity levels for validation issues."""
    ERROR = "error"       # Blocks execution
    WARNING = "warning"   # Should review but can proceed
    INFO = "info"         # Informational only


@dataclass
class ValidationIssue:
    """A single validation issue found."""
    severity: ValidationSeverity
    code: str           # Machine-readable code, e.g., "FORBIDDEN_KEYWORD"
    message: str        # Human-readable message
    location: str = ""  # Where the issue was found (e.g., "feature_0")
    suggestion: str = ""  # How to fix it


@dataclass
class ValidationResult:
    """Result of validation checks."""
    valid: bool
    issues: list[ValidationIssue] = field(default_factory=list)
    
    @property
    def errors(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == ValidationSeverity.ERROR]
    
    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == ValidationSeverity.WARNING]
    
    def add_error(self, code: str, message: str, location: str = "", suggestion: str = ""):
        self.issues.append(ValidationIssue(
            ValidationSeverity.ERROR, code, message, location, suggestion
        ))
        self.valid = False
    
    def add_warning(self, code: str, message: str, location: str = "", suggestion: str = ""):
        self.issues.append(ValidationIssue(
            ValidationSeverity.WARNING, code, message, location, suggestion
        ))
    
    def add_info(self, code: str, message: str, location: str = "", suggestion: str = ""):
        self.issues.append(ValidationIssue(
            ValidationSeverity.INFO, code, message, location, suggestion
        ))


# =============================================================================
# Forbidden SQL Patterns
# =============================================================================


# Keywords that should never appear in feature/dataset SQL
FORBIDDEN_KEYWORDS = [
    r'\bDROP\b',
    r'\bDELETE\b',
    r'\bINSERT\b',
    r'\bUPDATE\b',
    r'\bALTER\b',
    r'\bTRUNCATE\b',
    r'\bCREATE\b',
    r'\bGRANT\b',
    r'\bREVOKE\b',
    r'\bEXEC\b',
    r'\bEXECUTE\b',
]

# Compiled pattern for efficiency
FORBIDDEN_PATTERN = re.compile(
    '|'.join(FORBIDDEN_KEYWORDS),
    re.IGNORECASE
)


# =============================================================================
# Validation Service
# =============================================================================


class ValidationService:
    """
    Validates SQL and dataset configurations for safety and correctness.
    """

    @staticmethod
    def check_forbidden_keywords(sql: str, location: str = "") -> list[ValidationIssue]:
        """
        Check for forbidden SQL keywords that could modify data.
        
        Args:
            sql: SQL string to check
            location: Location identifier for error messages
            
        Returns:
            List of validation issues found
        """
        issues = []
        
        # Check for multiple statements (semicolons inside SQL)
        # Allow trailing semicolon but not embedded ones
        sql_stripped = sql.strip().rstrip(";")
        if ";" in sql_stripped:
            issues.append(ValidationIssue(
                ValidationSeverity.ERROR,
                "MULTI_STATEMENT",
                "SQL contains multiple statements (embedded semicolons)",
                location,
                "Remove embedded semicolons; only trailing semicolon allowed",
            ))
        
        # Check for forbidden keywords
        matches = FORBIDDEN_PATTERN.findall(sql)
        if matches:
            unique_matches = list(set(m.upper() for m in matches))
            issues.append(ValidationIssue(
                ValidationSeverity.ERROR,
                "FORBIDDEN_KEYWORD",
                f"SQL contains forbidden keywords: {', '.join(unique_matches)}",
                location,
                "Remove data modification statements; only SELECT is allowed",
            ))
        
        return issues

    @staticmethod
    def validate_sql_syntax(
        engine: Engine,
        sql: str,
        location: str = "",
    ) -> ValidationResult:
        """
        Validate SQL syntax using EXPLAIN.
        
        Args:
            engine: SQLAlchemy engine
            sql: SQL to validate
            location: Location identifier
            
        Returns:
            ValidationResult with syntax check outcome
        """
        result = ValidationResult(valid=True)
        
        # First check for forbidden keywords
        keyword_issues = ValidationService.check_forbidden_keywords(sql, location)
        for issue in keyword_issues:
            result.issues.append(issue)
            if issue.severity == ValidationSeverity.ERROR:
                result.valid = False
        
        # If keyword errors, don't bother with EXPLAIN
        if not result.valid:
            return result
        
        # Try EXPLAIN to validate syntax
        try:
            sql_clean = sql.strip().rstrip(";")
            explain_sql = f"EXPLAIN {sql_clean}"
            
            with engine.connect() as conn:
                conn.execute(text(explain_sql))
            
        except Exception as e:
            error_msg = str(e)
            # Extract just the relevant part of the error
            if "syntax error" in error_msg.lower():
                result.add_error(
                    "SYNTAX_ERROR",
                    f"SQL syntax error: {error_msg[:200]}",
                    location,
                    "Check SQL syntax and column/table names",
                )
            else:
                result.add_error(
                    "SQL_ERROR",
                    f"SQL validation failed: {error_msg[:200]}",
                    location,
                    "Verify table/column names exist and are accessible",
                )
        
        return result

    @staticmethod
    def validate_output_contract(
        engine: Engine,
        sql: str,
        required_columns: list[str],
        location: str = "",
    ) -> ValidationResult:
        """
        Validate that SQL outputs required columns.
        
        Uses SELECT * FROM (<sql>) t LIMIT 0 to get column metadata.
        
        Args:
            engine: SQLAlchemy engine
            sql: SQL to check
            required_columns: Columns that must be in output
            location: Location identifier
            
        Returns:
            ValidationResult with contract check outcome
        """
        result = ValidationResult(valid=True)
        
        try:
            sql_clean = sql.strip().rstrip(";")
            check_sql = f"SELECT * FROM ({sql_clean}) AS _contract_check LIMIT 0"
            
            with engine.connect() as conn:
                cursor_result = conn.execute(text(check_sql))
                actual_columns = list(cursor_result.keys())
            
            # Check for required columns
            missing = [c for c in required_columns if c not in actual_columns]
            if missing:
                result.add_error(
                    "MISSING_COLUMNS",
                    f"Required columns missing from output: {', '.join(missing)}",
                    location,
                    f"Add missing columns to SELECT: {', '.join(missing)}",
                )
            
            # Check for duplicate columns
            if len(actual_columns) != len(set(actual_columns)):
                seen = set()
                duplicates = []
                for col in actual_columns:
                    if col in seen:
                        duplicates.append(col)
                    seen.add(col)
                
                result.add_warning(
                    "DUPLICATE_COLUMNS",
                    f"Output contains duplicate columns: {', '.join(duplicates)}",
                    location,
                    "Use aliases to make column names unique",
                )
                
        except Exception as e:
            result.add_error(
                "CONTRACT_CHECK_FAILED",
                f"Could not verify output contract: {str(e)[:200]}",
                location,
                "Ensure SQL is valid and can be executed",
            )
        
        return result

    @staticmethod
    def validate_feature_columns(
        engine: Engine,
        sql: str,
        declared_columns: list[str],
        location: str = "",
    ) -> ValidationResult:
        """
        Validate that declared feature columns exist in SQL output.
        
        Args:
            engine: SQLAlchemy engine
            sql: Feature SQL
            declared_columns: Columns declared in feature_columns
            location: Location identifier
            
        Returns:
            ValidationResult
        """
        result = ValidationResult(valid=True)
        
        try:
            sql_clean = sql.strip().rstrip(";")
            check_sql = f"SELECT * FROM ({sql_clean}) AS _feature_check LIMIT 0"
            
            with engine.connect() as conn:
                cursor_result = conn.execute(text(check_sql))
                actual_columns = list(cursor_result.keys())
            
            # Check declared columns exist
            missing = [c for c in declared_columns if c not in actual_columns]
            if missing:
                result.add_error(
                    "DECLARED_COLUMNS_MISSING",
                    f"Declared feature columns not in SQL output: {', '.join(missing)}",
                    location,
                    "Ensure declared columns match SELECT clause",
                )
            
            # Info: list actual columns for debugging
            result.add_info(
                "ACTUAL_COLUMNS",
                f"SQL outputs: {', '.join(actual_columns)}",
                location,
            )
                
        except Exception as e:
            result.add_error(
                "COLUMN_CHECK_FAILED",
                f"Could not verify feature columns: {str(e)[:200]}",
                location,
            )
        
        return result

    @staticmethod
    def validate_mean_imputation_types(
        engine: Engine,
        sql: str,
        mean_columns: list[str],
        location: str = "",
    ) -> ValidationResult:
        """
        Warn if MEAN strategy is applied to non-numeric columns.
        
        Args:
            engine: SQLAlchemy engine
            sql: SQL to check
            mean_columns: Columns marked for mean imputation (must be valid identifiers)
            location: Location identifier
            
        Returns:
            ValidationResult with type warnings
        """
        result = ValidationResult(valid=True)
        
        if not mean_columns:
            return result
        
        # Validate that mean_columns are valid identifiers
        for col in mean_columns:
            try:
                validate_identifier(col, "mean_column")
            except ValueError as e:
                result.add_warning(
                    "INVALID_MEAN_COLUMN",
                    f"Invalid column name for mean imputation: {col}",
                    location,
                )
                return result  # Can't safely build SQL with invalid identifiers
        
        try:
            sql_clean = sql.strip().rstrip(";")
            
            # Use safe indexed aliases to avoid issues with dots/special chars
            type_checks = [f'pg_typeof("{col}")::text AS col{i}_type' for i, col in enumerate(mean_columns)]
            check_sql = f"SELECT {', '.join(type_checks)} FROM ({sql_clean}) AS _type_check LIMIT 1"
            
            with engine.connect() as conn:
                cursor_result = conn.execute(text(check_sql))
                row = cursor_result.fetchone()
            
            if row:
                numeric_types = {'integer', 'bigint', 'smallint', 'numeric', 'decimal', 'real', 'double precision', 'float', 'int4', 'int8', 'float4', 'float8'}
                
                for i, col in enumerate(mean_columns):
                    col_type = row[i] if row else "unknown"
                    col_type_lower = str(col_type).lower()
                    
                    if col_type_lower not in numeric_types:
                        result.add_warning(
                            "MEAN_NON_NUMERIC",
                            f"Column '{col}' has type '{col_type}' - mean imputation may not work",
                            location,
                            "Mean imputation is only meaningful for numeric types",
                        )
                        
        except Exception as e:
            result.add_warning(
                "TYPE_CHECK_FAILED",
                f"Could not verify column types: {str(e)[:100]}",
                location,
            )
        
        return result

    @staticmethod
    def validate_dataset_sql(
        engine: Engine,
        dataset_sql: str,
        feature_sqls: list[dict[str, Any]] | None = None,
        post_sql_impute: list[dict[str, str]] | None = None,
    ) -> ValidationResult:
        """
        Full validation of assembled dataset SQL.
        
        Args:
            engine: SQLAlchemy engine
            dataset_sql: Final assembled dataset SQL
            feature_sqls: Optional list of feature SQL dicts with 'sql', 'feature_columns'
            post_sql_impute: Optional list of columns needing post-SQL imputation
            
        Returns:
            Comprehensive ValidationResult
        """
        result = ValidationResult(valid=True)
        
        # 1. Validate main dataset SQL syntax and keywords
        main_result = ValidationService.validate_sql_syntax(
            engine, dataset_sql, "dataset_sql"
        )
        result.issues.extend(main_result.issues)
        if not main_result.valid:
            result.valid = False
        
        # 2. Validate output contract (must have entity_id, observation_date)
        # Only skip if main SQL had keyword errors (can't run EXPLAIN on bad SQL)
        main_has_keyword_errors = any(
            i.code in ("FORBIDDEN_KEYWORD", "MULTI_STATEMENT") 
            for i in main_result.issues if i.severity == ValidationSeverity.ERROR
        )
        if not main_has_keyword_errors:
            contract_result = ValidationService.validate_output_contract(
                engine, dataset_sql, ["entity_id", "observation_date"], "dataset_sql"
            )
            result.issues.extend(contract_result.issues)
            if not contract_result.valid:
                result.valid = False
        
        # 3. Validate individual feature SQLs if provided (collect all issues)
        if feature_sqls:
            for i, feature in enumerate(feature_sqls):
                loc = f"feature_{i}"
                sql = feature.get("sql", "")
                declared = feature.get("feature_columns", [])
                
                # Check forbidden keywords for this feature
                keyword_issues = ValidationService.check_forbidden_keywords(sql, loc)
                result.issues.extend(keyword_issues)
                feature_has_keyword_errors = any(
                    iss.severity == ValidationSeverity.ERROR for iss in keyword_issues
                )
                if feature_has_keyword_errors:
                    result.valid = False
                
                # Check declared columns exist (only skip if this feature has keyword errors)
                if declared and not feature_has_keyword_errors:
                    col_result = ValidationService.validate_feature_columns(
                        engine, sql, declared, loc
                    )
                    result.issues.extend(col_result.issues)
                    if not col_result.valid:
                        result.valid = False
        
        # 4. Validate mean imputation types (always run if we have mean columns)
        if post_sql_impute:
            mean_cols = [p["column"] for p in post_sql_impute if p.get("strategy") == "mean"]
            if mean_cols and not main_has_keyword_errors:
                type_result = ValidationService.validate_mean_imputation_types(
                    engine, dataset_sql, mean_cols, "post_sql_impute"
                )
                result.issues.extend(type_result.issues)
        
        return result



validation_service = ValidationService()
