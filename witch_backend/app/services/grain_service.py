"""
Grain Service
Defines the entity and observation point for ML dataset construction.


FIXES APPLIED:
- FIX 1: Identifier allowlist validation (SQL injection prevention)
- FIX 2: Fixed obs_date doesn't require obs_col NOT NULL
- FIX 3: Clean ranked SQL (no split/replace hacks)
- FIX 4: Validate dedup_order_by and dedup_tiebreaker
- FIX 5: Date parsing warnings for VARCHAR
- FIX 6: Row count estimate, not full COUNT(*)
- FIX 7: Renamed to duplicate_entity_count
- FIX 8: Removed "time" from _is_date_like
- FIX 9: Use DB CURRENT_DATE, not datetime.now()
"""

import re
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine


# =============================================================================
# FIX 1: Identifier Validation
# =============================================================================

# Pattern for valid SQL identifiers (Postgres-safe)
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(name: str, context: str = "identifier") -> None:
    """
    Validate that a string is a safe SQL identifier.
    
    Args:
        name: The identifier to validate.
        context: Description for error message (e.g., "table", "column").
        
    Raises:
        ValueError: If identifier is invalid.
    """
    if not name:
        raise ValueError(f"Empty {context} name")
    if len(name) > 128:
        raise ValueError(f"{context} name too long (max 128 chars): {name}")
    if not IDENTIFIER_PATTERN.match(name):
        raise ValueError(
            f"Invalid {context} name: '{name}'. "
            "Must start with letter/underscore, contain only letters/numbers/underscores."
        )


# NOTE: Column existence validation is done in validate_grain() using _get_columns()
# which fetches all columns from information_schema and builds an allowlist.


class GrainDefinition:
    """
    Stores the grain definition for a dataset.
    Grain = what constitutes "one row" in the final dataset.
    """

    def __init__(
        self,
        entity_type: str,
        entity_table: str,
        entity_id_column: str,
        observation_date_column: str,
        observation_date_type: str = "column",
        observation_date_value: str | None = None,
        deduplication_rule: str = "keep_latest",
        dedup_order_by: str | None = None,
        dedup_tiebreaker: str | None = None,
        schema: str = "public",
        
        snapshot_strategy: str = "column",  # "column", "monthly", "weekly", "daily"
        start_date: str | None = None,  # YYYY-MM-DD (for snapshot strategy)
        end_date: str | None = None,  # YYYY-MM-DD (for snapshot strategy)
        min_history_days: int = 30,  # Skip observations with < N days data
        train_end_date: str | None = None,  # YYYY-MM-DD (train/valid boundary)
        valid_end_date: str | None = None,  # YYYY-MM-DD (valid/test boundary)
    ):
        """
        Initialize grain definition.

        Args:
            entity_type: Type of entity (customer, account, transaction, loan)
            entity_table: Table containing entities
            entity_id_column: Column that identifies the entity
            observation_date_column: Column for observation point (required even if fixed, for reference)
            observation_date_type: "column" (use column value) or "fixed" (use fixed date)
            observation_date_value: Fixed date value if observation_date_type="fixed" (format: YYYY-MM-DD)
            deduplication_rule: How to handle duplicates:
                - "keep_first": Keep first occurrence (by order column)
                - "keep_latest": Keep latest occurrence (by order column)
                - "keep_all": Keep all (grain = entity + observation_date)
                - "error": Raise error if duplicates exist
            dedup_order_by: Column to order by for deduplication (default: observation_date_column)
            dedup_tiebreaker: Secondary column for tie-breaking (optional)
            schema: Database schema
            
            
            snapshot_strategy: How to generate observation dates:
                - "column": Use existing column values (default, backward compatible)
                - "monthly": Generate month-end snapshots
                - "weekly": Generate week-end snapshots 
                - "daily": Generate daily snapshots
            start_date: Start of date range for snapshot strategy (YYYY-MM-DD)
            end_date: End of date range for snapshot strategy (YYYY-MM-DD)
            min_history_days: Skip observations where entity has < N days of history
            train_end_date: End date for training set (train <= this date)
            valid_end_date: End date for validation set (valid <= this date, > train_end)
        """
        # FIX 1: Validate all identifiers
        validate_identifier(schema, "schema")
        validate_identifier(entity_table, "table")
        validate_identifier(entity_id_column, "entity_id column")
        validate_identifier(observation_date_column, "observation_date column")
        
        if dedup_order_by:
            validate_identifier(dedup_order_by, "dedup_order_by column")
        if dedup_tiebreaker:
            validate_identifier(dedup_tiebreaker, "dedup_tiebreaker column")
        
        # Validate observation_date_value format if fixed
        if observation_date_type == "fixed":
            if not observation_date_value:
                raise ValueError("observation_date_value required when observation_date_type='fixed'")
            # Basic date format validation (YYYY-MM-DD)
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", observation_date_value):
                raise ValueError(
                    f"Invalid observation_date_value format: '{observation_date_value}'. "
                    "Expected YYYY-MM-DD."
                )
        
        # Validate deduplication_rule
        valid_rules = {"keep_first", "keep_latest", "keep_all", "error"}
        if deduplication_rule not in valid_rules:
            raise ValueError(f"Invalid deduplication_rule: '{deduplication_rule}'. Must be one of {valid_rules}")
        
        
        valid_strategies = {"column", "monthly", "weekly", "daily"}
        if snapshot_strategy not in valid_strategies:
            raise ValueError(f"Invalid snapshot_strategy: '{snapshot_strategy}'. Must be one of {valid_strategies}")
        
        
        if snapshot_strategy in {"monthly", "weekly", "daily"}:
            if not start_date or not end_date:
                raise ValueError(f"start_date and end_date required for snapshot_strategy='{snapshot_strategy}'")
        
        
        date_pattern = r"^\d{4}-\d{2}-\d{2}$"
        for date_val, date_name in [
            (start_date, "start_date"),
            (end_date, "end_date"),
            (train_end_date, "train_end_date"),
            (valid_end_date, "valid_end_date"),
        ]:
            if date_val and not re.match(date_pattern, date_val):
                raise ValueError(f"Invalid {date_name} format: '{date_val}'. Expected YYYY-MM-DD.")
        
        
        if train_end_date and valid_end_date:
            if train_end_date >= valid_end_date:
                raise ValueError(
                    f"train_end_date ({train_end_date}) must be before valid_end_date ({valid_end_date})"
                )
        
        
        if min_history_days < 0:
            raise ValueError(f"min_history_days cannot be negative: {min_history_days}")
        
        self.entity_type = entity_type
        self.entity_table = entity_table
        self.entity_id_column = entity_id_column
        self.observation_date_column = observation_date_column
        self.observation_date_type = observation_date_type
        self.observation_date_value = observation_date_value
        self.deduplication_rule = deduplication_rule
        self.dedup_order_by = dedup_order_by or observation_date_column
        self.dedup_tiebreaker = dedup_tiebreaker
        self.schema = schema
        
        self.snapshot_strategy = snapshot_strategy
        self.start_date = start_date
        self.end_date = end_date
        self.min_history_days = min_history_days
        self.train_end_date = train_end_date
        self.valid_end_date = valid_end_date

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage/serialization."""
        return {
            "entity_type": self.entity_type,
            "entity_table": self.entity_table,
            "entity_id_column": self.entity_id_column,
            "observation_date_column": self.observation_date_column,
            "observation_date_type": self.observation_date_type,
            "observation_date_value": self.observation_date_value,
            "deduplication_rule": self.deduplication_rule,
            "dedup_order_by": self.dedup_order_by,
            "dedup_tiebreaker": self.dedup_tiebreaker,
            "schema": self.schema,
            
            "snapshot_strategy": self.snapshot_strategy,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "min_history_days": self.min_history_days,
            "train_end_date": self.train_end_date,
            "valid_end_date": self.valid_end_date,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GrainDefinition":
        """Create from dictionary."""
        return cls(**data)


class GrainService:
    """
    Service for defining and validating dataset grain.
    Implements 2.1 DEFINE GRAIN.
    """

    @staticmethod
    def validate_temporal_split(
        train_end_date: str | None,
        valid_end_date: str | None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[str]:
        """
        Validate temporal split configuration.
        
        Ensures train < valid < test dates are correctly ordered.
        
        Args:
            train_end_date: End of training period (YYYY-MM-DD)
            valid_end_date: End of validation period (YYYY-MM-DD)
            start_date: Optional start of overall date range
            end_date: Optional end of overall date range
            
        Returns:
            List of warning messages (empty if valid)
        """
        warnings = []
        
        # Check if dates are provided
        if not train_end_date and not valid_end_date:
            warnings.append("No temporal split defined. All data will be in one set.")
            return warnings
        
        if train_end_date and not valid_end_date:
            warnings.append("Only train_end_date set. Consider adding valid_end_date for proper train/valid/test split.")
        
        if valid_end_date and not train_end_date:
            warnings.append("valid_end_date set without train_end_date. Train set will be empty.")
            return warnings
        
        # Validate ordering
        if train_end_date and valid_end_date:
            if train_end_date >= valid_end_date:
                warnings.append(
                    f"INVALID: train_end_date ({train_end_date}) must be before valid_end_date ({valid_end_date})"
                )
        
        # Check against date range if provided
        if start_date and train_end_date:
            if train_end_date <= start_date:
                warnings.append(
                    f"INVALID: train_end_date ({train_end_date}) must be after start_date ({start_date})"
                )
        
        if end_date and valid_end_date:
            if valid_end_date >= end_date:
                warnings.append(
                    f"WARNING: valid_end_date ({valid_end_date}) is at or after end_date ({end_date}). "
                    "Test set may be empty."
                )
        
        return warnings

    @staticmethod
    def validate_grain(
        engine: Engine,
        grain: GrainDefinition,
    ) -> dict[str, Any]:
        """
        Validate grain definition against the database.

        Args:
            engine: SQLAlchemy engine.
            grain: Grain definition to validate.

        Returns:
            Validation result with stats and warnings.
        """
        result = {
            "grain_definition": grain.to_dict(),
            "stats": {},
            "warnings": [],
            "errors": [],
            "status": "valid",
        }

        schema = grain.schema
        table = grain.entity_table
        entity_col = grain.entity_id_column
        obs_col = grain.observation_date_column
        is_fixed_obs = grain.observation_date_type == "fixed"

        with engine.connect() as conn:
            # 1. Validate table exists
            if not GrainService._table_exists(conn, schema, table):
                result["errors"].append(f"Table '{schema}.{table}' does not exist")
                result["status"] = "invalid"
                return result

            # 2. Validate columns exist (using allowlist from DB)
            columns = GrainService._get_columns(conn, schema, table)
            column_names = {c["name"].lower() for c in columns}

            if entity_col.lower() not in column_names:
                result["errors"].append(f"Entity column '{entity_col}' not found in table")
                result["status"] = "invalid"
                return result

            # FIX 2: Only require obs_col if not fixed
            if not is_fixed_obs:
                if obs_col.lower() not in column_names:
                    result["errors"].append(f"Observation date column '{obs_col}' not found in table")
                    result["status"] = "invalid"
                    return result

            # FIX 4: Validate dedup_order_by and dedup_tiebreaker
            if grain.dedup_order_by and grain.dedup_order_by.lower() not in column_names:
                result["errors"].append(f"Dedup order column '{grain.dedup_order_by}' not found in table")
                result["status"] = "invalid"
                return result
            
            if grain.dedup_tiebreaker and grain.dedup_tiebreaker.lower() not in column_names:
                result["errors"].append(f"Dedup tiebreaker column '{grain.dedup_tiebreaker}' not found in table")
                result["status"] = "invalid"
                return result

            # 3. Validate observation column is date-like (if using column)
            if not is_fixed_obs:
                obs_col_info = next((c for c in columns if c["name"].lower() == obs_col.lower()), None)
                if obs_col_info:
                    if not GrainService._is_date_like(obs_col_info["type"]):
                        # Honest warning: Postgres doesn't have TRY_CAST, so we can't
                        # safely check if all values will cast. Warn about runtime risk.
                        result["warnings"].append(
                            f"Observation column '{obs_col}' is {obs_col_info['type']}, not a date type. "
                            "Casting to DATE will be attempted at runtime and may fail for some values. "
                            "Consider providing a date parse format or fixing source data."
                        )

            # 4. Get basic stats
            stats = GrainService._get_grain_stats(conn, grain)
            result["stats"] = stats

            # 5. Check for duplicates
            if stats["duplicate_entity_count"] > 0:
                if grain.deduplication_rule == "error":
                    result["errors"].append(
                        f"Found {stats['duplicate_entity_count']} entities with duplicates. "
                        f"Deduplication rule is 'error'."
                    )
                    result["status"] = "invalid"
                elif grain.deduplication_rule == "keep_all":
                    result["warnings"].append(
                        f"Found {stats['duplicate_entity_count']} entities with multiple observations. "
                        f"Grain will be entity + observation_date."
                    )
                else:
                    result["warnings"].append(
                        f"Found {stats['duplicate_entity_count']} entities with duplicates. "
                        f"Will apply '{grain.deduplication_rule}' rule."
                    )

            # 6. Check for NULL entity IDs
            if stats.get("null_entity_count", 0) > 0:
                result["warnings"].append(
                    f"Found {stats['null_entity_count']} rows with NULL entity ID. "
                    "These will be excluded."
                )

            # 7. Check for NULL observation dates (only if using column)
            if not is_fixed_obs and stats.get("null_obs_date_count", 0) > 0:
                total = stats.get("total_rows_estimate", 0)
                is_estimate = stats.get("total_rows_is_estimate", True)
                pct = stats["null_obs_date_count"] / total * 100 if total > 0 else 0
                if pct > 10:
                    # Use ~ to indicate approximate percentage when total is estimated
                    pct_str = f"~{pct:.1f}%" if is_estimate else f"{pct:.1f}%"
                    result["warnings"].append(
                        f"Found {stats['null_obs_date_count']} rows ({pct_str}) with NULL observation date. "
                        "These will be excluded."
                    )

            # FIX 9: Check observation date range using DB CURRENT_DATE
            if stats.get("obs_date_min") and stats.get("obs_date_max"):
                days_old = stats.get("days_since_max_obs", 0)
                if days_old and days_old > 90:
                    result["warnings"].append(
                        f"Most recent observation is {days_old} days old ({stats['obs_date_max'][:10]})"
                    )

            # Update status based on warnings
            if result["warnings"] and result["status"] == "valid":
                result["status"] = "warning"

        return result

    @staticmethod
    def _table_exists(conn, schema: str, table: str) -> bool:
        """Check if table exists."""
        result = conn.execute(
            text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = :schema AND table_name = :table
                )
            """),
            {"schema": schema, "table": table},
        )
        return result.scalar()

    @staticmethod
    def _get_columns(conn, schema: str, table: str) -> list[dict]:
        """Get column information for a table."""
        result = conn.execute(
            text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table
            """),
            {"schema": schema, "table": table},
        )
        return [{"name": row[0], "type": row[1]} for row in result.fetchall()]

    @staticmethod
    def _is_date_like(data_type: str) -> bool:
        """
        Check if data type is date-like.
        FIX 8: Removed "time" - it has no date component.
        """
        date_types = ["date", "timestamp", "timestamptz", "timestamp with time zone", "timestamp without time zone"]
        return any(dt in data_type.lower() for dt in date_types)

    # NOTE: _check_date_cast removed - Postgres doesn't have TRY_CAST, and sampling
    # only a few rows gives false confidence. Instead, we warn honestly that
    # non-date columns may fail at runtime. The warning is issued in validate_grain().

    @staticmethod
    def _get_grain_stats(conn, grain: GrainDefinition) -> dict[str, Any]:
        """
        Get statistics about the grain.
        
        FIX 6: Uses row count estimate for total_rows.
        FIX 7: Renamed duplicate_count to duplicate_entity_count.
        FIX 9: Uses DB CURRENT_DATE for days_since calculation.
        """
        schema = grain.schema
        table = grain.entity_table
        entity_col = grain.entity_id_column
        obs_col = grain.observation_date_column
        is_fixed_obs = grain.observation_date_type == "fixed"

        stats = {}

        # Use estimate for total rows (fast) - clearly named
        result = conn.execute(
            text("""
                SELECT COALESCE(
                    (SELECT reltuples::BIGINT FROM pg_class 
                     WHERE relname = :table 
                       AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = :schema)),
                    0
                )
            """),
            {"schema": schema, "table": table},
        )
        estimate = result.scalar() or 0
        
        # If estimate is 0 or negative (table just created / never analyzed), do actual count
        if estimate <= 0:
            result = conn.execute(
                text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            )
            stats["total_rows_estimate"] = result.scalar() or 0
            stats["total_rows_is_estimate"] = False  # Exact count
        else:
            stats["total_rows_estimate"] = int(estimate)
            stats["total_rows_is_estimate"] = True  # From pg_class reltuples

        if stats["total_rows_estimate"] == 0:
            return stats

        # Unique entities (exact count - needed for grain validation)
        result = conn.execute(
            text(f'''
                SELECT COUNT(DISTINCT "{entity_col}") 
                FROM "{schema}"."{table}"
                WHERE "{entity_col}" IS NOT NULL
            ''')
        )
        stats["unique_entities"] = result.scalar() or 0

        # Duplicate entity count (entities appearing more than once)
        result = conn.execute(
            text(f'''
                SELECT COUNT(*) FROM (
                    SELECT "{entity_col}"
                    FROM "{schema}"."{table}"
                    WHERE "{entity_col}" IS NOT NULL
                    GROUP BY "{entity_col}"
                    HAVING COUNT(*) > 1
                ) duplicates
            ''')
        )
        stats["duplicate_entity_count"] = result.scalar() or 0

        # For keep_all: also check (entity_id, observation_date) duplicates
        if grain.deduplication_rule == "keep_all" and not is_fixed_obs:
            result = conn.execute(
                text(f'''
                    SELECT COUNT(*) FROM (
                        SELECT "{entity_col}", "{obs_col}"::DATE
                        FROM "{schema}"."{table}"
                        WHERE "{entity_col}" IS NOT NULL AND "{obs_col}" IS NOT NULL
                        GROUP BY "{entity_col}", "{obs_col}"::DATE
                        HAVING COUNT(*) > 1
                    ) pair_duplicates
                ''')
            )
            stats["duplicate_entity_obs_count"] = result.scalar() or 0
        else:
            stats["duplicate_entity_obs_count"] = 0

        # NULL entity count
        result = conn.execute(
            text(f'''
                SELECT COUNT(*) 
                FROM "{schema}"."{table}"
                WHERE "{entity_col}" IS NULL
            ''')
        )
        stats["null_entity_count"] = result.scalar() or 0

        # NULL observation date count (only if using column)
        if not is_fixed_obs:
            result = conn.execute(
                text(f'''
                    SELECT COUNT(*) 
                    FROM "{schema}"."{table}"
                    WHERE "{obs_col}" IS NULL
                ''')
            )
            stats["null_obs_date_count"] = result.scalar() or 0
        else:
            stats["null_obs_date_count"] = 0

        # FIX 9: Observation date range with DB CURRENT_DATE
        if not is_fixed_obs:
            try:
                result = conn.execute(
                    text(f'''
                        SELECT 
                            MIN("{obs_col}")::TEXT,
                            MAX("{obs_col}")::TEXT,
                            CURRENT_DATE - MAX("{obs_col}")::DATE AS days_since_max
                        FROM "{schema}"."{table}"
                        WHERE "{obs_col}" IS NOT NULL
                    ''')
                )
                row = result.fetchone()
                if row:
                    stats["obs_date_min"] = row[0]
                    stats["obs_date_max"] = row[1]
                    stats["days_since_max_obs"] = row[2] if row[2] else None
            except Exception:
                stats["obs_date_min"] = None
                stats["obs_date_max"] = None
                stats["days_since_max_obs"] = None
        else:
            stats["obs_date_min"] = grain.observation_date_value
            stats["obs_date_max"] = grain.observation_date_value
            stats["days_since_max_obs"] = None

        return stats

    @staticmethod
    def generate_grain_sql(grain: GrainDefinition, include_split: bool = False) -> str:
        """
        Generate SQL for the grain (entity + observation date).

        This SQL serves as the base for all subsequent joins.
        It handles deduplication according to the specified rule.
        
        enhancements:
        - snapshot_strategy: Generate periodic snapshots (monthly/weekly/daily)
        - include_split: Add train/valid/test column based on date boundaries
        - min_history_days: Filter out entities with insufficient history
        
        FIX 2: Fixed obs_date doesn't filter on obs_col.
        FIX 3: Clean SQL construction (no split/replace hacks).

        Returns:
            SQL query that produces unique entity + observation_date rows.
        """
        schema = grain.schema
        table = grain.entity_table
        entity_col = grain.entity_id_column
        obs_col = grain.observation_date_column
        order_col = grain.dedup_order_by
        tiebreaker = grain.dedup_tiebreaker
        is_fixed_obs = grain.observation_date_type == "fixed"
        fixed_date_value = grain.observation_date_value
        
        
        snapshot_strategy = grain.snapshot_strategy
        start_date = grain.start_date
        end_date = grain.end_date
        train_end_date = grain.train_end_date
        valid_end_date = grain.valid_end_date

        # Build ORDER BY clause
        order_parts = [f'"{order_col}"']
        if tiebreaker:
            order_parts.append(f'"{tiebreaker}"')
        order_clause = ", ".join(order_parts)

        # FIX 2 & 3: Build observation date expression cleanly
        if is_fixed_obs:
            obs_date_expr = f"'{fixed_date_value}'::DATE"
            null_filter = f'"{entity_col}" IS NOT NULL'
        else:
            obs_date_expr = f'"{obs_col}"::DATE'
            null_filter = f'"{entity_col}" IS NOT NULL AND "{obs_col}" IS NOT NULL'
        
        
        split_expr = ""
        if include_split and train_end_date and valid_end_date:
            split_expr = f""",
    CASE 
        WHEN observation_date <= '{train_end_date}'::DATE THEN 'train'
        WHEN observation_date <= '{valid_end_date}'::DATE THEN 'valid'
        ELSE 'test'
    END AS split"""
        elif include_split and train_end_date:
            split_expr = f""",
    CASE 
        WHEN observation_date <= '{train_end_date}'::DATE THEN 'train'
        ELSE 'test'
    END AS split"""
        
        
        if snapshot_strategy in {"monthly", "weekly", "daily"}:
            # Generate periodic snapshots using generate_series
            return GrainService._generate_snapshot_sql(
                grain, snapshot_strategy, start_date, end_date, 
                split_expr, include_split
            )
        
        # Original column-based strategy
        if grain.deduplication_rule == "keep_all":
            # Keep all rows, grain = entity + observation_date
            sql = f'''
SELECT 
    "{entity_col}" AS entity_id,
    {obs_date_expr} AS observation_date{split_expr if include_split else ""}
FROM "{schema}"."{table}"
WHERE {null_filter}
'''
        elif grain.deduplication_rule == "keep_latest":
            # FIX 3: Clean ranked CTE with proper aliasing
            sql = f'''
WITH ranked AS (
    SELECT 
        "{entity_col}" AS entity_id,
        {obs_date_expr} AS observation_date,
        ROW_NUMBER() OVER (
            PARTITION BY "{entity_col}" 
            ORDER BY {order_clause} DESC
        ) AS rn
    FROM "{schema}"."{table}"
    WHERE {null_filter}
)
SELECT entity_id, observation_date{split_expr if include_split else ""}
FROM ranked
WHERE rn = 1
'''
        elif grain.deduplication_rule == "keep_first":
            # FIX 3: Clean ranked CTE with proper aliasing
            sql = f'''
WITH ranked AS (
    SELECT 
        "{entity_col}" AS entity_id,
        {obs_date_expr} AS observation_date,
        ROW_NUMBER() OVER (
            PARTITION BY "{entity_col}" 
            ORDER BY {order_clause} ASC
        ) AS rn
    FROM "{schema}"."{table}"
    WHERE {null_filter}
)
SELECT entity_id, observation_date{split_expr if include_split else ""}
FROM ranked
WHERE rn = 1
'''
        else:
            # "error" rule: no deduplication, expect unique
            sql = f'''
SELECT 
    "{entity_col}" AS entity_id,
    {obs_date_expr} AS observation_date{split_expr if include_split else ""}
FROM "{schema}"."{table}"
WHERE {null_filter}
'''

        return sql.strip()
    
    @staticmethod
    def _generate_snapshot_sql(
        grain: GrainDefinition,
        strategy: str,
        start_date: str,
        end_date: str,
        split_expr: str,
        include_split: bool,
    ) -> str:
        """
        Generate SQL for periodic snapshot strategy.
        
        Creates a cross join between entities and generated dates.
        
        Args:
            grain: Grain definition
            strategy: "monthly", "weekly", or "daily"
            start_date: Start of date range
            end_date: End of date range
            split_expr: SQL for split column (or empty)
            include_split: Whether to include split column
            
        Returns:
            SQL for snapshot-based grain
        """
        schema = grain.schema
        table = grain.entity_table
        entity_col = grain.entity_id_column
        obs_col = grain.observation_date_column
        min_history = grain.min_history_days
        
        # Interval for generate_series
        interval_map = {
            "monthly": "1 month",
            "weekly": "1 week",
            "daily": "1 day",
        }
        interval = interval_map[strategy]
        
        # Date truncation for month-end/week-end
        if strategy == "monthly":
            # Generate month-end dates
            date_expr = "DATE_TRUNC('month', d) + INTERVAL '1 month' - INTERVAL '1 day'"
        elif strategy == "weekly":
            # Generate week-end dates (Sunday)
            date_expr = "DATE_TRUNC('week', d) + INTERVAL '6 days'"
        else:
            # Daily - just use the date
            date_expr = "d::DATE"
        
        # Build the SQL
        sql = f'''
WITH 
-- Generate snapshot dates
snapshot_dates AS (
    SELECT DISTINCT {date_expr} AS observation_date
    FROM generate_series(
        '{start_date}'::DATE,
        '{end_date}'::DATE,
        '{interval}'::INTERVAL
    ) AS d
    WHERE {date_expr} <= '{end_date}'::DATE
),
-- Get distinct entities with their first activity date
entities AS (
    SELECT 
        "{entity_col}" AS entity_id,
        MIN("{obs_col}")::DATE AS first_activity_date
    FROM "{schema}"."{table}"
    WHERE "{entity_col}" IS NOT NULL
    GROUP BY "{entity_col}"
),
-- Cross join to create all possible entity + date combinations
grain_raw AS (
    SELECT 
        e.entity_id,
        s.observation_date,
        e.first_activity_date
    FROM entities e
    CROSS JOIN snapshot_dates s
    -- Only include dates after entity has sufficient history
    WHERE s.observation_date >= e.first_activity_date + INTERVAL '{min_history} days'
)
SELECT 
    entity_id,
    observation_date{split_expr if include_split else ""}
FROM grain_raw
'''
        
        return sql.strip()

    @staticmethod
    def preview_grain(
        engine: Engine,
        grain: GrainDefinition,
        limit: int = 100,
        include_split: bool = False,
    ) -> dict[str, Any]:
        """
        Preview the grain (first N rows after applying grain logic).

        Args:
            engine: SQLAlchemy engine.
            grain: Grain definition.
            limit: Number of rows to preview.

        Returns:
            Preview data with columns and rows.
        """
        sql = GrainService.generate_grain_sql(grain, include_split=include_split)
        sql_with_limit = f"{sql}\nLIMIT {limit}"

        with engine.connect() as conn:
            result = conn.execute(text(sql_with_limit))
            rows = result.fetchall()
            columns = list(result.keys())

        return {
            "columns": columns,
            "rows": [dict(zip(columns, row)) for row in rows],
            "row_count": len(rows),
            "sql": sql,
        }



grain_service = GrainService()
