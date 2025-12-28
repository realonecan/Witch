"""
Target Service
Time-aware target variable definition for ML datasets.


Two modes:
1. Simple mode (legacy): detect_target_columns, get_column_values, generate_target_from_values
2. Time-aware mode (new): TargetDefinition + TargetService

Time-aware mode integrates with grain (entity_id, observation_date) and applies:
- Time windows: event must happen within N months after observation
- Maturity filter: exclude observations that haven't had time to mature
- Cohort analysis: check target stability over time
"""

import re
from datetime import date
from typing import Any, Literal, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.services.grain_service import GrainDefinition, validate_identifier


# =============================================================================
# Time-Aware Target Definition 
# =============================================================================


class TargetDefinition:
    """
    Stores the target definition for a dataset.
    
    Defines what event we're predicting and the time window for observation.
    """

    def __init__(
        self,
        # Table and columns
        label_table: str,
        label_join_column: str,  # Column to join to grain's entity_id
        label_event_column: str,  # Column with event status (e.g., "state_name")
        label_event_time_column: str,  # Column with event timestamp (e.g., "date_close")
        positive_values: list[str],  # Values meaning "positive" (e.g., ["Closed"])
        # Time window config
        window_type: Literal["fixed", "variable"] = "fixed",
        window_months: int = 12,  # For fixed: months after observation
        window_end_column: Optional[str] = None,  # For variable: end time column
        # Maturity and reproducibility
        maturity_months: int = 0,  # Wait period after window ends
        extraction_date: Optional[str] = None,  # Fixed date for reproducibility (YYYY-MM-DD)
        # Optional
        target_name: Optional[str] = None,
        schema: str = "public",
    ):
        """
        Initialize target definition.

        Args:
            label_table: Table containing events (can be different from entity table)
            label_join_column: Column in label_table to join with grain's entity_id
            label_event_column: Column containing event status values
            label_event_time_column: Column with event timestamp
            positive_values: List of values that mean "positive class" (target=1)
            window_type: "fixed" (N months after observation) or "variable" (until end column)
            window_months: For fixed window, months after observation_date
            window_end_column: For variable window, column marking end time
            maturity_months: Months to wait after window ends before labeling
            extraction_date: Fixed date for reproducibility (default: CURRENT_DATE)
            target_name: Name for the target variable
            schema: Database schema
        """
        # Validate identifiers (SQL injection prevention)
        validate_identifier(schema, "schema")
        validate_identifier(label_table, "label_table")
        validate_identifier(label_join_column, "label_join_column")
        validate_identifier(label_event_column, "label_event_column")
        validate_identifier(label_event_time_column, "label_event_time_column")
        if window_end_column:
            validate_identifier(window_end_column, "window_end_column")

        # Validate positive_values
        if not positive_values:
            raise ValueError("positive_values cannot be empty")
        
        # Validate window config
        if window_type not in ("fixed", "variable"):
            raise ValueError(f"window_type must be 'fixed' or 'variable', got '{window_type}'")
        if window_type == "fixed" and window_months <= 0:
            raise ValueError(f"window_months must be > 0 for fixed window, got {window_months}")
        if window_type == "variable" and not window_end_column:
            raise ValueError("window_end_column required for variable window type")
        
        # Validate maturity
        if maturity_months < 0:
            raise ValueError(f"maturity_months cannot be negative, got {maturity_months}")
        
        # Validate extraction_date format
        if extraction_date:
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", extraction_date):
                raise ValueError(
                    f"Invalid extraction_date format: '{extraction_date}'. "
                    "Expected YYYY-MM-DD."
                )

        self.label_table = label_table
        self.label_join_column = label_join_column
        self.label_event_column = label_event_column
        self.label_event_time_column = label_event_time_column
        self.positive_values = positive_values
        self.window_type = window_type
        self.window_months = window_months
        self.window_end_column = window_end_column
        self.maturity_months = maturity_months
        self.extraction_date = extraction_date
        self.target_name = target_name or "target"
        self.schema = schema

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage/serialization."""
        return {
            "label_table": self.label_table,
            "label_join_column": self.label_join_column,
            "label_event_column": self.label_event_column,
            "label_event_time_column": self.label_event_time_column,
            "positive_values": self.positive_values,
            "window_type": self.window_type,
            "window_months": self.window_months,
            "window_end_column": self.window_end_column,
            "maturity_months": self.maturity_months,
            "extraction_date": self.extraction_date,
            "target_name": self.target_name,
            "schema": self.schema,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TargetDefinition":
        """Create from dictionary."""
        return cls(**data)


class TargetService:
    """
    Service for time-aware target definition.
    Implements 2.3 DEFINE TARGET.
    """

    # Date-like SQL types
    DATE_TYPES = ["date", "timestamp", "timestamptz", "timestamp with time zone", "timestamp without time zone"]

    @staticmethod
    def _is_date_like(data_type: str) -> bool:
        """Check if data type is date-like."""
        return any(dt in data_type.lower() for dt in TargetService.DATE_TYPES)

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
    def validate_target(
        engine: Engine,
        target: TargetDefinition,
        grain: GrainDefinition,
    ) -> dict[str, Any]:
        """
        Validate target definition against the database.

        Args:
            engine: SQLAlchemy engine.
            target: Target definition to validate.
            grain: Grain definition (needed for integration).

        Returns:
            Validation result with stats and warnings.
        """
        result = {
            "target_definition": target.to_dict(),
            "stats": {},
            "warnings": [],
            "errors": [],
            "status": "valid",
        }

        schema = target.schema
        table = target.label_table
        join_col = target.label_join_column
        event_col = target.label_event_column
        event_time_col = target.label_event_time_column

        with engine.connect() as conn:
            # 1. Validate table exists
            if not TargetService._table_exists(conn, schema, table):
                result["errors"].append(f"Label table '{schema}.{table}' does not exist")
                result["status"] = "invalid"
                return result

            # 2. Validate columns exist (using allowlist from DB)
            columns = TargetService._get_columns(conn, schema, table)
            column_names = {c["name"].lower() for c in columns}

            for col, name in [(join_col, "label_join_column"), 
                              (event_col, "label_event_column"),
                              (event_time_col, "label_event_time_column")]:
                if col.lower() not in column_names:
                    result["errors"].append(f"{name} '{col}' not found in table '{table}'")
                    result["status"] = "invalid"

            if result["status"] == "invalid":
                return result

            # 3. Validate event_time column is date-like
            event_time_info = next((c for c in columns if c["name"].lower() == event_time_col.lower()), None)
            if event_time_info and not TargetService._is_date_like(event_time_info["type"]):
                result["warnings"].append(
                    f"Event time column '{event_time_col}' is {event_time_info['type']}, not a date type. "
                    "Casting to DATE will be attempted at runtime."
                )

            # 4. Validate window_end_column exists (for variable window)
            if target.window_type == "variable" and target.window_end_column:
                if target.window_end_column.lower() not in column_names:
                    result["errors"].append(
                        f"window_end_column '{target.window_end_column}' not found in table"
                    )
                    result["status"] = "invalid"
                    return result

            # FIX 3: Validate join type compatibility
            # Check if label_join_column type is compatible with grain entity_id_column
            try:
                # Get grain entity column type
                grain_cols = TargetService._get_columns(conn, grain.schema, grain.entity_table)
                grain_entity_info = next(
                    (c for c in grain_cols if c["name"].lower() == grain.entity_id_column.lower()), 
                    None
                )
                label_join_info = next(
                    (c for c in columns if c["name"].lower() == join_col.lower()), 
                    None
                )
                
                if grain_entity_info and label_join_info:
                    grain_type = grain_entity_info["type"].lower()
                    label_type = label_join_info["type"].lower()
                    
                    # Check for obvious type mismatches
                    numeric_types = {"integer", "bigint", "smallint", "numeric", "int4", "int8"}
                    text_types = {"text", "varchar", "character varying", "char", "uuid"}
                    
                    grain_is_numeric = any(t in grain_type for t in numeric_types)
                    label_is_numeric = any(t in label_type for t in numeric_types)
                    grain_is_text = any(t in grain_type for t in text_types)
                    label_is_text = any(t in label_type for t in text_types)
                    
                    if (grain_is_numeric and label_is_text) or (grain_is_text and label_is_numeric):
                        result["warnings"].append(
                            f"Join type mismatch: grain.{grain.entity_id_column} is {grain_type}, "
                            f"but label.{join_col} is {label_type}. "
                            "This may cause join failures or silent zero-row matches."
                        )
            except Exception as e:
                result["warnings"].append(f"Could not validate join types: {str(e)[:100]}")

            # 5. Check positive_values exist in the data (sampling)
            try:
                sample_sql = f'''
                    SELECT DISTINCT "{event_col}"::TEXT
                    FROM "{schema}"."{table}"
                    WHERE "{event_col}" IS NOT NULL
                    LIMIT 100
                '''
                res = conn.execute(text(sample_sql))
                existing_values = {row[0] for row in res.fetchall()}
                
                missing = [v for v in target.positive_values if v not in existing_values]
                if missing:
                    result["warnings"].append(
                        f"These positive_values were not found in sample: {missing}. "
                        "They may exist in full data or be typos."
                    )
            except Exception as e:
                result["warnings"].append(f"Could not verify positive_values: {str(e)[:100]}")

            # 6. Get basic stats
            try:
                # Count label records
                count_sql = f'SELECT COUNT(*) FROM "{schema}"."{table}"'
                res = conn.execute(text(count_sql))
                result["stats"]["label_table_rows"] = res.scalar() or 0

                # Date range of events
                date_sql = f'''
                    SELECT 
                        MIN("{event_time_col}")::TEXT,
                        MAX("{event_time_col}")::TEXT
                    FROM "{schema}"."{table}"
                    WHERE "{event_time_col}" IS NOT NULL
                '''
                res = conn.execute(text(date_sql))
                row = res.fetchone()
                if row:
                    result["stats"]["event_date_min"] = row[0]
                    result["stats"]["event_date_max"] = row[1]
            except Exception as e:
                result["warnings"].append(f"Could not get stats: {str(e)[:100]}")

            # Update status based on warnings
            if result["warnings"] and result["status"] == "valid":
                result["status"] = "warning"

        return result

    @staticmethod
    def generate_target_sql(
        target: TargetDefinition,
        grain: GrainDefinition,
        grain_sql: Optional[str] = None,
        include_grain_cte: bool = True,
    ) -> str:
        """
        Generate SQL for the target variable.

        Integrates with grain (entity_id, observation_date) and applies:
        - Time window: event must happen within window after observation
        - Maturity filter: exclude observations that haven't matured

        Args:
            target: Target definition.
            grain: Grain definition.
            grain_sql: Optional pre-generated grain SQL. If None, generates from grain.
            include_grain_cte: If False, omits WITH grain AS (...) for embedding in assembler.

        Returns:
            SQL query producing (entity_id, observation_date, target).
        """
        from app.services.grain_service import GrainService
        
        schema = target.schema
        label_table = target.label_table
        join_col = target.label_join_column
        event_col = target.label_event_column
        event_time_col = target.label_event_time_column
        
        # Build positive values condition
        escaped_values = [v.replace("'", "''") for v in target.positive_values]
        if len(escaped_values) == 1:
            values_condition = f'"{event_col}" = \'{escaped_values[0]}\''
        else:
            values_str = "', '".join(escaped_values)
            values_condition = f'"{event_col}" IN (\'{values_str}\')'

        # Build extraction date expression
        if target.extraction_date:
            extraction_expr = f"'{target.extraction_date}'::DATE"
        else:
            extraction_expr = "CURRENT_DATE"

        # Calculate total wait period (window + maturity)
        total_months = target.window_months + target.maturity_months

        # Variable window disabled
        if target.window_type == "variable":
            raise ValueError(
                "Variable window mode is not yet supported. "
                "Use window_type='fixed' with window_months instead."
            )

        # Build label_events CTE
        label_events_sql = f'''label_events AS (
    -- Events from label table
    SELECT 
        "{join_col}" AS entity_id,
        "{event_time_col}"::DATE AS event_date,
        {values_condition} AS is_positive
    FROM "{schema}"."{label_table}"
    WHERE "{event_time_col}" IS NOT NULL
)'''

        # Build target_calc CTE
        target_calc_sql = f'''target_calc AS (
    SELECT 
        g.entity_id,
        g.observation_date,
        -- Target = 1 if any positive event within window after observation
        MAX(CASE 
            WHEN e.is_positive = TRUE
             AND e.event_date > g.observation_date
             AND e.event_date <= g.observation_date + INTERVAL '{target.window_months} months'
            THEN 1 ELSE 0 
        END) AS {target.target_name}
    FROM grain g
    LEFT JOIN label_events e ON g.entity_id = e.entity_id
    -- Maturity filter: only include observations that have had time to mature
    WHERE g.observation_date + INTERVAL '{total_months} months' <= {extraction_expr}
    GROUP BY g.entity_id, g.observation_date
)'''

        # Standalone mode: include grain CTE
        if include_grain_cte:
            if grain_sql is None:
                grain_sql = GrainService.generate_grain_sql(grain)
            
            sql = f'''
-- Target SQL: {target.target_name}
-- Time window: {target.window_months} months after observation_date
-- Maturity: {target.maturity_months} months after window ends
-- Positive values: {target.positive_values}

WITH grain AS (
    {grain_sql}
),
{label_events_sql},
{target_calc_sql}
SELECT entity_id, observation_date, {target.target_name}
FROM target_calc
'''
        else:
            # Embedded mode: return ONLY CTEs for assembler to append
            sql = f'''
-- Target CTEs (grain CTE provided by assembler)
{label_events_sql},
{target_calc_sql}
'''

        return sql.strip()

    @staticmethod
    def get_distribution(
        engine: Engine,
        target: TargetDefinition,
        grain: GrainDefinition,
    ) -> dict[str, Any]:
        """
        Get target distribution with warnings.

        Args:
            engine: SQLAlchemy engine.
            target: Target definition.
            grain: Grain definition.

        Returns:
            Distribution stats with imbalance warnings.
        """
        sql = TargetService.generate_target_sql(target, grain)
        
        distribution_sql = f'''
WITH target_data AS (
    {sql}
)
SELECT 
    {target.target_name},
    COUNT(*) as count
FROM target_data
GROUP BY {target.target_name}
ORDER BY {target.target_name}
'''

        try:
            with engine.connect() as conn:
                result = conn.execute(text(distribution_sql))
                rows = result.fetchall()

            # Calculate stats
            total = sum(row[1] for row in rows)
            distribution = []
            class_0_count = 0
            class_1_count = 0

            for row in rows:
                target_val = int(row[0]) if row[0] is not None else 0
                count = int(row[1])
                pct = round((count / total) * 100, 2) if total > 0 else 0.0
                distribution.append({
                    "value": target_val,
                    "count": count,
                    "percentage": pct,
                })
                if target_val == 0:
                    class_0_count = count
                elif target_val == 1:
                    class_1_count = count

            # Generate warnings
            warnings = []
            is_usable = True
            
            class_1_pct = (class_1_count / total * 100) if total > 0 else 0
            class_0_pct = (class_0_count / total * 100) if total > 0 else 0

            if class_1_pct == 100.0 or class_0_pct == 100.0:
                warnings.append({
                    "severity": "critical",
                    "code": "ZERO_VARIANCE",
                    "message": "Target has NO variance - all records are the same class!",
                })
                is_usable = False
            elif class_1_pct > 95.0 or class_0_pct > 95.0:
                minority_pct = min(class_1_pct, class_0_pct)
                warnings.append({
                    "severity": "high",
                    "code": "EXTREME_IMBALANCE",
                    "message": f"Extreme imbalance: minority class is {minority_pct:.1f}%",
                })
            elif class_1_pct > 80.0 or class_0_pct > 80.0:
                minority_pct = min(class_1_pct, class_0_pct)
                warnings.append({
                    "severity": "medium",
                    "code": "HIGH_IMBALANCE",
                    "message": f"Notable imbalance: minority class is {minority_pct:.1f}%",
                })

            if class_1_count < 100 and class_1_count > 0:
                warnings.append({
                    "severity": "medium",
                    "code": "LOW_POSITIVE_COUNT",
                    "message": f"Only {class_1_count} positive samples",
                })

            if total == 0:
                warnings.append({
                    "severity": "critical",
                    "code": "NO_DATA",
                    "message": "No data after applying maturity filter. Reduce maturity or wait for more data.",
                })
                is_usable = False

            return {
                "target_name": target.target_name,
                "total_samples": total,
                "class_0_count": class_0_count,
                "class_1_count": class_1_count,
                "class_0_pct": round(class_0_pct, 2),
                "class_1_pct": round(class_1_pct, 2),
                "distribution": distribution,
                "warnings": warnings,
                "is_usable": is_usable,
                "status": "success",
            }

        except Exception as e:
            return {
                "target_name": target.target_name,
                "error": str(e),
                "status": "error",
                "is_usable": False,
            }

    @staticmethod
    def get_cohort_analysis(
        engine: Engine,
        target: TargetDefinition,
        grain: GrainDefinition,
        period: Literal["month", "quarter"] = "month",
    ) -> dict[str, Any]:
        """
        Analyze target distribution by time cohort.

        Checks if target rate is stable over time (important for ML).

        Args:
            engine: SQLAlchemy engine.
            target: Target definition.
            grain: Grain definition.
            period: Grouping period ("month" or "quarter").

        Returns:
            Cohort analysis with stability assessment.
        """
        sql = TargetService.generate_target_sql(target, grain)
        
        if period == "month":
            date_trunc = "DATE_TRUNC('month', observation_date)"
        else:
            date_trunc = "DATE_TRUNC('quarter', observation_date)"

        cohort_sql = f'''
WITH target_data AS (
    {sql}
)
SELECT 
    {date_trunc}::DATE AS cohort,
    COUNT(*) as total,
    SUM({target.target_name}) as positive_count,
    ROUND(AVG({target.target_name}::NUMERIC) * 100, 2) as positive_rate
FROM target_data
GROUP BY {date_trunc}
ORDER BY {date_trunc}
'''

        try:
            with engine.connect() as conn:
                result = conn.execute(text(cohort_sql))
                rows = result.fetchall()

            cohorts = []
            rates = []
            for row in rows:
                cohort_date = str(row[0])[:10] if row[0] else "Unknown"
                total = int(row[1])
                positive = int(row[2]) if row[2] else 0
                rate = float(row[3]) if row[3] else 0.0
                cohorts.append({
                    "cohort": cohort_date,
                    "total": total,
                    "positive_count": positive,
                    "positive_rate": rate,
                })
                rates.append(rate)

            # Calculate stability metrics
            if len(rates) >= 2:
                avg_rate = sum(rates) / len(rates)
                variance = sum((r - avg_rate) ** 2 for r in rates) / len(rates)
                std_dev = variance ** 0.5
                coefficient_of_variation = (std_dev / avg_rate * 100) if avg_rate > 0 else 0
            else:
                avg_rate = rates[0] if rates else 0
                std_dev = 0
                coefficient_of_variation = 0

            # Assess stability
            if coefficient_of_variation > 50:
                stability = "unstable"
                stability_message = "Target rate varies significantly over time. Consider time-based features."
            elif coefficient_of_variation > 25:
                stability = "moderate"
                stability_message = "Some variation in target rate over time. Monitor for drift."
            else:
                stability = "stable"
                stability_message = "Target rate is relatively stable over time."

            return {
                "target_name": target.target_name,
                "period": period,
                "cohorts": cohorts,
                "avg_positive_rate": round(avg_rate, 2),
                "std_dev": round(std_dev, 2),
                "coefficient_of_variation": round(coefficient_of_variation, 2),
                "stability": stability,
                "stability_message": stability_message,
                "status": "success",
            }

        except Exception as e:
            return {
                "target_name": target.target_name,
                "error": str(e),
                "status": "error",
            }


# =============================================================================
# Simple Mode (Legacy) - Kept for backward compatibility
# =============================================================================


class TargetEngineer:
    """
    Generates SQL target variables by detecting categorical columns
    and letting users pick positive class values.
    
    THIS IS SIMPLE MODE - for quick demos and non-time-aware targets.
    For production ML, use TargetService with TargetDefinition instead.
    """

    # Patterns that suggest a column might be a status/state indicator
    STATUS_COLUMN_PATTERNS = [
        'status', 'state', 'flag', 'type', 'category',
        'active', 'closed', 'cancel', 'churn', 'default',
        'level', 'tier', 'grade', 'class', 'segment',
    ]

    # Maximum distinct values for a column to be considered categorical
    MAX_CATEGORICAL_DISTINCT = 20

    def detect_target_columns(
        self,
        engine: Engine,
        table_name: str,
        columns: list[dict[str, str]],
        schema: str = "public",
    ) -> list[dict[str, Any]]:
        """
        Detect columns that are likely candidates for target variable definition.
        """
        candidates = []
        categorical_types = ['character varying', 'varchar', 'text', 'char', 'integer', 'smallint']
        
        for col in columns:
            col_name = col.get('name', '')
            col_type = col.get('type', '').lower()
            
            if not any(t in col_type for t in categorical_types):
                continue
            
            col_lower = col_name.lower()
            is_status_like = any(pattern in col_lower for pattern in self.STATUS_COLUMN_PATTERNS)
            
            try:
                # Use parameterized-style but with validated identifiers
                validate_identifier(table_name, "table")
                validate_identifier(col_name, "column")
                validate_identifier(schema, "schema")
                
                count_sql = f'''
                SELECT COUNT(DISTINCT "{col_name}") as distinct_count
                FROM "{schema}"."{table_name}"
                '''
                with engine.connect() as conn:
                    result = conn.execute(text(count_sql))
                    row = result.fetchone()
                    distinct_count = int(row[0]) if row and row[0] else 0
                
                if is_status_like or (0 < distinct_count <= self.MAX_CATEGORICAL_DISTINCT):
                    candidates.append({
                        'column_name': col_name,
                        'column_type': col_type,
                        'distinct_count': distinct_count,
                        'is_status_like': is_status_like,
                        'priority': 1 if is_status_like else 2,
                    })
                    
            except Exception:
                continue
        
        candidates.sort(key=lambda x: (x['priority'], x['distinct_count']))
        return candidates

    def get_column_values(
        self,
        engine: Engine,
        table_name: str,
        column_name: str,
        schema: str = "public",
        limit: int = 50,
        sample_size: int = 100000,
    ) -> dict[str, Any]:
        """Get distinct values and their counts for a column."""
        try:
            validate_identifier(table_name, "table")
            validate_identifier(column_name, "column")
            validate_identifier(schema, "schema")

            sampled = False
            sample_percent = None
            row_estimate = 0

            with engine.connect() as conn:
                try:
                    estimate_result = conn.execute(text("""
                        SELECT COALESCE(c.reltuples::bigint, 0) AS estimate
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = :schema
                          AND c.relname = :table
                    """), {"schema": schema, "table": table_name})
                    estimate_row = estimate_result.fetchone()
                    row_estimate = int(estimate_row[0]) if estimate_row and estimate_row[0] else 0
                except Exception:
                    row_estimate = 0

                use_sample = sample_size > 0 and row_estimate > sample_size
                if use_sample:
                    sampled = True
                    sample_percent = (sample_size / row_estimate) * 100 if row_estimate > 0 else 100.0
                    sample_percent = max(0.1, min(100.0, sample_percent))
                    values_sql = f'''
                    SELECT 
                        "{column_name}"::TEXT as value,
                        COUNT(*) as count
                    FROM (
                        SELECT "{column_name}"
                        FROM "{schema}"."{table_name}"
                        TABLESAMPLE BERNOULLI({sample_percent})
                        LIMIT {int(sample_size)}
                    ) sampled
                    GROUP BY "{column_name}"
                    ORDER BY COUNT(*) DESC
                    LIMIT {int(limit)}
                    '''
                    total_sql = f'''
                    SELECT COUNT(*)
                    FROM (
                        SELECT 1
                        FROM "{schema}"."{table_name}"
                        TABLESAMPLE BERNOULLI({sample_percent})
                        LIMIT {int(sample_size)}
                    ) sampled
                    '''
                else:
                    values_sql = f'''
                    SELECT 
                        "{column_name}"::TEXT as value,
                        COUNT(*) as count
                    FROM "{schema}"."{table_name}"
                    GROUP BY "{column_name}"
                    ORDER BY COUNT(*) DESC
                    LIMIT {int(limit)}
                    '''
                    total_sql = f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'

                result = conn.execute(text(values_sql))
                rows = result.fetchall()

                total_result = conn.execute(text(total_sql))
                total_row = total_result.fetchone()
                total_count = int(total_row[0]) if total_row else 0
            
            values = []
            for row in rows:
                value = row[0] if row[0] is not None else '__NULL__'
                count = int(row[1])
                percentage = round((count / total_count) * 100, 2) if total_count > 0 else 0.0
                values.append({
                    'value': value,
                    'count': count,
                    'percentage': percentage,
                    'is_null': row[0] is None,
                })
            
            return {
                'column_name': column_name,
                'table_name': table_name,
                'total_records': total_count,
                'distinct_count': len(rows),
                'values': values,
                'sampled': sampled,
                'sample_size': sample_size if sampled else None,
                'sample_percent': round(sample_percent, 3) if sampled and sample_percent else None,
                'status': 'success',
            }
            
        except Exception as e:
            return {
                'column_name': column_name,
                'table_name': table_name,
                'error': str(e),
                'status': 'error',
            }

    def generate_target_from_values(
        self,
        column_name: str,
        selected_values: list[str],
        target_name: Optional[str] = None,
        grouping_column: Optional[str] = None,
    ) -> dict[str, Any]:
        """Generate SQL target logic from selected column values."""
        validate_identifier(column_name, "column")
        if grouping_column:
            validate_identifier(grouping_column, "grouping_column")
        
        if not selected_values:
            return {
                'target_name': 'target_undefined',
                'sql_logic': 'CASE WHEN 1=0 THEN 1 ELSE 0 END',
                'description': 'No values selected for positive class.',
                'error': 'No values selected',
            }
        
        has_null = '__NULL__' in selected_values
        non_null_values = [v for v in selected_values if v != '__NULL__']
        
        conditions = []
        
        if non_null_values:
            if len(non_null_values) == 1:
                escaped_value = non_null_values[0].replace("'", "''")
                conditions.append(f'"{column_name}" = \'{escaped_value}\'')
            else:
                escaped_values = [v.replace("'", "''") for v in non_null_values]
                values_str = "', '".join(escaped_values)
                conditions.append(f'"{column_name}" IN (\'{values_str}\')')
        
        if has_null:
            conditions.append(f'"{column_name}" IS NULL')
        
        condition_str = ' OR '.join(conditions)
        
        if grouping_column:
            sql_logic = f'MAX(CASE WHEN {condition_str} THEN 1 ELSE 0 END)'
        else:
            sql_logic = f'CASE WHEN {condition_str} THEN 1 ELSE 0 END'
        
        if not target_name:
            clean_col = column_name.lower().replace(' ', '_')
            if len(non_null_values) == 1:
                clean_val = non_null_values[0].lower()[:20].replace(' ', '_')
                clean_val = ''.join(c for c in clean_val if c.isalnum() or c == '_')
                target_name = f'is_{clean_col}_{clean_val}'
            else:
                target_name = f'is_{clean_col}_positive'
        
        if len(selected_values) == 1:
            desc_values = selected_values[0] if selected_values[0] != '__NULL__' else 'NULL'
            description = f"1 = {column_name} is '{desc_values}', 0 = otherwise"
        else:
            desc_values = ', '.join(v if v != '__NULL__' else 'NULL' for v in selected_values)
            description = f"1 = {column_name} is one of [{desc_values}], 0 = otherwise"
        
        return {
            'target_name': target_name,
            'sql_logic': sql_logic,
            'description': description,
            'column_name': column_name,
            'selected_values': selected_values,
        }



target_service = TargetService()
target_engineer = TargetEngineer()  # Legacy, for backward compatibility
