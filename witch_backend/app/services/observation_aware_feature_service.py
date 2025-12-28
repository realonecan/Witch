"""
Observation-Aware Feature Service


This service generates feature SQL that:
- Outputs: entity_id, observation_date, feature_columns, max_source_time
- Enforces time rule: event_time::DATE <= observation_date (no future data)
- Always groups by (entity_id, observation_date) for join contract

Available templates:
- Rolling count (last N days)
- Rolling sum/avg (last N days)
- Recency (days since last event)
- Distinct count (unique values in N days)
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

from app.services.grain_service import GrainDefinition, GrainService, validate_identifier


# =============================================================================
# Feature Template Types
# =============================================================================


class FeatureTemplateType(str, Enum):
    """Available feature template types."""
    # Count-based
    ROLLING_COUNT = "rolling_count"
    DISTINCT_COUNT = "distinct_count"
    # Numeric aggregations
    ROLLING_SUM = "rolling_sum"
    ROLLING_AVG = "rolling_avg"
    ROLLING_MIN = "rolling_min"
    ROLLING_MAX = "rolling_max"
    ROLLING_STDDEV = "rolling_stddev"
    # Categorical
    MODE = "mode"  # Most frequent value
    # Boolean
    PCT_TRUE = "pct_true"  # Percentage true
    # Time-based
    RECENCY = "recency"


# =============================================================================
# Feature Definition
# =============================================================================


@dataclass
class FeatureDefinition:
    """
    Definition for generating a time-aware feature.
    
    Attributes:
        name: Human-readable feature name (metadata only)
        key: SQL-safe identifier used in column names (validated)
        template_type: Type of feature template to use
        source_table: Source table containing events
        source_schema: Schema of source table
        join_column: Column to join with entity_id
        time_column: Event timestamp column
        value_column: Column to aggregate (for sum/avg/distinct)
        window_days: Rolling window in days (for count/sum/avg/distinct)
    """
    name: str  # Human-readable, can have spaces/special chars
    key: str   # SQL-safe identifier for column names
    template_type: FeatureTemplateType
    source_table: str
    join_column: str
    time_column: str
    value_column: Optional[str] = None
    window_days: int = 30
    source_schema: str = "public"
    
    def __post_init__(self):
        """Validate feature definition."""
        # key must be SQL-safe identifier (used in column names)
        validate_identifier(self.key, "key")
        
        # Validate table/column identifiers
        validate_identifier(self.source_table, "source_table")
        validate_identifier(self.join_column, "join_column")
        validate_identifier(self.time_column, "time_column")
        validate_identifier(self.source_schema, "source_schema")
        if self.value_column:
            validate_identifier(self.value_column, "value_column")
        
        # Validate window
        if self.window_days < 1:
            raise ValueError("window_days must be >= 1")
        
        # Validate value_column required for sum/avg/distinct
        if self.template_type in (
            FeatureTemplateType.ROLLING_SUM, 
            FeatureTemplateType.ROLLING_AVG,
            FeatureTemplateType.DISTINCT_COUNT
        ):
            if not self.value_column:
                raise ValueError(f"{self.template_type.value} requires value_column")


# =============================================================================
# Observation-Aware Feature Service
# =============================================================================


class ObservationAwareFeatureService:
    """
    Generates observation-date aware feature SQL.
    
    All features enforce:
    - Time constraint: event_time::DATE <= observation_date
    - Output contract: entity_id, observation_date, feature_columns, max_source_time
    - Join contract: GROUP BY entity_id, observation_date
    """

    @staticmethod
    def generate_feature_sql(
        feature: FeatureDefinition,
        grain: GrainDefinition,
        include_grain_cte: bool = True,
    ) -> dict[str, Any]:
        """
        Generate observation-date aware feature SQL.
        
        Args:
            feature: Feature definition
            grain: Grain definition for joining
            include_grain_cte: If True, include WITH grain AS (...). 
                               If False, assume grain CTE exists (for assembler)
            
        Returns:
            Dict with sql, feature_columns, max_source_time_column
        """
        # Build common parts
        schema = feature.source_schema
        table = feature.source_table
        join_col = feature.join_column
        time_col = feature.time_column
        window = feature.window_days
        
        # Get grain SQL only if including CTE
        grain_sql = None
        if include_grain_cte:
            grain_sql = GrainService.generate_grain_sql(grain).strip().rstrip(";")
        
        # Generate based on template type
        if feature.template_type == FeatureTemplateType.ROLLING_COUNT:
            return ObservationAwareFeatureService._rolling_count(
                feature, grain_sql, schema, table, join_col, time_col, window, include_grain_cte
            )
        elif feature.template_type == FeatureTemplateType.ROLLING_SUM:
            return ObservationAwareFeatureService._rolling_sum(
                feature, grain_sql, schema, table, join_col, time_col, 
                feature.value_column, window, include_grain_cte
            )
        elif feature.template_type == FeatureTemplateType.ROLLING_AVG:
            return ObservationAwareFeatureService._rolling_avg(
                feature, grain_sql, schema, table, join_col, time_col,
                feature.value_column, window, include_grain_cte
            )
        elif feature.template_type == FeatureTemplateType.ROLLING_MIN:
            return ObservationAwareFeatureService._rolling_min(
                feature, grain_sql, schema, table, join_col, time_col,
                feature.value_column, window, include_grain_cte
            )
        elif feature.template_type == FeatureTemplateType.ROLLING_MAX:
            return ObservationAwareFeatureService._rolling_max(
                feature, grain_sql, schema, table, join_col, time_col,
                feature.value_column, window, include_grain_cte
            )
        elif feature.template_type == FeatureTemplateType.ROLLING_STDDEV:
            return ObservationAwareFeatureService._rolling_stddev(
                feature, grain_sql, schema, table, join_col, time_col,
                feature.value_column, window, include_grain_cte
            )
        elif feature.template_type == FeatureTemplateType.MODE:
            return ObservationAwareFeatureService._mode(
                feature, grain_sql, schema, table, join_col, time_col,
                feature.value_column, window, include_grain_cte
            )
        elif feature.template_type == FeatureTemplateType.PCT_TRUE:
            return ObservationAwareFeatureService._pct_true(
                feature, grain_sql, schema, table, join_col, time_col,
                feature.value_column, window, include_grain_cte
            )
        elif feature.template_type == FeatureTemplateType.RECENCY:
            return ObservationAwareFeatureService._recency(
                feature, grain_sql, schema, table, join_col, time_col, include_grain_cte
            )
        elif feature.template_type == FeatureTemplateType.DISTINCT_COUNT:
            return ObservationAwareFeatureService._distinct_count(
                feature, grain_sql, schema, table, join_col, time_col,
                feature.value_column, window, include_grain_cte
            )
        else:
            raise ValueError(f"Unknown template type: {feature.template_type}")

    @staticmethod
    def _build_sql(
        feature_name: str,
        template_desc: str,
        time_col: str,
        grain_sql: Optional[str],
        select_body: str,
        include_grain_cte: bool,
    ) -> str:
        """Build SQL with or without grain CTE."""
        if include_grain_cte:
            return f'''
-- Feature: {feature_name} ({template_desc})
-- Time rule: {time_col}::DATE <= observation_date
WITH grain AS (
    {grain_sql}
)
{select_body}
'''.strip()
        else:
            # Embedded mode: assume grain CTE already exists
            return f'''
-- Feature: {feature_name} ({template_desc})
-- Time rule: {time_col}::DATE <= observation_date
{select_body}
'''.strip()

    @staticmethod
    def _rolling_count(
        feature: FeatureDefinition,
        grain_sql: Optional[str],
        schema: str,
        table: str,
        join_col: str,
        time_col: str,
        window: int,
        include_grain_cte: bool,
    ) -> dict[str, Any]:
        """Rolling count (last N days)."""
        col_name = f"cnt_{feature.key}_{window}d"
        validate_identifier(col_name, "generated column name")
        
        select_body = f'''SELECT
    g.entity_id,
    g.observation_date,
    COUNT(e."{join_col}") AS {col_name},
    MAX(e."{time_col}") AS max_source_time
FROM grain g
LEFT JOIN "{schema}"."{table}" e
    ON e."{join_col}" = g.entity_id
   AND e."{time_col}"::DATE <= g.observation_date
   AND e."{time_col}"::DATE > g.observation_date - INTERVAL '{window} days'
GROUP BY g.entity_id, g.observation_date'''

        sql = ObservationAwareFeatureService._build_sql(
            feature.name, f"rolling count, {window} days", time_col, 
            grain_sql, select_body, include_grain_cte
        )
        
        return {
            "sql": sql,
            "feature_columns": [col_name],
            "max_source_time_column": "max_source_time",
            "window_description": f"Count of events in last {window} days",
        }

    @staticmethod
    def _rolling_sum(
        feature: FeatureDefinition,
        grain_sql: Optional[str],
        schema: str,
        table: str,
        join_col: str,
        time_col: str,
        value_col: str,
        window: int,
        include_grain_cte: bool,
    ) -> dict[str, Any]:
        """Rolling sum (last N days)."""
        col_name = f"sum_{feature.key}_{window}d"
        validate_identifier(col_name, "generated column name")
        
        select_body = f'''SELECT
    g.entity_id,
    g.observation_date,
    COALESCE(SUM(e."{value_col}"), 0) AS {col_name},
    MAX(e."{time_col}") AS max_source_time
FROM grain g
LEFT JOIN "{schema}"."{table}" e
    ON e."{join_col}" = g.entity_id
   AND e."{time_col}"::DATE <= g.observation_date
   AND e."{time_col}"::DATE > g.observation_date - INTERVAL '{window} days'
GROUP BY g.entity_id, g.observation_date'''

        sql = ObservationAwareFeatureService._build_sql(
            feature.name, f"rolling sum, {window} days", time_col,
            grain_sql, select_body, include_grain_cte
        )
        
        return {
            "sql": sql,
            "feature_columns": [col_name],
            "max_source_time_column": "max_source_time",
            "window_description": f"Sum of {value_col} in last {window} days",
        }

    @staticmethod
    def _rolling_avg(
        feature: FeatureDefinition,
        grain_sql: Optional[str],
        schema: str,
        table: str,
        join_col: str,
        time_col: str,
        value_col: str,
        window: int,
        include_grain_cte: bool,
    ) -> dict[str, Any]:
        """Rolling average (last N days)."""
        col_name = f"avg_{feature.key}_{window}d"
        validate_identifier(col_name, "generated column name")
        
        select_body = f'''SELECT
    g.entity_id,
    g.observation_date,
    AVG(e."{value_col}") AS {col_name},
    MAX(e."{time_col}") AS max_source_time
FROM grain g
LEFT JOIN "{schema}"."{table}" e
    ON e."{join_col}" = g.entity_id
   AND e."{time_col}"::DATE <= g.observation_date
   AND e."{time_col}"::DATE > g.observation_date - INTERVAL '{window} days'
GROUP BY g.entity_id, g.observation_date'''

        sql = ObservationAwareFeatureService._build_sql(
            feature.name, f"rolling avg, {window} days", time_col,
            grain_sql, select_body, include_grain_cte
        )
        
        return {
            "sql": sql,
            "feature_columns": [col_name],
            "max_source_time_column": "max_source_time",
            "window_description": f"Avg of {value_col} in last {window} days",
        }

    @staticmethod
    def _recency(
        feature: FeatureDefinition,
        grain_sql: Optional[str],
        schema: str,
        table: str,
        join_col: str,
        time_col: str,
        include_grain_cte: bool,
    ) -> dict[str, Any]:
        """Recency (days since last event)."""
        col_name = f"days_since_{feature.key}"
        validate_identifier(col_name, "generated column name")
        
        select_body = f'''SELECT
    g.entity_id,
    g.observation_date,
    (g.observation_date - MAX(e."{time_col}"::DATE)) AS {col_name},
    MAX(e."{time_col}") AS max_source_time
FROM grain g
LEFT JOIN "{schema}"."{table}" e
    ON e."{join_col}" = g.entity_id
   AND e."{time_col}"::DATE <= g.observation_date
GROUP BY g.entity_id, g.observation_date'''

        sql = ObservationAwareFeatureService._build_sql(
            feature.name, "recency - days since last event", time_col,
            grain_sql, select_body, include_grain_cte
        )
        
        return {
            "sql": sql,
            "feature_columns": [col_name],
            "max_source_time_column": "max_source_time",
            "window_description": "Days since last event (NULL if no events)",
        }

    @staticmethod
    def _distinct_count(
        feature: FeatureDefinition,
        grain_sql: Optional[str],
        schema: str,
        table: str,
        join_col: str,
        time_col: str,
        value_col: str,
        window: int,
        include_grain_cte: bool,
    ) -> dict[str, Any]:
        """Distinct count (unique values in N days)."""
        col_name = f"uniq_{feature.key}_{window}d"
        validate_identifier(col_name, "generated column name")
        
        select_body = f'''SELECT
    g.entity_id,
    g.observation_date,
    COUNT(DISTINCT e."{value_col}") AS {col_name},
    MAX(e."{time_col}") AS max_source_time
FROM grain g
LEFT JOIN "{schema}"."{table}" e
    ON e."{join_col}" = g.entity_id
   AND e."{time_col}"::DATE <= g.observation_date
   AND e."{time_col}"::DATE > g.observation_date - INTERVAL '{window} days'
GROUP BY g.entity_id, g.observation_date'''

        sql = ObservationAwareFeatureService._build_sql(
            feature.name, f"distinct count, {window} days", time_col,
            grain_sql, select_body, include_grain_cte
        )
        
        return {
            "sql": sql,
            "feature_columns": [col_name],
            "max_source_time_column": "max_source_time",
            "window_description": f"Unique {value_col} values in last {window} days",
        }

    @staticmethod
    def _rolling_min(
        feature: FeatureDefinition,
        grain_sql: Optional[str],
        schema: str,
        table: str,
        join_col: str,
        time_col: str,
        value_col: str,
        window: int,
        include_grain_cte: bool,
    ) -> dict[str, Any]:
        """Rolling minimum (last N days)."""
        col_name = f"{feature.key}_min_{window}d"
        
        select_body = f'''SELECT 
    g.entity_id,
    g.observation_date,
    MIN(e."{value_col}") AS {col_name},
    MAX(e."{time_col}") AS max_source_time
FROM grain g
LEFT JOIN "{schema}"."{table}" e
    ON e."{join_col}" = g.entity_id
   AND e."{time_col}"::DATE <= g.observation_date
   AND e."{time_col}"::DATE > g.observation_date - INTERVAL '{window} days'
GROUP BY g.entity_id, g.observation_date'''

        sql = ObservationAwareFeatureService._build_sql(
            feature.name, f"rolling min, {window} days", time_col,
            grain_sql, select_body, include_grain_cte
        )
        
        return {
            "sql": sql,
            "feature_columns": [col_name],
            "max_source_time_column": "max_source_time",
            "window_description": f"Min {value_col} in last {window} days",
        }

    @staticmethod
    def _rolling_max(
        feature: FeatureDefinition,
        grain_sql: Optional[str],
        schema: str,
        table: str,
        join_col: str,
        time_col: str,
        value_col: str,
        window: int,
        include_grain_cte: bool,
    ) -> dict[str, Any]:
        """Rolling maximum (last N days)."""
        col_name = f"{feature.key}_max_{window}d"
        
        select_body = f'''SELECT 
    g.entity_id,
    g.observation_date,
    MAX(e."{value_col}") AS {col_name},
    MAX(e."{time_col}") AS max_source_time
FROM grain g
LEFT JOIN "{schema}"."{table}" e
    ON e."{join_col}" = g.entity_id
   AND e."{time_col}"::DATE <= g.observation_date
   AND e."{time_col}"::DATE > g.observation_date - INTERVAL '{window} days'
GROUP BY g.entity_id, g.observation_date'''

        sql = ObservationAwareFeatureService._build_sql(
            feature.name, f"rolling max, {window} days", time_col,
            grain_sql, select_body, include_grain_cte
        )
        
        return {
            "sql": sql,
            "feature_columns": [col_name],
            "max_source_time_column": "max_source_time",
            "window_description": f"Max {value_col} in last {window} days",
        }

    @staticmethod
    def _rolling_stddev(
        feature: FeatureDefinition,
        grain_sql: Optional[str],
        schema: str,
        table: str,
        join_col: str,
        time_col: str,
        value_col: str,
        window: int,
        include_grain_cte: bool,
    ) -> dict[str, Any]:
        """Rolling standard deviation (last N days)."""
        col_name = f"{feature.key}_stddev_{window}d"
        
        select_body = f'''SELECT 
    g.entity_id,
    g.observation_date,
    COALESCE(STDDEV(e."{value_col}"), 0) AS {col_name},
    MAX(e."{time_col}") AS max_source_time
FROM grain g
LEFT JOIN "{schema}"."{table}" e
    ON e."{join_col}" = g.entity_id
   AND e."{time_col}"::DATE <= g.observation_date
   AND e."{time_col}"::DATE > g.observation_date - INTERVAL '{window} days'
GROUP BY g.entity_id, g.observation_date'''

        sql = ObservationAwareFeatureService._build_sql(
            feature.name, f"rolling stddev, {window} days", time_col,
            grain_sql, select_body, include_grain_cte
        )
        
        return {
            "sql": sql,
            "feature_columns": [col_name],
            "max_source_time_column": "max_source_time",
            "window_description": f"Std dev of {value_col} in last {window} days",
        }

    @staticmethod
    def _mode(
        feature: FeatureDefinition,
        grain_sql: Optional[str],
        schema: str,
        table: str,
        join_col: str,
        time_col: str,
        value_col: str,
        window: int,
        include_grain_cte: bool,
    ) -> dict[str, Any]:
        """Mode (most frequent value in last N days)."""
        col_name = f"{feature.key}_mode_{window}d"
        
        # Use MODE() aggregate function (Postgres 9.4+)
        select_body = f'''SELECT 
    g.entity_id,
    g.observation_date,
    MODE() WITHIN GROUP (ORDER BY e."{value_col}") AS {col_name},
    MAX(e."{time_col}") AS max_source_time
FROM grain g
LEFT JOIN "{schema}"."{table}" e
    ON e."{join_col}" = g.entity_id
   AND e."{time_col}"::DATE <= g.observation_date
   AND e."{time_col}"::DATE > g.observation_date - INTERVAL '{window} days'
GROUP BY g.entity_id, g.observation_date'''

        sql = ObservationAwareFeatureService._build_sql(
            feature.name, f"mode, {window} days", time_col,
            grain_sql, select_body, include_grain_cte
        )
        
        return {
            "sql": sql,
            "feature_columns": [col_name],
            "max_source_time_column": "max_source_time",
            "window_description": f"Most frequent {value_col} in last {window} days",
        }

    @staticmethod
    def _pct_true(
        feature: FeatureDefinition,
        grain_sql: Optional[str],
        schema: str,
        table: str,
        join_col: str,
        time_col: str,
        value_col: str,
        window: int,
        include_grain_cte: bool,
    ) -> dict[str, Any]:
        """Percentage true (for boolean columns)."""
        col_name = f"{feature.key}_pct_true_{window}d"
        
        select_body = f'''SELECT 
    g.entity_id,
    g.observation_date,
    COALESCE(
        100.0 * SUM(CASE WHEN e."{value_col}" THEN 1 ELSE 0 END)::FLOAT / 
        NULLIF(COUNT(*), 0), 
        0
    ) AS {col_name},
    MAX(e."{time_col}") AS max_source_time
FROM grain g
LEFT JOIN "{schema}"."{table}" e
    ON e."{join_col}" = g.entity_id
   AND e."{time_col}"::DATE <= g.observation_date
   AND e."{time_col}"::DATE > g.observation_date - INTERVAL '{window} days'
GROUP BY g.entity_id, g.observation_date'''

        sql = ObservationAwareFeatureService._build_sql(
            feature.name, f"pct true, {window} days", time_col,
            grain_sql, select_body, include_grain_cte
        )
        
        return {
            "sql": sql,
            "feature_columns": [col_name],
            "max_source_time_column": "max_source_time",
            "window_description": f"Percentage of {value_col} true in last {window} days",
        }

    @staticmethod
    def list_templates() -> list[dict[str, Any]]:
        """List available feature templates."""
        return [
            {
                "type": FeatureTemplateType.ROLLING_COUNT.value,
                "name": "Rolling Count",
                "description": "Count of events in a rolling time window",
                "requires_value_column": False,
                "requires_window_days": True,
            },
            {
                "type": FeatureTemplateType.ROLLING_SUM.value,
                "name": "Rolling Sum",
                "description": "Sum of a column in a rolling time window",
                "requires_value_column": True,
                "requires_window_days": True,
            },
            {
                "type": FeatureTemplateType.ROLLING_AVG.value,
                "name": "Rolling Average",
                "description": "Average of a column in a rolling time window",
                "requires_value_column": True,
                "requires_window_days": True,
            },
            {
                "type": FeatureTemplateType.ROLLING_MIN.value,
                "name": "Rolling Min",
                "description": "Minimum value in a rolling time window",
                "requires_value_column": True,
                "requires_window_days": True,
            },
            {
                "type": FeatureTemplateType.ROLLING_MAX.value,
                "name": "Rolling Max",
                "description": "Maximum value in a rolling time window",
                "requires_value_column": True,
                "requires_window_days": True,
            },
            {
                "type": FeatureTemplateType.ROLLING_STDDEV.value,
                "name": "Rolling Std Dev",
                "description": "Standard deviation in a rolling time window",
                "requires_value_column": True,
                "requires_window_days": True,
            },
            {
                "type": FeatureTemplateType.DISTINCT_COUNT.value,
                "name": "Distinct Count",
                "description": "Count of unique values in a rolling time window",
                "requires_value_column": True,
                "requires_window_days": True,
            },
            {
                "type": FeatureTemplateType.MODE.value,
                "name": "Mode",
                "description": "Most frequent value in a rolling time window",
                "requires_value_column": True,
                "requires_window_days": True,
            },
            {
                "type": FeatureTemplateType.PCT_TRUE.value,
                "name": "Percent True",
                "description": "Percentage of true values for boolean columns",
                "requires_value_column": True,
                "requires_window_days": True,
            },
            {
                "type": FeatureTemplateType.RECENCY.value,
                "name": "Recency",
                "description": "Days since last event (relative to observation_date)",
                "requires_value_column": False,
                "requires_window_days": False,
            },
        ]



observation_aware_feature_service = ObservationAwareFeatureService()
