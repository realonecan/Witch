"""
Missing Value Handling Service


Two layers of handling:
A) SQL-layer defaults (assembler-time) - COALESCE strategies
B) Missingness indicator columns - is_missing_<feature> flags

Available strategies:
- zero: COALESCE(x, 0) - best for counts/sums
- null: keep NULL - for averages when meaningful
- sentinel: large value (99999) - for recency features
- mean: placeholder marker (computed later, not SQL-level)
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from app.services.grain_service import validate_identifier


# =============================================================================
# Missing Strategy Types
# =============================================================================


class MissingStrategy(str, Enum):
    """Available missing value handling strategies."""
    ZERO = "zero"           # COALESCE(x, 0) - counts, sums, distinct_count
    NULL = "null"           # Keep NULL - meaningful nulls (no prior event)
    SENTINEL = "sentinel"   # Large value (99999) - recency, distances
    MEAN = "mean"           # Marker for post-SQL imputation


# =============================================================================
# Feature Column Config
# =============================================================================


@dataclass
class FeatureColumnConfig:
    """
    Configuration for handling missing values in a feature column.
    
    Attributes:
        column_name: Name of the feature column
        strategy: How to handle missing values
        add_indicator: Whether to add is_missing_<column> flag
        sentinel_value: Custom sentinel value (default: 99999)
    """
    column_name: str
    strategy: MissingStrategy = MissingStrategy.ZERO
    add_indicator: bool = False
    sentinel_value: int = 99999
    
    def __post_init__(self):
        validate_identifier(self.column_name, "column_name")


@dataclass
class FeatureMissingConfig:
    """
    Missing value configuration for a complete feature set.
    
    Attributes:
        feature_name: Human-readable feature name
        feature_key: SQL-safe identifier for the feature
        columns: List of column configurations
        source_alias: CTE alias for this feature (e.g., feature_0)
    """
    feature_name: str
    feature_key: str
    columns: list[FeatureColumnConfig] = field(default_factory=list)
    source_alias: str = ""
    
    def __post_init__(self):
        validate_identifier(self.feature_key, "feature_key")
        if self.source_alias:
            validate_identifier(self.source_alias, "source_alias")


# =============================================================================
# Missing Value Service
# =============================================================================


class MissingValueService:
    """
    Generates SQL expressions for handling missing values.
    
    Two capabilities:
    1. Apply missing strategy (COALESCE, NULL, SENTINEL)
    2. Add missingness indicator columns (is_missing_<col>)
    """

    @staticmethod
    def apply_strategy(
        column_name: str,
        strategy: MissingStrategy,
        alias: str = "",
        sentinel_value: int = 99999,
    ) -> str:
        """
        Generate SQL expression for a column with missing strategy applied.
        
        Args:
            column_name: Column to wrap
            strategy: Missing value strategy
            alias: Optional table/CTE alias
            sentinel_value: Value for SENTINEL strategy
            
        Returns:
            SQL expression with strategy applied
        """
        # Validate identifiers for safety
        validate_identifier(column_name, "column_name")
        if alias:
            validate_identifier(alias, "alias")
        
        col_ref = f"{alias}.{column_name}" if alias else column_name
        
        if strategy == MissingStrategy.ZERO:
            return f"COALESCE({col_ref}, 0)"
        elif strategy == MissingStrategy.NULL:
            return col_ref  # Keep as-is
        elif strategy == MissingStrategy.SENTINEL:
            return f"COALESCE({col_ref}, {sentinel_value})"
        elif strategy == MissingStrategy.MEAN:
            # Mean imputation is post-SQL; return column as-is (tracked separately)
            return col_ref
        else:
            raise ValueError(f"Unknown strategy: {strategy}")

    @staticmethod
    def generate_indicator_column(
        column_name: str,
        alias: str = "",
    ) -> tuple[str, str]:
        """
        Generate SQL for missingness indicator column.
        
        Args:
            column_name: Column to check for NULL
            alias: Optional table/CTE alias
            
        Returns:
            Tuple of (indicator_column_name, sql_expression)
        """
        col_ref = f"{alias}.{column_name}" if alias else column_name
        indicator_name = f"is_missing_{column_name}"
        
        # Validate generated name
        validate_identifier(indicator_name, "indicator column name")
        
        sql = f"CASE WHEN {col_ref} IS NULL THEN 1 ELSE 0 END"
        
        return indicator_name, sql

    @staticmethod
    def generate_select_columns(
        config: FeatureMissingConfig,
    ) -> list[tuple[str, str]]:
        """
        Generate SELECT column expressions for a feature with missing handling.
        
        Args:
            config: Feature missing value configuration
            
        Returns:
            List of (column_name, sql_expression) tuples
        """
        result = []
        alias = config.source_alias
        
        for col_config in config.columns:
            col_name = col_config.column_name
            
            # Add the main column with strategy applied
            sql_expr = MissingValueService.apply_strategy(
                col_name,
                col_config.strategy,
                alias,
                col_config.sentinel_value,
            )
            result.append((col_name, sql_expr))
            
            # Add indicator column if requested
            if col_config.add_indicator:
                ind_name, ind_sql = MissingValueService.generate_indicator_column(
                    col_name, alias
                )
                result.append((ind_name, ind_sql))
        
        return result

    @staticmethod
    def wrap_feature_cte(
        feature_alias: str,
        config: FeatureMissingConfig,
        passthrough_columns: list[str] | None = None,
    ) -> str:
        """
        Generate a wrapper CTE that applies missing value handling.
        
        Args:
            feature_alias: Alias for the wrapped CTE (e.g., feature_0_handled)
            config: Feature missing value configuration
            passthrough_columns: Columns to pass through unchanged (e.g., entity_id)
            
        Returns:
            SQL CTE definition string
        """
        if passthrough_columns is None:
            passthrough_columns = ["entity_id", "observation_date"]
        
        # Validate feature_alias to prevent SQL injection
        validate_identifier(feature_alias, "feature_alias")
        
        source = config.source_alias
        
        # Build passthrough column list
        passthrough = [f"{source}.{col}" for col in passthrough_columns]
        
        # Build handled columns
        handled_cols = MissingValueService.generate_select_columns(config)
        
        # Format as SELECT expressions with aliases
        all_columns = passthrough.copy()
        for col_name, sql_expr in handled_cols:
            all_columns.append(f"{sql_expr} AS {col_name}")
        
        # Format with proper newlines between columns
        select_list = ",\n        ".join(all_columns)
        
        return f"""{feature_alias} AS (
    SELECT
        {select_list}
    FROM {source}
)"""

    @staticmethod
    def get_recommended_strategy(template_type: str) -> dict[str, Any]:
        """
        Get recommended missing strategy for a feature template type.
        
        Args:
            template_type: Feature template type (rolling_count, etc.)
            
        Returns:
            Dict with recommended strategy and add_indicator flag
        """
        recommendations = {
            "rolling_count": {
                "strategy": MissingStrategy.ZERO,
                "add_indicator": False,
                "reason": "Count of 0 is meaningful (no events)",
            },
            "rolling_sum": {
                "strategy": MissingStrategy.ZERO,
                "add_indicator": False,
                "reason": "Sum of 0 is meaningful (no events)",
            },
            "rolling_avg": {
                "strategy": MissingStrategy.NULL,
                "add_indicator": True,
                "reason": "NULL avg means no data; indicator helps model",
            },
            "recency": {
                "strategy": MissingStrategy.SENTINEL,
                "add_indicator": True,
                "reason": "NULL means no prior event; sentinel (99999) preserves ordering",
            },
            "distinct_count": {
                "strategy": MissingStrategy.ZERO,
                "add_indicator": False,
                "reason": "Count of 0 unique values is meaningful",
            },
        }
        
        return recommendations.get(template_type, {
            "strategy": MissingStrategy.NULL,
            "add_indicator": True,
            "reason": "Default: keep NULL with indicator for unknown template",
        })

    @staticmethod
    def list_strategies() -> list[dict[str, Any]]:
        """List available missing value strategies with descriptions."""
        return [
            {
                "strategy": MissingStrategy.ZERO.value,
                "description": "Replace NULL with 0",
                "sql_example": "COALESCE(column, 0)",
                "best_for": ["counts", "sums", "distinct_count"],
            },
            {
                "strategy": MissingStrategy.NULL.value,
                "description": "Keep NULL as-is",
                "sql_example": "column",
                "best_for": ["averages", "meaningful nulls"],
            },
            {
                "strategy": MissingStrategy.SENTINEL.value,
                "description": "Replace NULL with large value (default: 99999)",
                "sql_example": "COALESCE(column, 99999)",
                "best_for": ["recency", "time-since features"],
            },
            {
                "strategy": MissingStrategy.MEAN.value,
                "description": "Marker for post-SQL mean imputation",
                "sql_example": "column /* IMPUTE_MEAN */",
                "best_for": ["numeric features requiring mean imputation"],
            },
        ]



missing_value_service = MissingValueService()
