"""
Quality Check Service
Automated data quality analysis for ML feature sets.
Provides data quality analysis for ML feature sets.
"""

import logging
from dataclasses import dataclass
from typing import Any, Literal, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


# =============================================================================
# Missing Value Strategies
# =============================================================================

MISSING_STRATEGIES = {
    "numeric_count": ["ZERO", "NULL"],  # Counts default to 0
    "numeric_amount": ["MEDIAN", "MEAN", "ZERO", "NULL"],
    "categorical": ["MODE", "UNKNOWN", "NULL"],
    "boolean": ["FALSE", "NULL"],
}


def generate_imputation_sql(
    column: str,
    strategy: str,
    add_indicator: bool = True,
) -> dict[str, str]:
    """
    Generate SQL for imputing missing values.
    
    Args:
        column: Column name to impute
        strategy: Imputation strategy (ZERO, MEAN, MEDIAN, MODE, UNKNOWN, etc.)
        add_indicator: If True, add is_missing indicator column
        
    Returns:
        Dict with 'imputed_expr' and optionally 'indicator_expr'
    """
    result = {}
    
    strategy_upper = strategy.upper()
    
    if strategy_upper == "ZERO":
        result["imputed_expr"] = f'COALESCE("{column}", 0) AS "{column}"'
    elif strategy_upper == "NULL":
        result["imputed_expr"] = f'"{column}"'  # Keep as-is
    elif strategy_upper == "MEAN":
        # Use window function for mean
        result["imputed_expr"] = f'COALESCE("{column}", AVG("{column}") OVER ()) AS "{column}"'
    elif strategy_upper == "MEDIAN":
        # Postgres PERCENTILE_CONT for median
        result["imputed_expr"] = f'COALESCE("{column}", PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{column}") OVER ()) AS "{column}"'
    elif strategy_upper == "MODE":
        # Use MODE() for categorical
        result["imputed_expr"] = f'COALESCE("{column}", MODE() WITHIN GROUP (ORDER BY "{column}") OVER ()) AS "{column}"'
    elif strategy_upper == "UNKNOWN":
        result["imputed_expr"] = f"COALESCE(\"{column}\", 'UNKNOWN') AS \"{column}\""
    elif strategy_upper == "FALSE":
        result["imputed_expr"] = f'COALESCE("{column}", FALSE) AS "{column}"'
    else:
        result["imputed_expr"] = f'"{column}"'  # Default: keep as-is
    
    if add_indicator:
        result["indicator_expr"] = f'("{column}" IS NULL)::INT AS "{column}_is_missing"'
    
    return result


# =============================================================================
# Quality Check Service
# =============================================================================

@dataclass
class FeatureStats:
    """Statistics for a single feature."""
    column_name: str
    null_count: int
    null_percent: float
    distinct_count: int
    min_value: Any
    max_value: Any
    mean_value: float | None
    is_low_variance: bool = False
    warnings: list[str] = None
    
    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


class QualityCheckService:
    """
    Service for automated data quality checks.
    
    Provides:
    - Feature-level statistics (NULL %, distinct count, variance)
    - Leakage detection (high correlation to target)
    - Missing value strategies
    """
    
    @staticmethod
    def run_feature_eda(
        engine: Engine,
        sql: str,
        feature_columns: list[str],
        target_column: str | None = None,
        sample_limit: int = 100000,
    ) -> dict[str, Any]:
        """
        Run EDA on generated features.
        
        Args:
            engine: SQLAlchemy engine
            sql: SQL query producing features
            feature_columns: List of feature column names
            target_column: Optional target column for correlation
            sample_limit: Max rows to analyze
            
        Returns:
            EDA report with feature stats and warnings
        """
        # Wrap SQL with sample limit
        sample_sql = f"""
            SELECT * FROM (
                {sql.strip().rstrip(';')}
            ) AS eda_sample
            LIMIT {sample_limit}
        """
        
        feature_stats = []
        warnings = []
        
        with engine.connect() as conn:
            # Get overall stats per column
            for col in feature_columns:
                try:
                    stats = QualityCheckService._get_column_stats(
                        conn, sql, col, sample_limit
                    )
                    feature_stats.append(stats)
                    
                    # Check for issues
                    if stats.null_percent > 50:
                        warnings.append(f"{col}: {stats.null_percent:.1f}% NULL")
                    if stats.is_low_variance:
                        warnings.append(f"{col}: Low variance (may be uninformative)")
                        
                except Exception as e:
                    logger.warning(f"Failed to get stats for {col}: {e}")
            
            # Check target correlation if provided
            high_corr_features = []
            if target_column and len(feature_columns) > 0:
                high_corr_features = QualityCheckService._check_target_correlation(
                    conn, sql, feature_columns, target_column, sample_limit
                )
                for feat, corr in high_corr_features:
                    warnings.append(f"LEAKAGE WARNING: {feat} has {corr:.2f} correlation with target")
        
        return {
            "feature_stats": [
                {
                    "column": s.column_name,
                    "null_percent": s.null_percent,
                    "distinct_count": s.distinct_count,
                    "min": s.min_value,
                    "max": s.max_value,
                    "mean": s.mean_value,
                    "is_low_variance": s.is_low_variance,
                    "warnings": s.warnings,
                }
                for s in feature_stats
            ],
            "high_correlation_features": high_corr_features,
            "warnings": warnings,
            "feature_count": len(feature_columns),
            "status": "success",
        }
    
    @staticmethod
    def scan_for_leakage(
        engine: Engine,
        sql: str,
        feature_columns: list[str],
        target_column: str,
        correlation_threshold: float = 0.9,
        sample_limit: int = 100000,
    ) -> dict[str, Any]:
        """
        Scan features for potential data leakage.
        
        Args:
            engine: SQLAlchemy engine
            sql: SQL query producing features
            feature_columns: Feature column names
            target_column: Target column name
            correlation_threshold: Flag features above this correlation
            sample_limit: Max rows to analyze
            
        Returns:
            Leakage report with suspicious features
        """
        suspicious_features = []
        
        with engine.connect() as conn:
            for col in feature_columns:
                try:
                    corr = QualityCheckService._calculate_correlation(
                        conn, sql, col, target_column, sample_limit
                    )
                    if corr is not None and abs(corr) >= correlation_threshold:
                        suspicious_features.append({
                            "feature": col,
                            "correlation": round(corr, 4),
                            "severity": "HIGH" if abs(corr) > 0.95 else "MEDIUM",
                        })
                except Exception as e:
                    logger.warning(f"Correlation check failed for {col}: {e}")
        
        # Sort by correlation
        suspicious_features.sort(key=lambda x: abs(x["correlation"]), reverse=True)
        
        return {
            "suspicious_features": suspicious_features[:10],  # Top 10
            "total_checked": len(feature_columns),
            "leakage_detected": len(suspicious_features) > 0,
            "threshold": correlation_threshold,
            "status": "warning" if suspicious_features else "success",
        }
    
    @staticmethod
    def get_missing_strategies(column_type: str = "numeric_amount") -> list[str]:
        """Get available missing value strategies for a column type."""
        return MISSING_STRATEGIES.get(column_type, ["NULL"])
    
    @staticmethod
    def generate_imputation_sql(
        column: str,
        strategy: str,
        add_indicator: bool = True,
    ) -> dict[str, str]:
        """Generate SQL for imputing missing values."""
        return generate_imputation_sql(column, strategy, add_indicator)
    
    @staticmethod
    def _get_column_stats(
        conn,
        sql: str,
        column: str,
        sample_limit: int,
    ) -> FeatureStats:
        """Get statistics for a single column."""
        stats_sql = f"""
            WITH sample AS (
                SELECT * FROM ({sql.strip().rstrip(';')}) s LIMIT {sample_limit}
            )
            SELECT 
                COUNT(*) AS total_rows,
                COUNT(*) - COUNT("{column}") AS null_count,
                COUNT(DISTINCT "{column}") AS distinct_count,
                MIN("{column}"::TEXT) AS min_val,
                MAX("{column}"::TEXT) AS max_val,
                AVG(CASE WHEN "{column}"::TEXT ~ '^[0-9.-]+$' 
                    THEN "{column}"::FLOAT ELSE NULL END) AS mean_val
            FROM sample
        """
        
        result = conn.execute(text(stats_sql))
        row = result.fetchone()
        
        total = row[0] or 1
        null_count = row[1] or 0
        distinct_count = row[2] or 0
        
        # Low variance if <2 distinct values
        is_low_variance = distinct_count < 2
        
        return FeatureStats(
            column_name=column,
            null_count=null_count,
            null_percent=100.0 * null_count / total,
            distinct_count=distinct_count,
            min_value=row[3],
            max_value=row[4],
            mean_value=float(row[5]) if row[5] else None,
            is_low_variance=is_low_variance,
        )
    
    @staticmethod
    def _check_target_correlation(
        conn,
        sql: str,
        feature_columns: list[str],
        target_column: str,
        sample_limit: int,
        threshold: float = 0.9,
    ) -> list[tuple[str, float]]:
        """Check correlation between features and target."""
        high_corr = []
        
        for col in feature_columns:
            try:
                corr = QualityCheckService._calculate_correlation(
                    conn, sql, col, target_column, sample_limit
                )
                if corr is not None and abs(corr) >= threshold:
                    high_corr.append((col, corr))
            except Exception:
                pass
        
        return high_corr
    
    @staticmethod
    def _calculate_correlation(
        conn,
        sql: str,
        col1: str,
        col2: str,
        sample_limit: int,
    ) -> float | None:
        """Calculate Pearson correlation between two columns."""
        corr_sql = f"""
            WITH sample AS (
                SELECT * FROM ({sql.strip().rstrip(';')}) s LIMIT {sample_limit}
            )
            SELECT CORR(
                CASE WHEN "{col1}"::TEXT ~ '^[0-9.-]+$' 
                    THEN "{col1}"::FLOAT ELSE NULL END,
                CASE WHEN "{col2}"::TEXT ~ '^[0-9.-]+$' 
                    THEN "{col2}"::FLOAT ELSE NULL END
            ) AS correlation
            FROM sample
        """
        
        result = conn.execute(text(corr_sql))
        row = result.fetchone()
        return float(row[0]) if row[0] is not None else None



quality_checker = QualityCheckService()
