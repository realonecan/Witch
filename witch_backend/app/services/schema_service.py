"""
Schema Service
Implements data discovery for ML dataset preparation.

This service provides:
1. List all tables with row counts
2. Detect entity columns (ID columns with high cardinality)
3. Profile tables (NULL%, cardinality, date ranges)
4. Estimate computational cost

Part of 
"""

from dataclasses import dataclass, field
from typing import Optional, Any
import logging
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.services.grain_service import validate_identifier

logger = logging.getLogger(__name__)


# =============================================================================
# Data Types
# =============================================================================

@dataclass
class ColumnInfo:
    """Information about a single column."""
    name: str
    data_type: str
    is_nullable: bool
    
    # Profiling stats (populated by profile_table)
    null_count: int = 0
    null_percent: float = 0.0
    distinct_count: int = 0
    min_value: Optional[str] = None
    max_value: Optional[str] = None


@dataclass
class TableInfo:
    """Information about a single table."""
    schema_name: str
    table_name: str
    row_count: int
    column_count: int
    
    # Date range (if table has date columns)
    min_date: Optional[str] = None
    max_date: Optional[str] = None
    date_column: Optional[str] = None
    
    # Entity detection
    has_entity_column: bool = False
    entity_columns: list[str] = field(default_factory=list)


@dataclass
class EntityColumn:
    """A detected entity column (potential grain ID)."""
    column_name: str
    tables: list[str]  # Tables containing this column
    total_unique: int  # Unique values across all tables
    
    # Confidence score (0-1) based on naming + cardinality
    confidence: float = 0.0


@dataclass
class TableProfile:
    """Detailed profile of a single table."""
    schema_name: str
    table_name: str
    row_count: int
    columns: list[ColumnInfo]
    
    # Quality summary
    total_null_percent: float = 0.0
    date_columns: list[str] = field(default_factory=list)
    id_columns: list[str] = field(default_factory=list)
    
    # Date range
    min_date: Optional[str] = None
    max_date: Optional[str] = None


@dataclass
class CostEstimate:
    """Estimated computational cost for dataset generation."""
    estimated_rows: int
    estimated_seconds: float
    estimated_memory_gb: float
    
    # Warnings
    warning: Optional[str] = None
    recommendation: Optional[str] = None


# =============================================================================
# Schema Service
# =============================================================================

class SchemaService:
    """
    Service for database schema discovery and profiling.
    
    Implements 
    - List tables with row counts
    - Detect entity columns
    - Profile tables for quality
    - Estimate computational cost
    """
    
    def get_all_tables(
        self,
        engine: Engine,
        schema: str = "public",
    ) -> list[TableInfo]:
        """
        Get all tables in schema with row counts.
        
        Args:
            engine: SQLAlchemy engine
            schema: Database schema (default: public)
            
        Returns:
            List of TableInfo with row counts and basic stats
        """
        validate_identifier(schema, "schema")
        
        tables: list[TableInfo] = []
        
        with engine.connect() as conn:
            # Get all tables with column counts and estimated row counts
            result = conn.execute(text("""
                SELECT 
                    t.table_name,
                    (
                        SELECT COUNT(*) 
                        FROM information_schema.columns c 
                        WHERE c.table_schema = t.table_schema 
                          AND c.table_name = t.table_name
                    ) AS column_count,
                    COALESCE(pg_stat.reltuples::bigint, 0) AS row_estimate
                FROM information_schema.tables t
                LEFT JOIN pg_class pg_stat ON pg_stat.relname = t.table_name
                LEFT JOIN pg_namespace ns ON ns.oid = pg_stat.relnamespace 
                    AND ns.nspname = t.table_schema
                WHERE t.table_schema = :schema
                  AND t.table_type = 'BASE TABLE'
                ORDER BY t.table_name
            """), {"schema": schema})
            
            table_rows = result.fetchall()
            
            # For each table, get column info
            for table_row in table_rows:
                table_name = table_row[0]
                column_count = table_row[1]
                row_count = max(0, int(table_row[2]))  # Can be -1 for new tables
                
                # Get columns for this table
                col_result = conn.execute(text("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = :schema
                      AND table_name = :table
                    ORDER BY ordinal_position
                """), {"schema": schema, "table": table_name})
                
                columns = col_result.fetchall()
                
                # Detect date and entity columns
                date_columns = []
                entity_columns = []
                
                for col in columns:
                    col_name = col[0]
                    col_type = col[1]
                    
                    if self._is_date_type(col_type):
                        date_columns.append(col_name)
                    
                    if self._is_id_column(col_name):
                        entity_columns.append(col_name)
                
                # Get date range if table has date columns and has rows
                min_date = None
                max_date = None
                date_column = None
                
                if date_columns and row_count > 0:
                    # Use first date column for range
                    date_column = date_columns[0]
                    try:
                        # Safe: date_column comes from information_schema
                        if row_count > 200000:
                            sample_percent = min(100.0, max(0.1, (100000 / row_count) * 100))
                            date_sql = f'''
                                SELECT 
                                    MIN("{date_column}")::text,
                                    MAX("{date_column}")::text
                                FROM (
                                    SELECT "{date_column}"
                                    FROM "{schema}"."{table_name}"
                                    TABLESAMPLE BERNOULLI({sample_percent})
                                    LIMIT 100000
                                ) sampled
                            '''
                        else:
                            date_sql = f'''
                                SELECT 
                                    MIN("{date_column}")::text,
                                    MAX("{date_column}")::text
                                FROM "{schema}"."{table_name}"
                            '''
                        date_result = conn.execute(text(date_sql))
                        date_row = date_result.fetchone()
                        if date_row:
                            min_date = date_row[0][:10] if date_row[0] else None
                            max_date = date_row[1][:10] if date_row[1] else None
                    except Exception:
                        # Skip date range on error (might be non-date column)
                        pass
                
                tables.append(TableInfo(
                    schema_name=schema,
                    table_name=table_name,
                    row_count=row_count,
                    column_count=column_count,
                    min_date=min_date,
                    max_date=max_date,
                    date_column=date_column,
                    has_entity_column=len(entity_columns) > 0,
                    entity_columns=entity_columns,
                ))
        
        return tables
    
    def detect_entity_columns(
        self,
        engine: Engine,
        schema: str = "public",
    ) -> list[EntityColumn]:
        """
        Detect potential entity ID columns across all tables.
        
        Looks for:
        - Columns ending in "_id" with high cardinality
        - Columns appearing in multiple tables (likely join keys)
        - Primary keys
        
        Args:
            engine: SQLAlchemy engine
            schema: Database schema
            
        Returns:
            List of EntityColumn sorted by confidence
        """
        validate_identifier(schema, "schema")
        
        # Collect all ID-like columns across tables
        # Key: column_name, Value: list of (table_name, approx_distinct)
        column_occurrences: dict[str, list[tuple[str, int]]] = {}

        with engine.connect() as conn:
            # Get all columns that look like entity IDs
            result = conn.execute(text("""
                SELECT 
                    c.table_name,
                    c.column_name
                FROM information_schema.columns c
                JOIN information_schema.tables t 
                    ON t.table_schema = c.table_schema 
                    AND t.table_name = c.table_name
                WHERE c.table_schema = :schema
                  AND t.table_type = 'BASE TABLE'
                  AND (
                      c.column_name LIKE '%_id'
                      OR c.column_name = 'id'
                      OR c.column_name LIKE '%_key'
                      OR c.column_name LIKE '%_code'
                  )
                ORDER BY c.column_name, c.table_name
            """), {"schema": schema})

            rows = result.fetchall()
            if not rows:
                return []

            table_names = sorted({row[0] for row in rows})
            column_names = sorted({row[1] for row in rows})

            # Get row estimates per table (fast, from pg_class)
            table_row_estimates: dict[str, int] = {}
            try:
                estimate_result = conn.execute(text("""
                    SELECT c.relname, COALESCE(c.reltuples::bigint, 0) AS row_estimate
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = :schema
                      AND c.relname = ANY(:tables)
                """), {"schema": schema, "tables": table_names})

                for row in estimate_result.fetchall():
                    table_row_estimates[row[0]] = max(0, int(row[1] or 0))
            except Exception as e:
                logger.warning(f"Failed to fetch row estimates: {e}")

            # Get approximate distinct counts from pg_stats (fast)
            stats_map: dict[tuple[str, str], float] = {}
            try:
                stats_result = conn.execute(text("""
                    SELECT tablename, attname, n_distinct
                    FROM pg_stats
                    WHERE schemaname = :schema
                      AND attname = ANY(:columns)
                """), {"schema": schema, "columns": column_names})

                for row in stats_result.fetchall():
                    stats_map[(row[0], row[1])] = float(row[2])
            except Exception as e:
                logger.warning(f"Failed to read pg_stats for entity detection: {e}")

            for table_name, col_name in rows:
                if col_name not in column_occurrences:
                    column_occurrences[col_name] = []

                n_distinct = stats_map.get((table_name, col_name))
                row_estimate = table_row_estimates.get(table_name, 0)

                if n_distinct is None:
                    distinct_count = 0
                elif n_distinct < 0:
                    # Negative means fraction of total rows
                    distinct_count = int(abs(n_distinct) * row_estimate) if row_estimate > 0 else 0
                else:
                    distinct_count = int(n_distinct)

                column_occurrences[col_name].append((table_name, distinct_count))
        
        # Build EntityColumn list with confidence scoring
        entities: list[EntityColumn] = []
        
        for col_name, occurrences in column_occurrences.items():
            tables = [t[0] for t in occurrences]
            max_distinct = max(t[1] for t in occurrences) if occurrences else 0
            
            # Confidence scoring (0-1):
            # - Higher if column appears in multiple tables (join key)
            # - Higher if high cardinality (not a status column)
            # - Higher if ends with _id vs _key/_code
            
            # Base score from table count
            table_count = len(tables)
            table_score = min(table_count / 5.0, 1.0)  # Max at 5 tables
            
            # Cardinality score (high cardinality = good entity)
            if max_distinct > 100000:
                cardinality_score = 1.0
            elif max_distinct > 10000:
                cardinality_score = 0.8
            elif max_distinct > 1000:
                cardinality_score = 0.6
            elif max_distinct > 100:
                cardinality_score = 0.4
            else:
                cardinality_score = 0.2
            
            # Naming score
            if col_name.endswith("_id") or col_name == "id":
                naming_score = 1.0
            elif col_name.endswith("_key"):
                naming_score = 0.7
            else:
                naming_score = 0.5
            
            # Combined confidence
            confidence = (table_score * 0.3) + (cardinality_score * 0.5) + (naming_score * 0.2)
            
            entities.append(EntityColumn(
                column_name=col_name,
                tables=tables,
                total_unique=max_distinct,
                confidence=round(confidence, 2),
            ))
        
        # Sort by confidence (highest first)
        entities.sort(key=lambda e: e.confidence, reverse=True)
        
        return entities
    
    def profile_table(
        self,
        engine: Engine,
        table_name: str,
        schema: str = "public",
    ) -> TableProfile:
        """
        Get detailed profile of a single table.
        
        Includes:
        - Row count (exact or estimated)
        - Per-column: NULL%, distinct count, min/max
        - Date column detection
        - ID column detection
        
        Uses single query for all columns (no N+1 problem).
        
        Args:
            engine: SQLAlchemy engine
            table_name: Table to profile
            schema: Database schema
            
        Returns:
            TableProfile with detailed stats
        """
        validate_identifier(table_name, "table")
        validate_identifier(schema, "schema")
        
        columns: list[ColumnInfo] = []
        date_columns: list[str] = []
        id_columns: list[str] = []
        min_date = None
        max_date = None
        
        with engine.connect() as conn:
            # Get row count
            row_count = self._get_row_count_estimate(engine, schema, table_name)
            
            # Get column metadata
            col_result = conn.execute(text("""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable
                FROM information_schema.columns
                WHERE table_schema = :schema
                  AND table_name = :table
                ORDER BY ordinal_position
            """), {"schema": schema, "table": table_name})
            
            col_rows = col_result.fetchall()
            col_names = [row[0] for row in col_rows]
            col_types = {row[0]: row[1] for row in col_rows}
            col_nullable = {row[0]: row[2] == "YES" for row in col_rows}
            
            # Initialize stats dicts
            null_counts: dict[str, int] = {c: 0 for c in col_names}
            distinct_counts: dict[str, int] = {c: 0 for c in col_names}
            min_values: dict[str, str | None] = {c: None for c in col_names}
            max_values: dict[str, str | None] = {c: None for c in col_names}
            
            # Only profile if table has rows and columns
            if row_count > 0 and col_names:
                try:
                    # Build SINGLE query for all columns
                    # Column names come from information_schema, safe to use
                    null_exprs = [f'COUNT(*) - COUNT("{c}") AS "{c}_null"' for c in col_names]
                    distinct_exprs = [f'COUNT(DISTINCT "{c}") AS "{c}_distinct"' for c in col_names]
                    
                    stats_sql = f'''
                        SELECT 
                            {", ".join(null_exprs)},
                            {", ".join(distinct_exprs)}
                        FROM "{schema}"."{table_name}"
                    '''
                    
                    stats_result = conn.execute(text(stats_sql))
                    stats_row = stats_result.fetchone()
                    
                    if stats_row:
                        # Parse null counts (first half of results)
                        for i, col_name in enumerate(col_names):
                            null_counts[col_name] = int(stats_row[i]) if stats_row[i] else 0
                        
                        # Parse distinct counts (second half of results)
                        offset = len(col_names)
                        for i, col_name in enumerate(col_names):
                            distinct_counts[col_name] = int(stats_row[offset + i]) if stats_row[offset + i] else 0
                    
                except Exception as e:
                    logger.warning(f"Failed to get column stats for {schema}.{table_name}: {e}")
                
                # Get min/max for date columns (separate query, but only for date cols)
                date_type_cols = [c for c in col_names if self._is_date_type(col_types[c])]
                
                if date_type_cols:
                    try:
                        minmax_exprs = []
                        for c in date_type_cols:
                            minmax_exprs.append(f'MIN("{c}")::text AS "{c}_min"')
                            minmax_exprs.append(f'MAX("{c}")::text AS "{c}_max"')
                        
                        minmax_sql = f'''
                            SELECT {", ".join(minmax_exprs)}
                            FROM "{schema}"."{table_name}"
                        '''
                        
                        minmax_result = conn.execute(text(minmax_sql))
                        minmax_row = minmax_result.fetchone()
                        
                        if minmax_row:
                            for i, col_name in enumerate(date_type_cols):
                                raw_min = minmax_row[i * 2]
                                raw_max = minmax_row[i * 2 + 1]
                                
                                min_val = raw_min[:10] if raw_min else None
                                max_val = raw_max[:10] if raw_max else None
                                
                                min_values[col_name] = min_val
                                max_values[col_name] = max_val
                                
                                # Track overall date range
                                if min_date is None or (min_val and min_val < min_date):
                                    min_date = min_val
                                if max_date is None or (max_val and max_val > max_date):
                                    max_date = max_val
                    
                    except Exception as e:
                        logger.warning(f"Failed to get date ranges for {schema}.{table_name}: {e}")
            
            # Build column list
            total_null_count = 0
            for col_name in col_names:
                data_type = col_types[col_name]
                null_count = null_counts[col_name]
                null_percent = (null_count / row_count * 100) if row_count > 0 else 0.0
                
                total_null_count += null_count
                
                # Track column types
                if self._is_date_type(data_type):
                    date_columns.append(col_name)
                if self._is_id_column(col_name):
                    id_columns.append(col_name)
                
                columns.append(ColumnInfo(
                    name=col_name,
                    data_type=data_type,
                    is_nullable=col_nullable[col_name],
                    null_count=null_count,
                    null_percent=round(null_percent, 2),
                    distinct_count=distinct_counts[col_name],
                    min_value=min_values[col_name],
                    max_value=max_values[col_name],
                ))
        
        # Calculate overall null percentage
        total_cell_count = row_count * len(col_names) if col_names else 0
        total_null_percent = (total_null_count / total_cell_count * 100) if total_cell_count > 0 else 0.0
        
        return TableProfile(
            schema_name=schema,
            table_name=table_name,
            row_count=row_count,
            columns=columns,
            total_null_percent=round(total_null_percent, 2),
            date_columns=date_columns,
            id_columns=id_columns,
            min_date=min_date,
            max_date=max_date,
        )

    def get_numeric_histogram(
        self,
        engine: Engine,
        table_name: str,
        column_name: str,
        schema: str = "public",
        bins: int = 12,
        sample_size: int = 100000,
    ) -> dict[str, Any]:
        """
        Build a numeric histogram for a column.

        Uses sampling on large tables to avoid heavy scans.
        """
        validate_identifier(table_name, "table")
        validate_identifier(column_name, "column")
        validate_identifier(schema, "schema")

        if bins < 2:
            bins = 2
        if bins > 50:
            bins = 50

        numeric_types = {
            "integer", "int", "smallint", "bigint", "serial", "bigserial",
            "decimal", "numeric", "real", "double precision", "float", "money",
        }

        with engine.connect() as conn:
            type_result = conn.execute(text("""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = :schema
                  AND table_name = :table
                  AND column_name = :column
            """), {"schema": schema, "table": table_name, "column": column_name})

            type_row = type_result.fetchone()
            if not type_row:
                raise ValueError(f"Column not found: {table_name}.{column_name}")

            data_type = type_row[0].lower()
            if data_type not in numeric_types:
                raise ValueError(f"Column is not numeric: {table_name}.{column_name}")

            row_estimate = self._get_row_count_estimate(engine, schema, table_name)
            use_sample = sample_size > 0 and row_estimate > sample_size
            sample_percent = None

            if use_sample:
                sample_percent = (sample_size / row_estimate) * 100
                sample_percent = max(0.1, min(100.0, sample_percent))
                base_from = f'"{schema}"."{table_name}" TABLESAMPLE BERNOULLI({sample_percent})'
            else:
                base_from = f'"{schema}"."{table_name}"'

            sample_sql = f'''
                WITH sampled AS (
                    SELECT "{column_name}"::double precision AS value
                    FROM {base_from}
                    WHERE "{column_name}" IS NOT NULL
                )
                SELECT MIN(value), MAX(value), COUNT(*) FROM sampled
            '''

            histogram = []
            total_count = 0
            min_val = None
            max_val = None

            try:
                conn.execute(text("SET statement_timeout = 30000"))
                minmax_result = conn.execute(text(sample_sql))
                minmax_row = minmax_result.fetchone()
                if minmax_row:
                    min_val = minmax_row[0]
                    max_val = minmax_row[1]
                    total_count = int(minmax_row[2]) if minmax_row[2] else 0

                if total_count == 0 or min_val is None or max_val is None:
                    return {
                        "table_name": table_name,
                        "column_name": column_name,
                        "min": None,
                        "max": None,
                        "bins": bins,
                        "total_count": total_count,
                        "sampled": use_sample,
                        "sample_percent": round(sample_percent, 3) if sample_percent else None,
                        "sample_size": sample_size if use_sample else None,
                        "histogram": [],
                    }

                if min_val == max_val:
                    return {
                        "table_name": table_name,
                        "column_name": column_name,
                        "min": float(min_val),
                        "max": float(max_val),
                        "bins": 1,
                        "total_count": total_count,
                        "sampled": use_sample,
                        "sample_percent": round(sample_percent, 3) if sample_percent else None,
                        "sample_size": sample_size if use_sample else None,
                        "histogram": [{"bucket": 1, "count": total_count}],
                    }

                hist_sql = f'''
                    WITH sampled AS (
                        SELECT "{column_name}"::double precision AS value
                        FROM {base_from}
                        WHERE "{column_name}" IS NOT NULL
                    )
                    SELECT width_bucket(value, :min_val, :max_val, :bins) AS bucket,
                           COUNT(*) AS count
                    FROM sampled
                    GROUP BY bucket
                    ORDER BY bucket
                '''

                hist_result = conn.execute(text(hist_sql), {
                    "min_val": min_val,
                    "max_val": max_val,
                    "bins": bins,
                })

                for row in hist_result.fetchall():
                    bucket = int(row[0]) if row[0] is not None else 0
                    count = int(row[1]) if row[1] else 0
                    if bucket <= 0 or bucket > bins:
                        continue
                    histogram.append({"bucket": bucket, "count": count})

            finally:
                try:
                    conn.execute(text("SET statement_timeout = 0"))
                except Exception:
                    pass

        return {
            "table_name": table_name,
            "column_name": column_name,
            "min": float(min_val) if min_val is not None else None,
            "max": float(max_val) if max_val is not None else None,
            "bins": bins,
            "total_count": total_count,
            "sampled": use_sample,
            "sample_percent": round(sample_percent, 3) if sample_percent else None,
            "sample_size": sample_size if use_sample else None,
            "histogram": histogram,
        }
    
    def estimate_cost(
        self,
        row_count: int,
        feature_count: int,
        window_sizes: list[int],
    ) -> CostEstimate:
        """
        Estimate computational cost for dataset generation.
        
        NOTE: We are EXPORT-ONLY. We generate SQL, not execute it.
        - estimated_seconds = SQL generation time (our tool)
        - User runs the SQL on their own database
        
        Args:
            row_count: Total observation rows
            feature_count: Number of features to generate
            window_sizes: List of window sizes (e.g., [30, 60, 90])
            
        Returns:
            CostEstimate with time/memory estimates and warnings
        """
        # SQL GENERATION is fast - doesn't depend on data volume
        # Just string building, ~2 minutes regardless of size
        sql_generation_seconds = 120.0  # ~2 minutes
        
        # We don't load data into memory - user's DB does
        # This is informational for the user about their DB requirements
        estimated_memory_gb = 0.0  # We don't use memory
        
        warning = None
        recommendation = None
        
        if row_count > 100_000_000:
            warning = "VERY LARGE DATASET (>100M rows)"
            recommendation = "When you run the SQL, expect it to take 1-2 hours. Consider reducing date range or sampling to 10M rows for faster iteration."
        elif row_count > 50_000_000:
            warning = "Large dataset (>50M rows)"
            recommendation = "When you run the SQL, expect it to take 30-60 minutes. Consider sampling for faster iteration."
        elif row_count > 10_000_000:
            warning = None
            recommendation = "Estimated SQL runtime: 5-15 minutes on typical Postgres."
        
        return CostEstimate(
            estimated_rows=row_count,
            estimated_seconds=sql_generation_seconds,
            estimated_memory_gb=estimated_memory_gb,
            warning=warning,
            recommendation=recommendation,
        )
    
    # =========================================================================
    # Helper Methods
    # =========================================================================
    
    def _get_row_count_estimate(
        self,
        engine: Engine,
        schema: str,
        table: str,
    ) -> int:
        """
        Get estimated row count from pg_stat.
        
        Fast but approximate. Use for large tables.
        Falls back to COUNT(*) for small tables.
        """
        validate_identifier(schema, "schema")
        validate_identifier(table, "table")
        
        with engine.connect() as conn:
            # Try pg_stat first (fast)
            result = conn.execute(text("""
                SELECT reltuples::bigint AS estimate
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = :schema
                  AND c.relname = :table
            """), {"schema": schema, "table": table})
            
            row = result.fetchone()
            if row and row[0] > 0:
                return int(row[0])
            
            # Fallback to COUNT(*) for small/new tables
            # Using format since we validated identifiers above
            count_sql = f'SELECT COUNT(*) FROM "{schema}"."{table}"'
            result = conn.execute(text(count_sql))
            row = result.fetchone()
            return int(row[0]) if row else 0
    
    def _is_date_type(self, data_type: str) -> bool:
        """Check if data type is date-like."""
        date_types = [
            "date", "timestamp", "timestamptz",
            "timestamp with time zone",
            "timestamp without time zone"
        ]
        return data_type.lower() in date_types
    
    def _is_id_column(self, column_name: str) -> bool:
        """Check if column name suggests an ID column."""
        name_lower = column_name.lower()
        return (
            name_lower.endswith("_id") or
            name_lower == "id" or
            name_lower.endswith("_key") or
            name_lower.endswith("_code")
        )


# =============================================================================
# Global Instance
# =============================================================================

schema_service = SchemaService()
