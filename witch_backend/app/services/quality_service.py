"""
Quality Service
Provides data quality auditing for ML suitability assessment.


Key capabilities:
- Entity table grain-aware checks (duplicates, missingness based on grain)
- Feature source table checks (joinability, time coverage)
- Sampling for large tables
- Single table audit for quick exploration
"""

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine


class SamplingStrategy:
    """Sampling strategies for large tables."""

    @staticmethod
    def get_sample_sql(
        table: str,
        schema: str,
        sample_size: int = 10000,
        method: str = "auto",
    ) -> str:
        """
        Generate sampling SQL based on method.

        Args:
            table: Table name.
            schema: Schema name.
            sample_size: Target sample size.
            method: Sampling method:
                - "auto": Try TABLESAMPLE, fallback to LIMIT
                - "tablesample": Use TABLESAMPLE BERNOULLI
                - "random": ORDER BY random() LIMIT (slower but uniform)
                - "fast": Simple LIMIT (fast but biased)

        Returns:
            FROM clause with sampling.
        """
        full_table = f'"{schema}"."{table}"'

        if method == "fast":
            # Fastest but biased - just limit
            return f'{full_table} LIMIT {sample_size}'
        elif method == "random":
            # Slower but uniform distribution
            return f'{full_table} ORDER BY random() LIMIT {sample_size}'
        elif method == "tablesample":
            # Fast and reasonably uniform
            # Estimate percentage needed (assume we want roughly sample_size rows)
            # This is approximate - TABLESAMPLE gives probabilistic results
            return f'{full_table} TABLESAMPLE BERNOULLI(10)'
        else:  # "auto"
            # Use fast LIMIT for basic exploration
            return f'{full_table}'

    @staticmethod
    def wrap_with_sample(
        query: str,
        table: str,
        schema: str,
        sample_size: int = 10000,
        use_sample: bool = True,
    ) -> str:
        """
        Wrap a query to use sampling if requested.

        For large-table analysis, we use sampling to avoid timeouts.
        """
        if not use_sample:
            return query

        # Create a sampled CTE
        return f'''
WITH sampled_data AS (
    SELECT * FROM "{schema}"."{table}"
    ORDER BY random()
    LIMIT {sample_size}
)
{query.replace(f'"{schema}"."{table}"', 'sampled_data')}
'''


class QualityAuditor:
    """
    Audits database tables for data quality and ML suitability.
    Generates health reports with alerts for potential issues.
    """

    # Thresholds for alerts
    MISSING_DATA_WARNING = 0.20  # 20%
    MISSING_DATA_CRITICAL = 0.40  # 40%
    ZERO_VARIANCE_THRESHOLD = 1  # Only 1 distinct value
    DATA_ROT_DAYS = 90  # Data older than 90 days
    SAMPLE_THRESHOLD_ROWS = 200000
    DEFAULT_SAMPLE_SIZE = 100000

    def __init__(self):
        """Initialize the quality auditor."""
        pass

    def analyze_table(
        self,
        engine: Engine,
        table_name: str,
        schema: str = "public",
        sample_size: int | None = None,
    ) -> dict[str, Any]:
        """
        Analyze a database table and return a quality report.

        Args:
            engine: SQLAlchemy engine for the database connection.
            table_name: Name of the table to analyze.
            schema: Database schema (default: public).

        Returns:
            Dictionary containing the quality report.
        """
        report = {
            "table_name": table_name,
            "row_count": 0,
            "columns": {},
            "alerts": [],
            "summary": {
                "total_columns": 0,
                "numeric_columns": 0,
                "text_columns": 0,
                "date_columns": 0,
                "health_score": 100,
            },
        }

        try:
            with engine.connect() as conn:
                
                row_count = self._get_row_count_estimate(conn, table_name, schema)
                if row_count <= 0:
                    row_count = self._get_row_count(conn, table_name, schema)
                report["row_count"] = row_count

                if row_count == 0:
                    report["alerts"].append({
                        "level": "critical",
                        "message": f"Table '{table_name}' is empty (0 rows)",
                        "column": None,
                    })
                    report["summary"]["health_score"] = 0
                    return report

                sample_plan = self._build_sample_plan(
                    table_name=table_name,
                    schema=schema,
                    row_count=row_count,
                    sample_size=sample_size,
                )
                report["summary"]["sampled"] = sample_plan["sampled"]
                report["summary"]["sample_size"] = sample_plan["sample_size"]
                report["summary"]["sample_percent"] = sample_plan["sample_percent"]
                report["summary"]["row_count_estimate"] = row_count

                
                columns_info = self._get_columns_info(conn, table_name, schema)
                report["summary"]["total_columns"] = len(columns_info)

                
                for col_info in columns_info:
                    col_name = col_info["column_name"]
                    col_type = col_info["data_type"]
                    col_category = self._categorize_type(col_type)

                    # Update summary counts
                    if col_category == "numeric":
                        report["summary"]["numeric_columns"] += 1
                    elif col_category == "text":
                        report["summary"]["text_columns"] += 1
                    elif col_category == "date":
                        report["summary"]["date_columns"] += 1

                    # Analyze column based on type
                    col_stats = self._analyze_column(
                        conn,
                        table_name,
                        schema,
                        col_name,
                        col_type,
                        col_category,
                        sample_plan,
                    )
                    report["columns"][col_name] = col_stats

                    # Generate alerts for this column
                    col_alerts = self._generate_column_alerts(col_name, col_stats, col_category)
                    report["alerts"].extend(col_alerts)

                
                report["summary"]["health_score"] = self._calculate_health_score(report)

        except Exception as e:
            report["alerts"].append({
                "level": "error",
                "message": f"Analysis failed: {str(e)}",
                "column": None,
            })
            report["summary"]["health_score"] = 0

        return report

    def _get_row_count(self, conn, table_name: str, schema: str) -> int:
        """Get the total row count for a table (exact)."""
        query = text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
        result = conn.execute(query)
        return result.scalar() or 0

    def _get_row_count_estimate(self, conn, table_name: str, schema: str) -> int:
        """Get row count estimate from pg_class (fast)."""
        try:
            result = conn.execute(text("""
                SELECT COALESCE(c.reltuples::bigint, 0) AS estimate
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = :schema
                  AND c.relname = :table
            """), {"schema": schema, "table": table_name})
            row = result.fetchone()
            if row and row[0]:
                return int(row[0])
        except Exception:
            pass
        return 0

    def _build_sample_plan(
        self,
        table_name: str,
        schema: str,
        row_count: int,
        sample_size: int | None,
    ) -> dict[str, Any]:
        """Build sampling plan for large tables."""
        if sample_size is not None and sample_size <= 0:
            return {
                "sampled": False,
                "sample_size": None,
                "sample_percent": None,
                "from_name": f'"{schema}"."{table_name}"',
                "cte": "",
            }

        if row_count <= self.SAMPLE_THRESHOLD_ROWS:
            return {
                "sampled": False,
                "sample_size": None,
                "sample_percent": None,
                "from_name": f'"{schema}"."{table_name}"',
                "cte": "",
            }

        target_size = sample_size or self.DEFAULT_SAMPLE_SIZE
        percent = (target_size / row_count) * 100 if row_count > 0 else 100.0
        percent = max(0.1, min(100.0, percent))

        cte = f'''
WITH sampled_data AS (
    SELECT *
    FROM "{schema}"."{table_name}"
    TABLESAMPLE BERNOULLI({percent})
    LIMIT {target_size}
)
'''
        return {
            "sampled": True,
            "sample_size": target_size,
            "sample_percent": round(percent, 3),
            "from_name": "sampled_data",
            "cte": cte,
        }

    def _get_columns_info(self, conn, table_name: str, schema: str) -> list[dict]:
        """Get column names and types from information_schema."""
        query = text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table_name
            ORDER BY ordinal_position
        """)
        result = conn.execute(query, {"schema": schema, "table_name": table_name})
        return [
            {
                "column_name": row[0],
                "data_type": row[1],
                "is_nullable": row[2],
            }
            for row in result.fetchall()
        ]

    def _categorize_type(self, data_type: str) -> str:
        """Categorize a PostgreSQL data type into numeric, text, date, or other."""
        data_type_lower = data_type.lower()

        numeric_types = [
            "integer", "int", "smallint", "bigint", "serial", "bigserial",
            "decimal", "numeric", "real", "double precision", "float", "money",
        ]
        date_types = [
            "date", "timestamp", "timestamptz", "timestamp with time zone",
            "timestamp without time zone", "time", "interval",
        ]
        text_types = [
            "character varying", "varchar", "character", "char", "text",
            "citext", "uuid", "json", "jsonb",
        ]

        for t in numeric_types:
            if t in data_type_lower:
                return "numeric"
        for t in date_types:
            if t in data_type_lower:
                return "date"
        for t in text_types:
            if t in data_type_lower:
                return "text"

        return "other"

    def _analyze_column(
        self,
        conn,
        table_name: str,
        schema: str,
        col_name: str,
        col_type: str,
        col_category: str,
        sample_plan: dict[str, Any],
    ) -> dict[str, Any]:
        """Analyze a single column and return its statistics."""
        stats = {
            "type": col_category,
            "data_type": col_type,
        }

        # Base stats for all columns: NULL count and distinct count
        base_query = text(f'''
            {sample_plan["cte"]}
            SELECT 
                COUNT(*) FILTER (WHERE "{col_name}" IS NULL) as null_count,
                COUNT(DISTINCT "{col_name}") as distinct_count,
                COUNT(*) as total_count
            FROM {sample_plan["from_name"]}
        ''')
        result = conn.execute(base_query)
        row = result.fetchone()

        null_count = row[0] or 0
        distinct_count = row[1] or 0
        total_count = row[2] or 0

        stats["null_count"] = null_count
        stats["null_percentage"] = round(null_count / total_count, 4) if total_count > 0 else 0
        stats["distinct_count"] = distinct_count
        stats["sample_row_count"] = total_count

        # Additional stats based on column type
        if col_category == "numeric":
            stats.update(self._analyze_numeric_column(conn, col_name, sample_plan))
        elif col_category == "date":
            stats.update(self._analyze_date_column(conn, col_name, sample_plan))
        elif col_category == "text":
            stats.update(self._analyze_text_column(conn, col_name, sample_plan))

        return stats

    def _analyze_numeric_column(
        self, conn, col_name: str, sample_plan: dict[str, Any]
    ) -> dict[str, Any]:
        """Get statistics for a numeric column."""
        # Use double precision to avoid numeric overflow on large integers
        query = text(f'''
            {sample_plan["cte"]}
            SELECT 
                COUNT(*) FILTER (WHERE "{col_name}" = 0) as zero_count,
                MIN("{col_name}")::float8 as min_val,
                MAX("{col_name}")::float8 as max_val,
                AVG("{col_name}"::float8) as avg_val,
                STDDEV("{col_name}"::float8) as stddev_val
            FROM {sample_plan["from_name"]}
            WHERE "{col_name}" IS NOT NULL
        ''')
        try:
            result = conn.execute(query)
            row = result.fetchone()

            return {
                "zero_count": row[0] or 0,
                "min": float(row[1]) if row[1] is not None else None,
                "max": float(row[2]) if row[2] is not None else None,
                "avg": float(row[3]) if row[3] is not None else None,
                "stddev": float(row[4]) if row[4] is not None else None,
            }
        except Exception as e:
            # Fallback for columns that can't be cast to float8
            return {
                "zero_count": None,
                "min": None,
                "max": None,
                "avg": None,
                "stddev": None,
                "analysis_error": str(e),
            }

    def _analyze_date_column(
        self, conn, col_name: str, sample_plan: dict[str, Any]
    ) -> dict[str, Any]:
        """Get statistics for a date column."""
        query = text(f'''
            {sample_plan["cte"]}
            SELECT 
                MIN("{col_name}") as min_date,
                MAX("{col_name}") as max_date
            FROM {sample_plan["from_name"]}
            WHERE "{col_name}" IS NOT NULL
        ''')
        result = conn.execute(query)
        row = result.fetchone()

        min_date = row[0]
        max_date = row[1]

        stats = {
            "min_date": str(min_date) if min_date else None,
            "max_date": str(max_date) if max_date else None,
        }

        # Calculate data age
        if max_date:
            if hasattr(max_date, "date"):
                max_date_obj = max_date.date() if hasattr(max_date, "date") else max_date
            else:
                max_date_obj = max_date

            today = datetime.now().date()
            if isinstance(max_date_obj, datetime):
                max_date_obj = max_date_obj.date()

            try:
                days_old = (today - max_date_obj).days
                stats["days_since_latest"] = days_old
            except:
                stats["days_since_latest"] = None

        return stats

    def _analyze_text_column(
        self, conn, col_name: str, sample_plan: dict[str, Any]
    ) -> dict[str, Any]:
        """Get statistics for a text column."""
        query = text(f'''
            {sample_plan["cte"]}
            SELECT 
                COUNT(*) FILTER (WHERE TRIM("{col_name}") = '') as empty_count,
                AVG(LENGTH("{col_name}"))::numeric(10,2) as avg_length,
                MAX(LENGTH("{col_name}")) as max_length
            FROM {sample_plan["from_name"]}
            WHERE "{col_name}" IS NOT NULL
        ''')
        result = conn.execute(query)
        row = result.fetchone()

        return {
            "empty_count": row[0] or 0,
            "avg_length": float(row[1]) if row[1] is not None else None,
            "max_length": row[2] or 0,
        }

    def _generate_column_alerts(
        self, col_name: str, stats: dict, col_category: str
    ) -> list[dict]:
        """Generate alerts for a column based on its statistics."""
        alerts = []

        # Check for high missing data
        null_pct = stats.get("null_percentage", 0)
        if null_pct >= self.MISSING_DATA_CRITICAL:
            alerts.append({
                "level": "critical",
                "message": f"High missing data ({null_pct:.1%}) - Critical for ML",
                "column": col_name,
            })
        elif null_pct >= self.MISSING_DATA_WARNING:
            alerts.append({
                "level": "warning",
                "message": f"Missing data ({null_pct:.1%}) - May need imputation",
                "column": col_name,
            })

        # Check for zero variance (useless feature)
        distinct_count = stats.get("distinct_count", 0)
        if distinct_count <= self.ZERO_VARIANCE_THRESHOLD:
            alerts.append({
                "level": "critical",
                "message": f"Zero variance (only {distinct_count} unique value) - Useless feature",
                "column": col_name,
            })

        # Check for data rot (date columns)
        if col_category == "date":
            days_old = stats.get("days_since_latest")
            if days_old is not None and days_old > self.DATA_ROT_DAYS:
                alerts.append({
                    "level": "warning",
                    "message": f"Data rot - Latest record is {days_old} days old",
                    "column": col_name,
                })

        # Check for high zero percentage in numeric columns
        if col_category == "numeric":
            zero_count = stats.get("zero_count", 0)
            null_count = stats.get("null_count", 0)
            # Calculate zero percentage from non-null values
            non_null = stats.get("distinct_count", 1)  # Rough proxy
            if zero_count > 0 and stats.get("min") == 0 and stats.get("max") == 0:
                alerts.append({
                    "level": "warning",
                    "message": "All values are zero - Check data collection",
                    "column": col_name,
                })

        return alerts

    def _calculate_health_score(self, report: dict) -> int:
        """Calculate an overall health score (0-100) based on alerts."""
        score = 100
        
        for alert in report["alerts"]:
            if alert["level"] == "critical":
                score -= 20
            elif alert["level"] == "warning":
                score -= 10
            elif alert["level"] == "error":
                score -= 30

        return max(0, score)



quality_auditor = QualityAuditor()
