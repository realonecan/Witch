"""
Database Service
Handles database connections, schema discovery, and relationship detection.

"""

import re
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import quote_plus

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool


# =============================================================================
# 1.1 CONNECT - Connection Management
# =============================================================================


class ConnectionConfig:
    """Configuration for database connections with safety limits."""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        db_type: str = "postgres",
        ssl_mode: str = "prefer",
        ssl_cert_path: str | None = None,
        schema_whitelist: list[str] | None = None,
        statement_timeout_seconds: int = 30,
        max_rows_default: int = 100000,
        pool_size: int = 5,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.db_type = db_type
        self.ssl_mode = ssl_mode
        self.ssl_cert_path = ssl_cert_path
        self.schema_whitelist = schema_whitelist or ["public"]
        self.statement_timeout_seconds = statement_timeout_seconds
        self.max_rows_default = max_rows_default
        self.pool_size = pool_size

    def build_connection_url(self) -> str:
        """Build database connection URL with proper escaping."""
        # URL-encode password to handle special characters
        encoded_password = quote_plus(self.password)

        if self.db_type == "postgres":
            base_url = f"postgresql://{self.user}:{encoded_password}@{self.host}:{self.port}/{self.database}"

            # Add SSL parameters
            params = []
            if self.ssl_mode and self.ssl_mode != "disable":
                params.append(f"sslmode={self.ssl_mode}")
            if self.ssl_cert_path:
                params.append(f"sslrootcert={self.ssl_cert_path}")

            if params:
                base_url += "?" + "&".join(params)

            return base_url
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")


class DBConnector:
    """
    Manages database connections with safety controls.
    Implements 1.1 CONNECT.
    """

    @staticmethod
    def create_engine_from_config(config: ConnectionConfig) -> Engine:
        """
        Create SQLAlchemy engine with safety limits.

        Args:
            config: Connection configuration.

        Returns:
            SQLAlchemy Engine with configured limits.
        """
        url = config.build_connection_url()

        # Engine options
        connect_args = {}
        if config.db_type == "postgres":
            # Set statement timeout at connection level
            connect_args["options"] = f"-c statement_timeout={config.statement_timeout_seconds * 1000}"

        engine = create_engine(
            url,
            poolclass=QueuePool,
            pool_size=config.pool_size,
            max_overflow=2,
            pool_pre_ping=True,
            connect_args=connect_args,
        )

        return engine

    @staticmethod
    def test_connection(engine: Engine) -> dict[str, Any]:
        """
        Test connection and get database info.

        Returns:
            Dictionary with version and accessible schemas.
        """
        with engine.connect() as conn:
            # Test basic connectivity
            conn.execute(text("SELECT 1"))

            # Get database version
            result = conn.execute(text("SELECT version()"))
            version = result.scalar()

            # Get accessible schemas
            result = conn.execute(
                text("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                    ORDER BY schema_name
                """)
            )
            schemas = [row[0] for row in result.fetchall()]

        return {
            "version": version,
            "accessible_schemas": schemas,
        }

    @staticmethod
    def get_engine(db_url: str) -> Engine:
        """
        Legacy method: Create engine from URL string.
        Kept for backward compatibility.
        """
        engine = create_engine(db_url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine


# =============================================================================
# 1.2 DISCOVER - Schema Discovery
# =============================================================================


# Patterns for detecting date-like columns
DATE_COLUMN_PATTERNS = [
    r".*_date$",
    r".*_dt$",
    r".*_time$",
    r".*_at$",
    r"^date_.*",
    r"^time_.*",
    r"^created.*",
    r"^updated.*",
    r"^modified.*",
    r"^timestamp.*",
    r"^event_.*",
]

DATE_COLUMN_REGEX = re.compile("|".join(DATE_COLUMN_PATTERNS), re.IGNORECASE)

# Date-like SQL types
DATE_TYPES = [
    "date",
    "timestamp",
    "timestamptz",
    "timestamp with time zone",
    "timestamp without time zone",
]


class SchemaDiscovery:
    """
    Discovers database schema including tables, views, columns, and constraints.
    Implements 1.2 DISCOVER.
    """

    @staticmethod
    def discover_tables(
        engine: Engine,
        schemas: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Discover all tables and views in the database.

        Args:
            engine: SQLAlchemy engine.
            schemas: List of schemas to scan (default: ["public"]).

        Returns:
            Dictionary with tables, views, and their metadata.
        """
        schemas = schemas or ["public"]
        inspector = inspect(engine)
        all_tables = []

        with engine.connect() as conn:
            for schema in schemas:
                # Get base tables
                table_names = inspector.get_table_names(schema=schema)
                for table_name in table_names:
                    table_info = SchemaDiscovery._analyze_table(
                        conn, inspector, schema, table_name, "table"
                    )
                    all_tables.append(table_info)

                # Get views
                try:
                    view_names = inspector.get_view_names(schema=schema)
                    for view_name in view_names:
                        view_info = SchemaDiscovery._analyze_table(
                            conn, inspector, schema, view_name, "view"
                        )
                        all_tables.append(view_info)
                except Exception:
                    # Some DBs may not support view introspection
                    pass

                # Get materialized views (PostgreSQL specific)
                try:
                    result = conn.execute(
                        text("""
                            SELECT matviewname 
                            FROM pg_matviews 
                            WHERE schemaname = :schema
                        """),
                        {"schema": schema},
                    )
                    for row in result.fetchall():
                        mv_info = SchemaDiscovery._analyze_table(
                            conn, inspector, schema, row[0], "materialized_view"
                        )
                        all_tables.append(mv_info)
                except Exception:
                    pass

        return {
            "tables": all_tables,
            "total_count": len(all_tables),
            "schemas_scanned": schemas,
        }

    @staticmethod
    def _analyze_table(
        conn,
        inspector,
        schema: str,
        table_name: str,
        table_type: str,
    ) -> dict[str, Any]:
        """Analyze a single table/view and return metadata."""

        # Get columns
        try:
            columns = inspector.get_columns(table_name, schema=schema)
        except Exception:
            columns = []

        # Get primary key
        try:
            pk = inspector.get_pk_constraint(table_name, schema=schema)
            pk_columns = pk.get("constrained_columns", []) if pk else []
        except Exception:
            pk_columns = []

        # Get unique constraints
        try:
            unique_constraints = inspector.get_unique_constraints(table_name, schema=schema)
        except Exception:
            unique_constraints = []

        # Get row count estimate (fast, from statistics)
        row_count_estimate = SchemaDiscovery._get_row_count_estimate(
            conn, schema, table_name
        )

        # Analyze columns
        column_details = []
        date_columns = []

        for col in columns:
            col_name = col["name"]
            col_type = str(col["type"]).lower()
            nullable = col.get("nullable", True)

            is_pk = col_name in pk_columns
            is_unique = any(
                col_name in uc.get("column_names", []) for uc in unique_constraints
            )

            # Detect date-like columns
            is_date_type = any(dt in col_type for dt in DATE_TYPES)
            is_date_pattern = bool(DATE_COLUMN_REGEX.match(col_name))
            is_date_like = is_date_type or is_date_pattern

            if is_date_like:
                date_columns.append(col_name)

            column_details.append({
                "name": col_name,
                "type": str(col["type"]),
                "nullable": nullable,
                "is_primary_key": is_pk,
                "is_unique": is_unique,
                "is_date_like": is_date_like,
            })

        # Get freshness for date columns (sample-based)
        freshness = {}
        if date_columns and row_count_estimate > 0:
            freshness = SchemaDiscovery._check_freshness(
                conn, schema, table_name, date_columns[:3]  # Limit to 3 date columns
            )

        return {
            "schema": schema,
            "name": table_name,
            "type": table_type,
            "row_count_estimate": row_count_estimate,
            "column_count": len(column_details),
            "columns": column_details,
            "primary_key": pk_columns,
            "unique_constraints": [uc.get("column_names", []) for uc in unique_constraints],
            "date_columns": date_columns,
            "freshness": freshness,
        }

    @staticmethod
    def _get_row_count_estimate(conn, schema: str, table_name: str) -> int:
        """Get estimated row count from PostgreSQL statistics (fast)."""
        try:
            result = conn.execute(
                text("""
                    SELECT n_live_tup 
                    FROM pg_stat_user_tables 
                    WHERE schemaname = :schema AND relname = :table
                """),
                {"schema": schema, "table": table_name},
            )
            row = result.fetchone()
            return int(row[0]) if row and row[0] else 0
        except Exception:
            return 0

    @staticmethod
    def _check_freshness(
        conn,
        schema: str,
        table_name: str,
        date_columns: list[str],
    ) -> dict[str, Any]:
        """Check freshness of date columns (max date, days old)."""
        freshness = {}
        today = datetime.now().date()

        for col in date_columns:
            try:
                # Try to get max date - handle both date and varchar columns
                result = conn.execute(
                    text(f'''
                        SELECT MAX("{col}")::text 
                        FROM "{schema}"."{table_name}" 
                        WHERE "{col}" IS NOT NULL
                        LIMIT 1
                    ''')
                )
                row = result.fetchone()
                if row and row[0]:
                    max_date_str = str(row[0])[:10]  # Take first 10 chars (YYYY-MM-DD)
                    try:
                        max_date = datetime.strptime(max_date_str, "%Y-%m-%d").date()
                        days_old = (today - max_date).days
                        freshness[col] = {
                            "max_date": max_date_str,
                            "days_old": days_old,
                            "is_stale": days_old > 90,
                        }
                    except ValueError:
                        freshness[col] = {"error": "Could not parse date"}
            except Exception as e:
                freshness[col] = {"error": str(e)[:100]}

        return freshness


# =============================================================================
# 1.3 RELATIONSHIPS - Relationship Detection
# =============================================================================


class RelationshipDetector:
    """
    Detects relationships between tables using FK metadata and inference.
    Implements 1.3 RELATIONSHIPS.
    """

    @staticmethod
    def detect_relationships(
        engine: Engine,
        tables: list[dict[str, Any]],
        schemas: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Detect relationships between tables.

        Args:
            engine: SQLAlchemy engine.
            tables: List of table metadata from discover_tables.
            schemas: Schemas to analyze.

        Returns:
            Dictionary with confirmed and suggested relationships.
        """
        schemas = schemas or ["public"]
        inspector = inspect(engine)

        confirmed_relationships = []
        suggested_relationships = []

        # Build lookup for quick access
        table_lookup = {(t["schema"], t["name"]): t for t in tables}

        with engine.connect() as conn:
            # Layer 1: Explicit Foreign Keys
            for schema in schemas:
                for table in tables:
                    if table["schema"] != schema:
                        continue

                    try:
                        fks = inspector.get_foreign_keys(table["name"], schema=schema)
                        for fk in fks:
                            ref_schema = fk.get("referred_schema") or schema
                            ref_table = fk.get("referred_table")
                            constrained_cols = fk.get("constrained_columns", [])
                            referred_cols = fk.get("referred_columns", [])

                            if ref_table and constrained_cols:
                                confirmed_relationships.append({
                                    "type": "confirmed",
                                    "parent_schema": ref_schema,
                                    "parent_table": ref_table,
                                    "parent_column": referred_cols[0] if referred_cols else "id",
                                    "child_schema": schema,
                                    "child_table": table["name"],
                                    "child_column": constrained_cols[0],
                                    "cardinality": "one-to-many",
                                    "confidence": 1.0,
                                })
                    except Exception:
                        pass

            # Layer 2: Inferred Relationships (if few confirmed FKs)
            if len(confirmed_relationships) < len(tables) // 2:
                suggested = RelationshipDetector._infer_relationships(
                    conn, tables, table_lookup
                )
                suggested_relationships.extend(suggested)

        return {
            "confirmed": confirmed_relationships,
            "suggested": suggested_relationships,
            "total_confirmed": len(confirmed_relationships),
            "total_suggested": len(suggested_relationships),
        }

    @staticmethod
    def _infer_relationships(
        conn,
        tables: list[dict[str, Any]],
        table_lookup: dict,
    ) -> list[dict[str, Any]]:
        """Infer relationships based on column name patterns and data."""
        suggested = []
        processed_pairs = set()

        # ID-like patterns
        id_patterns = [
            (r"^id$", "id"),
            (r"^(.+)_id$", r"\1"),
            (r"^(.+)id$", r"\1"),
            (r"^fk_(.+)$", r"\1"),
        ]

        for table in tables:
            schema = table["schema"]
            table_name = table["name"]

            for col in table["columns"]:
                col_name = col["name"].lower()

                # Skip if this is a primary key (likely not a FK)
                if col["is_primary_key"] and col_name == "id":
                    continue

                # Check if column name suggests a relationship
                for pattern, extract in id_patterns:
                    match = re.match(pattern, col_name, re.IGNORECASE)
                    if match:
                        # Extract potential parent table name
                        if extract == "id":
                            continue  # Skip bare "id" columns
                        parent_hint = match.expand(extract).lower()

                        # Look for matching parent table
                        for candidate in tables:
                            if candidate["name"] == table_name:
                                continue

                            cand_name = candidate["name"].lower()

                            # Check if candidate table name matches hint
                            if (
                                cand_name == parent_hint
                                or cand_name == parent_hint + "s"
                                or cand_name == parent_hint + "es"
                                or cand_name.endswith("_" + parent_hint)
                                or parent_hint in cand_name
                            ):
                                # Check if candidate has a matching PK or unique column
                                parent_key = None
                                for cand_col in candidate["columns"]:
                                    if cand_col["is_primary_key"] or cand_col["is_unique"]:
                                        # Type compatibility check
                                        if RelationshipDetector._types_compatible(
                                            col["type"], cand_col["type"]
                                        ):
                                            parent_key = cand_col["name"]
                                            break

                                if parent_key:
                                    pair_key = tuple(sorted([
                                        f"{schema}.{table_name}.{col_name}",
                                        f"{candidate['schema']}.{candidate['name']}.{parent_key}",
                                    ]))

                                    if pair_key not in processed_pairs:
                                        processed_pairs.add(pair_key)

                                        # Calculate confidence
                                        confidence = RelationshipDetector._calculate_confidence(
                                            conn, schema, table_name, col_name,
                                            candidate["schema"], candidate["name"], parent_key
                                        )

                                        if confidence > 0.3:
                                            suggested.append({
                                                "type": "suggested",
                                                "parent_schema": candidate["schema"],
                                                "parent_table": candidate["name"],
                                                "parent_column": parent_key,
                                                "child_schema": schema,
                                                "child_table": table_name,
                                                "child_column": col["name"],
                                                "cardinality": "one-to-many",
                                                "confidence": confidence,
                                                "reason": f"Column '{col['name']}' matches pattern for '{candidate['name']}'",
                                            })

        # Sort by confidence
        suggested.sort(key=lambda x: x["confidence"], reverse=True)
        return suggested

    @staticmethod
    def _types_compatible(type1: str, type2: str) -> bool:
        """Check if two column types are compatible for a relationship."""
        t1 = type1.lower()
        t2 = type2.lower()

        # Integer types
        int_types = ["integer", "int", "bigint", "smallint", "serial", "bigserial"]
        if any(it in t1 for it in int_types) and any(it in t2 for it in int_types):
            return True

        # String types
        str_types = ["varchar", "character varying", "text", "char"]
        if any(st in t1 for st in str_types) and any(st in t2 for st in str_types):
            return True

        # UUID
        if "uuid" in t1 and "uuid" in t2:
            return True

        return False

    @staticmethod
    def _calculate_confidence(
        conn,
        child_schema: str,
        child_table: str,
        child_column: str,
        parent_schema: str,
        parent_table: str,
        parent_column: str,
    ) -> float:
        """Calculate confidence score for an inferred relationship."""
        try:
            # Check match rate on sample
            result = conn.execute(
                text(f'''
                    WITH child_sample AS (
                        SELECT "{child_column}" as val
                        FROM "{child_schema}"."{child_table}"
                        WHERE "{child_column}" IS NOT NULL
                        LIMIT 1000
                    )
                    SELECT 
                        COUNT(*) as total,
                        COUNT(p."{parent_column}") as matched
                    FROM child_sample c
                    LEFT JOIN "{parent_schema}"."{parent_table}" p 
                        ON c.val::text = p."{parent_column}"::text
                ''')
            )
            row = result.fetchone()
            if row and row[0] > 0:
                match_rate = row[1] / row[0]
                return round(match_rate, 2)
        except Exception:
            pass

        return 0.5  # Default medium confidence if check fails


# =============================================================================
# 1.5 AVAILABILITY - Availability Assessment
# =============================================================================


class AvailabilityChecker:
    """
    Assesses data availability, freshness, and access permissions.
    Implements 1.5 AVAILABILITY.
    """

    @staticmethod
    def check_availability(
        engine: Engine,
        tables: list[dict[str, Any]],
        freshness_threshold_days: int = 90,
    ) -> dict[str, Any]:
        """
        Check availability and freshness of selected tables.

        Args:
            engine: SQLAlchemy engine.
            tables: List of table metadata to check.
            freshness_threshold_days: Days after which data is considered stale.

        Returns:
            Availability report for each table.
        """
        reports = []

        with engine.connect() as conn:
            for table in tables:
                report = AvailabilityChecker._check_table(
                    conn, table, freshness_threshold_days
                )
                reports.append(report)

        # Summary
        ready_count = sum(1 for r in reports if r["status"] == "ready")
        warning_count = sum(1 for r in reports if r["status"] == "warning")
        blocked_count = sum(1 for r in reports if r["status"] == "blocked")

        return {
            "reports": reports,
            "summary": {
                "total": len(reports),
                "ready": ready_count,
                "warning": warning_count,
                "blocked": blocked_count,
            },
        }

    @staticmethod
    def _check_table(
        conn,
        table: dict[str, Any],
        freshness_threshold_days: int,
    ) -> dict[str, Any]:
        """Check availability for a single table."""
        schema = table["schema"]
        table_name = table["name"]
        issues = []

        # Check access permission
        access_ok = False
        try:
            conn.execute(
                text(f'SELECT 1 FROM "{schema}"."{table_name}" LIMIT 1')
            )
            access_ok = True
        except Exception as e:
            issues.append({
                "type": "access_denied",
                "message": f"Cannot access table: {str(e)[:100]}",
            })

        # Check if empty
        row_count = table.get("row_count_estimate", 0)
        if row_count == 0 and access_ok:
            # Verify with actual check
            try:
                result = conn.execute(
                    text(f'SELECT 1 FROM "{schema}"."{table_name}" LIMIT 1')
                )
                if result.fetchone() is None:
                    issues.append({
                        "type": "empty_table",
                        "message": "Table has no data",
                    })
            except Exception:
                pass

        # Check freshness per date column
        freshness_details = []
        for col, freshness in table.get("freshness", {}).items():
            if isinstance(freshness, dict) and "days_old" in freshness:
                days_old = freshness["days_old"]
                is_stale = days_old > freshness_threshold_days

                freshness_details.append({
                    "column": col,
                    "max_date": freshness.get("max_date"),
                    "days_old": days_old,
                    "is_stale": is_stale,
                })

                if is_stale:
                    issues.append({
                        "type": "stale_data",
                        "message": f"Column '{col}' is {days_old} days old (threshold: {freshness_threshold_days})",
                    })

        # No date columns detected
        if not table.get("date_columns"):
            issues.append({
                "type": "no_date_column",
                "message": "No date-like column detected (freshness unknown)",
            })

        # Determine overall status
        if any(i["type"] == "access_denied" for i in issues):
            status = "blocked"
        elif any(i["type"] in ["empty_table", "stale_data"] for i in issues):
            status = "warning"
        else:
            status = "ready"

        return {
            "schema": schema,
            "table": table_name,
            "row_count_estimate": row_count,
            "access": "ok" if access_ok else "denied",
            "freshness": freshness_details,
            "issues": issues,
            "status": status,
        }


# =============================================================================
# 1.4 IDENTIFY - Relevant Data Identification
# =============================================================================


# Use case templates
USE_CASE_TEMPLATES = {
    "churn": {
        "description": "Customer churn prediction",
        "entity_hints": ["customer", "client", "user", "account"],
        "label_hints": ["status", "state", "churn", "active", "closed"],
        "feature_hints": ["transaction", "balance", "activity", "payment"],
        "time_hints": ["date", "created", "opened", "closed", "last"],
    },
    "fraud": {
        "description": "Fraud detection",
        "entity_hints": ["transaction", "event", "order", "payment"],
        "label_hints": ["fraud", "suspicious", "flag", "alert", "status"],
        "feature_hints": ["amount", "merchant", "location", "device", "velocity"],
        "time_hints": ["timestamp", "event_time", "created", "processed"],
    },
    "default": {
        "description": "Credit default prediction",
        "entity_hints": ["loan", "credit", "account", "application"],
        "label_hints": ["default", "delinquent", "overdue", "status", "dpd"],
        "feature_hints": ["payment", "balance", "income", "bureau", "score"],
        "time_hints": ["origination", "disbursement", "due_date", "payment_date"],
    },
}


class RelevanceIdentifier:
    """
    Identifies relevant tables and columns for a given use case.
    Implements 1.4 IDENTIFY.
    """

    @staticmethod
    def suggest_relevant_data(
        tables: list[dict[str, Any]],
        use_case: str | None = None,
        custom_description: str | None = None,
    ) -> dict[str, Any]:
        """
        Suggest relevant tables and columns for a use case.

        Args:
            tables: List of table metadata.
            use_case: Predefined use case (churn, fraud, default).
            custom_description: Custom description if no predefined use case.

        Returns:
            Suggestions for entity, label, features, and time columns.
        """
        if use_case and use_case in USE_CASE_TEMPLATES:
            template = USE_CASE_TEMPLATES[use_case]
        else:
            # Default to churn as fallback
            template = USE_CASE_TEMPLATES["churn"]

        suggestions = {
            "use_case": use_case or "custom",
            "description": template["description"] if use_case else custom_description,
            "entity_table": None,
            "label_candidates": [],
            "feature_candidates": [],
            "time_candidates": [],
        }

        entity_scores = []
        label_scores = []
        feature_scores = []
        time_scores = []

        for table in tables:
            table_name = table["name"].lower()
            columns = table.get("columns", [])

            # Score for entity table
            entity_score = 0
            for hint in template["entity_hints"]:
                if hint in table_name:
                    entity_score += 10
            # Prefer tables with clear PK
            if table.get("primary_key"):
                entity_score += 5
            if entity_score > 0:
                entity_scores.append((table, entity_score))

            # Score columns for label, features, time
            for col in columns:
                col_name = col["name"].lower()

                # Label candidates
                label_score = 0
                for hint in template["label_hints"]:
                    if hint in col_name:
                        label_score += 10
                if label_score > 0:
                    label_scores.append({
                        "table": table["name"],
                        "column": col["name"],
                        "type": col["type"],
                        "score": label_score,
                    })

                # Time candidates
                if col.get("is_date_like"):
                    time_score = 5
                    for hint in template["time_hints"]:
                        if hint in col_name:
                            time_score += 5
                    time_scores.append({
                        "table": table["name"],
                        "column": col["name"],
                        "type": col["type"],
                        "score": time_score,
                    })

            # Score for feature tables (not entity, has relevant columns)
            feature_score = 0
            for hint in template["feature_hints"]:
                if hint in table_name:
                    feature_score += 10
                for col in columns:
                    if hint in col["name"].lower():
                        feature_score += 2
            if feature_score > 0:
                feature_scores.append((table, feature_score))

        # Select best candidates
        entity_scores.sort(key=lambda x: x[1], reverse=True)
        if entity_scores:
            suggestions["entity_table"] = {
                "name": entity_scores[0][0]["name"],
                "schema": entity_scores[0][0]["schema"],
                "primary_key": entity_scores[0][0].get("primary_key"),
                "score": entity_scores[0][1],
            }

        label_scores.sort(key=lambda x: x["score"], reverse=True)
        suggestions["label_candidates"] = label_scores[:5]

        feature_scores.sort(key=lambda x: x[1], reverse=True)
        suggestions["feature_candidates"] = [
            {"name": t["name"], "schema": t["schema"], "score": s}
            for t, s in feature_scores[:5]
        ]

        time_scores.sort(key=lambda x: x["score"], reverse=True)
        suggestions["time_candidates"] = time_scores[:5]

        return suggestions


# =============================================================================
# Legacy Compatibility
# =============================================================================


class DBConnectorLegacy:
    """Legacy interface for backward compatibility."""

    @staticmethod
    def get_engine(db_url: str) -> Engine:
        return DBConnector.get_engine(db_url)

    @staticmethod
    def scan_schema(engine: Engine, schema: str = "public") -> dict[str, Any]:
        """
        Legacy method: Scan schema and return in old format.
        """
        discovery = SchemaDiscovery.discover_tables(engine, [schema])

        # Convert to old format
        tables_detail = {}
        table_list = []
        schema_lines = []

        for table in discovery["tables"]:
            table_name = table["name"]
            table_list.append(table_name)

            column_info = []
            column_strs = []

            for col in table["columns"]:
                column_info.append({
                    "name": col["name"],
                    "type": col["type"],
                    "nullable": col["nullable"],
                })
                column_strs.append(f"{col['name']} ({col['type']})")

            tables_detail[table_name] = column_info
            schema_lines.append(f"Table '{table_name}': [{', '.join(column_strs)}]")

        return {
            "schema_summary": "\n".join(schema_lines),
            "table_list": table_list,
            "tables_detail": tables_detail,
        }

    @staticmethod
    def build_connection_url(
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        db_type: str = "postgres",
    ) -> str:
        config = ConnectionConfig(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            db_type=db_type,
        )
        return config.build_connection_url()



db_connector = DBConnectorLegacy()
