"""
Join Service
Provides join key suggestions and fast join diagnostics for exploration.
"""

from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.services.grain_service import validate_identifier


def _type_group(data_type: str) -> str:
    t = (data_type or "").lower()
    numeric = ["int", "integer", "bigint", "smallint", "serial", "numeric", "decimal", "real", "double", "float"]
    text_types = ["char", "text", "varchar", "uuid", "citext"]
    date_types = ["date", "time", "timestamp"]

    if any(x in t for x in numeric):
        return "numeric"
    if any(x in t for x in date_types):
        return "date"
    if any(x in t for x in text_types):
        return "text"
    return "other"


def _is_id_like(name: str) -> bool:
    lower = name.lower()
    return lower == "id" or lower.endswith("_id") or lower.endswith("_key") or lower.endswith("_code")


def _strip_table_prefix(name: str, table: str) -> str:
    lower = name.lower()
    table_lower = table.lower()
    if lower.startswith(f"{table_lower}_"):
        return lower[len(table_lower) + 1 :]
    return lower


def _root_name(name: str) -> str:
    lower = name.lower()
    for suffix in ["_id", "_key", "_code"]:
        if lower.endswith(suffix):
            return lower[: -len(suffix)]
    if lower.endswith("id") and len(lower) > 2:
        return lower[:-2]
    return lower


def _get_row_estimate(conn, schema: str, table: str) -> int:
    try:
        result = conn.execute(text("""
            SELECT COALESCE(c.reltuples::bigint, 0) AS estimate
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = :schema
              AND c.relname = :table
        """), {"schema": schema, "table": table})
        row = result.fetchone()
        return int(row[0]) if row and row[0] else 0
    except Exception:
        return 0


def fetch_fk_graph(
    engine: Engine,
    schema: str = "public",
    tables: list[str] | None = None,
) -> dict[str, Any]:
    validate_identifier(schema, "schema")

    with engine.connect() as conn:
        try:
            conn.execute(text("SET statement_timeout = 30000"))
        except Exception:
            pass

        fk_sql = text("""
            WITH fk AS (
                SELECT con.oid AS con_oid,
                       con.conname AS constraint_name,
                       con.conrelid AS left_oid,
                       con.confrelid AS right_oid,
                       con.conkey AS left_attnums,
                       con.confkey AS right_attnums
                FROM pg_constraint con
                JOIN pg_namespace ns ON ns.oid = con.connamespace
                WHERE con.contype = 'f'
                  AND ns.nspname = :schema
            ),
            left_cols AS (
                SELECT fk.con_oid,
                       array_agg(att.attname ORDER BY u.ordinality) AS columns,
                       array_agg(u.attnum::int ORDER BY u.ordinality) AS attnums
                FROM fk
                JOIN LATERAL unnest(fk.left_attnums) WITH ORDINALITY AS u(attnum, ordinality) ON true
                JOIN pg_attribute att ON att.attrelid = fk.left_oid AND att.attnum = u.attnum
                GROUP BY fk.con_oid
            ),
            right_cols AS (
                SELECT fk.con_oid,
                       array_agg(att.attname ORDER BY u.ordinality) AS columns,
                       array_agg(u.attnum::int ORDER BY u.ordinality) AS attnums
                FROM fk
                JOIN LATERAL unnest(fk.right_attnums) WITH ORDINALITY AS u(attnum, ordinality) ON true
                JOIN pg_attribute att ON att.attrelid = fk.right_oid AND att.attnum = u.attnum
                GROUP BY fk.con_oid
            ),
            uniq AS (
                SELECT fk.con_oid,
                       bool_or(uc.contype IN ('p', 'u') AND (uc.conkey::int[] @> rc.attnums)) AS is_unique,
                       bool_or(uc.contype = 'p' AND (uc.conkey::int[] @> rc.attnums)) AS is_primary
                FROM fk
                JOIN right_cols rc ON rc.con_oid = fk.con_oid
                LEFT JOIN pg_constraint uc
                  ON uc.conrelid = fk.right_oid
                 AND uc.contype IN ('p', 'u')
                GROUP BY fk.con_oid
            )
            SELECT fk.constraint_name,
                   l_schema.nspname AS left_schema,
                   l_table.relname AS left_table,
                   r_schema.nspname AS right_schema,
                   r_table.relname AS right_table,
                   lc.columns AS left_columns,
                   rc.columns AS right_columns,
                   COALESCE(uniq.is_unique, false) AS is_unique,
                   COALESCE(uniq.is_primary, false) AS is_primary,
                   l_table.reltuples::bigint AS left_estimate,
                   r_table.reltuples::bigint AS right_estimate
            FROM fk
            JOIN pg_class l_table ON l_table.oid = fk.left_oid
            JOIN pg_namespace l_schema ON l_schema.oid = l_table.relnamespace
            JOIN pg_class r_table ON r_table.oid = fk.right_oid
            JOIN pg_namespace r_schema ON r_schema.oid = r_table.relnamespace
            JOIN left_cols lc ON lc.con_oid = fk.con_oid
            JOIN right_cols rc ON rc.con_oid = fk.con_oid
            LEFT JOIN uniq ON uniq.con_oid = fk.con_oid
            WHERE l_schema.nspname = :schema
              AND r_schema.nspname = :schema
            ORDER BY left_table, right_table, constraint_name
        """)

        fk_rows = conn.execute(fk_sql, {"schema": schema}).fetchall()

        edges = []
        for row in fk_rows:
            edges.append({
                "id": f'{row.constraint_name}:{row.left_table}:{row.right_table}',
                "constraint_name": row.constraint_name,
                "left_schema": row.left_schema,
                "left_table": row.left_table,
                "right_schema": row.right_schema,
                "right_table": row.right_table,
                "left_columns": list(row.left_columns or []),
                "right_columns": list(row.right_columns or []),
                "is_unique": bool(row.is_unique),
                "is_primary": bool(row.is_primary),
                "left_estimate": int(row.left_estimate or 0),
                "right_estimate": int(row.right_estimate or 0),
            })

        nodes = []
        table_set = set()
        if tables:
            table_set.update(tables)
        for edge in edges:
            table_set.add(edge["left_table"])
            table_set.add(edge["right_table"])

        if table_set:
            table_sql = text("""
                SELECT relname, COALESCE(reltuples::bigint, 0) AS estimate
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = :schema
            """)
            table_rows = conn.execute(table_sql, {"schema": schema}).fetchall()
            estimates = {row.relname: int(row.estimate or 0) for row in table_rows}
            for table in sorted(table_set):
                nodes.append({
                    "id": table,
                    "table_name": table,
                    "schema": schema,
                    "row_estimate": estimates.get(table, 0),
                })

        try:
            conn.execute(text("SET statement_timeout = 0"))
        except Exception:
            pass

        return {
            "nodes": nodes,
            "edges": edges,
        }


def suggest_join_keys(
    left_table: str,
    right_table: str,
    left_columns: list[dict[str, Any]],
    right_columns: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Suggest join key pairs based on naming and type heuristics.
    """
    candidates: list[dict[str, Any]] = []

    right_lookup = {c["name"].lower(): c for c in right_columns}
    right_norm = {_root_name(_strip_table_prefix(c["name"], right_table)): c for c in right_columns}

    for left_col in left_columns:
        left_name = left_col["name"]
        left_lower = left_name.lower()
        left_type = left_col.get("type", "")
        left_group = _type_group(left_type)
        left_norm = _root_name(_strip_table_prefix(left_name, left_table))

        # Exact name match
        if left_lower in right_lookup:
            right_col = right_lookup[left_lower]
            score = 10
            if _type_group(right_col.get("type", "")) == left_group:
                score += 2
            if _is_id_like(left_name) and _is_id_like(right_col["name"]):
                score += 3
            candidates.append({
                "left_column": left_name,
                "right_column": right_col["name"],
                "left_type": left_type,
                "right_type": right_col.get("type", ""),
                "score": score,
                "reason": "name match",
            })
            continue

        # Normalized root match
        if left_norm in right_norm and left_norm:
            right_col = right_norm[left_norm]
            score = 6
            if _type_group(right_col.get("type", "")) == left_group:
                score += 2
            if _is_id_like(left_name) and _is_id_like(right_col["name"]):
                score += 3
            candidates.append({
                "left_column": left_name,
                "right_column": right_col["name"],
                "left_type": left_type,
                "right_type": right_col.get("type", ""),
                "score": score,
                "reason": "normalized match",
            })

    candidates.sort(key=lambda c: c["score"], reverse=True)
    return candidates


def analyze_join(
    engine: Engine,
    left_table: str,
    right_table: str,
    left_key: str,
    right_key: str,
    schema: str = "public",
    sample_size: int | None = 100000,
) -> dict[str, Any]:
    validate_identifier(left_table, "table")
    validate_identifier(right_table, "table")
    validate_identifier(left_key, "column")
    validate_identifier(right_key, "column")
    validate_identifier(schema, "schema")

    with engine.connect() as conn:
        try:
            conn.execute(text("SET statement_timeout = 30000"))
        except Exception:
            pass

        left_est = _get_row_estimate(conn, schema, left_table)
        right_est = _get_row_estimate(conn, schema, right_table)
        use_sample = sample_size is not None and sample_size > 0 and (left_est > sample_size or right_est > sample_size)

        def _sample_clause(table: str, estimate: int) -> tuple[str, float | None]:
            if not use_sample or sample_size is None or sample_size <= 0:
                return f'"{schema}"."{table}"', None
            percent = (sample_size / estimate) * 100 if estimate > 0 else 100.0
            percent = max(0.1, min(100.0, percent))
            clause = f'"{schema}"."{table}" TABLESAMPLE BERNOULLI({percent})'
            return f"{clause} LIMIT {int(sample_size)}", round(percent, 3)

        left_from, left_percent = _sample_clause(left_table, left_est)
        right_from, right_percent = _sample_clause(right_table, right_est)

        sql = f"""
        WITH left_sample AS (
            SELECT "{left_key}" AS left_key
            FROM {left_from}
        ),
        right_sample AS (
            SELECT "{right_key}" AS right_key
            FROM {right_from}
        ),
        left_keys AS (
            SELECT DISTINCT left_key
            FROM left_sample
            WHERE left_key IS NOT NULL
        ),
        right_keys AS (
            SELECT DISTINCT right_key
            FROM right_sample
            WHERE right_key IS NOT NULL
        )
        SELECT
            (SELECT COUNT(*) FROM left_sample) AS left_total,
            (SELECT COUNT(*) FROM right_sample) AS right_total,
            (SELECT COUNT(*) FROM left_sample WHERE left_key IS NULL) AS left_null,
            (SELECT COUNT(*) FROM right_sample WHERE right_key IS NULL) AS right_null,
            (SELECT COUNT(*) FROM left_keys) AS left_distinct,
            (SELECT COUNT(*) FROM right_keys) AS right_distinct,
            (SELECT COUNT(*) FROM left_keys lk JOIN right_keys rk ON lk.left_key = rk.right_key) AS matched_keys
        """
        row = conn.execute(text(sql)).fetchone()

        left_total = int(row[0] or 0)
        right_total = int(row[1] or 0)
        left_null = int(row[2] or 0)
        right_null = int(row[3] or 0)
        left_distinct = int(row[4] or 0)
        right_distinct = int(row[5] or 0)
        matched_keys = int(row[6] or 0)

        left_non_null = max(0, left_total - left_null)
        right_non_null = max(0, right_total - right_null)

        left_match_rate = matched_keys / left_distinct if left_distinct > 0 else 0.0
        right_match_rate = matched_keys / right_distinct if right_distinct > 0 else 0.0

        left_dup_rate = 1 - (left_distinct / left_non_null) if left_non_null > 0 else 0.0
        right_dup_rate = 1 - (right_distinct / right_non_null) if right_non_null > 0 else 0.0

        def _cardinality(left_dup: float, right_dup: float) -> str:
            left_many = left_dup >= 0.05
            right_many = right_dup >= 0.05
            if not left_many and not right_many:
                return "one-to-one"
            if left_many and not right_many:
                return "many-to-one"
            if not left_many and right_many:
                return "one-to-many"
            return "many-to-many"

        try:
            conn.execute(text("SET statement_timeout = 0"))
        except Exception:
            pass

        return {
            "left_table": left_table,
            "right_table": right_table,
            "left_key": left_key,
            "right_key": right_key,
            "left": {
                "total_rows": left_total,
                "null_pct": (left_null / left_total) if left_total else 0.0,
                "distinct_keys": left_distinct,
                "duplicate_rate": left_dup_rate,
            },
            "right": {
                "total_rows": right_total,
                "null_pct": (right_null / right_total) if right_total else 0.0,
                "distinct_keys": right_distinct,
                "duplicate_rate": right_dup_rate,
            },
            "match": {
                "matched_keys": matched_keys,
                "left_key_count": left_distinct,
                "right_key_count": right_distinct,
                "left_match_rate": left_match_rate,
                "right_match_rate": right_match_rate,
            },
            "cardinality": _cardinality(left_dup_rate, right_dup_rate),
            "sampled": bool(use_sample),
            "left_sample_percent": left_percent,
            "right_sample_percent": right_percent,
            "sample_size": sample_size if use_sample else None,
        }


@dataclass(frozen=True)
class JoinKey:
    left_column: str
    right_column: str


@dataclass(frozen=True)
class JoinDefinition:
    left_table: str
    left_schema: str
    right_table: str
    right_schema: str
    join_keys: list[JoinKey]
    join_type: str = "left"


class JoinService:
    def _normalize_join_type(self, join_type: str) -> str:
        normalized = (join_type or "left").strip().lower()
        if normalized not in {"left", "inner", "right", "full"}:
            raise ValueError("Invalid join type. Use left, inner, right, or full.")
        mapping = {
            "left": "LEFT JOIN",
            "inner": "INNER JOIN",
            "right": "RIGHT JOIN",
            "full": "FULL JOIN",
        }
        return mapping[normalized]

    def _build_join_sql(self, join_def: JoinDefinition, limit: int | None = None) -> str:
        join_keyword = self._normalize_join_type(join_def.join_type)
        on_parts = []
        for key in join_def.join_keys:
            validate_identifier(key.left_column, "column")
            validate_identifier(key.right_column, "column")
            on_parts.append(f'l."{key.left_column}" = r."{key.right_column}"')
        if not on_parts:
            raise ValueError("Join keys are required")
        on_clause = " AND ".join(on_parts)
        sql = (
            f'SELECT * FROM "{join_def.left_schema}"."{join_def.left_table}" l '
            f'{join_keyword} "{join_def.right_schema}"."{join_def.right_table}" r '
            f"ON {on_clause}"
        )
        if limit is not None and limit > 0:
            sql += f" LIMIT {int(limit)}"
        return sql

    def define_join(
        self,
        engine: Engine,
        left_table: str,
        right_table: str,
        join_keys: list[tuple[str, str]],
        join_type: str = "left",
        left_schema: str = "public",
        right_schema: str = "public",
    ) -> dict[str, Any]:
        validate_identifier(left_table, "table")
        validate_identifier(right_table, "table")
        validate_identifier(left_schema, "schema")
        validate_identifier(right_schema, "schema")

        if not join_keys:
            return {
                "is_valid": False,
                "errors": ["Join keys are required."],
                "warnings": [],
                "join_definition": None,
                "join_sql": None,
            }

        keys = [JoinKey(left_column=k[0], right_column=k[1]) for k in join_keys]
        join_def = JoinDefinition(
            left_table=left_table,
            left_schema=left_schema,
            right_table=right_table,
            right_schema=right_schema,
            join_keys=keys,
            join_type=join_type,
        )

        errors: list[str] = []
        warnings: list[str] = []

        analysis = analyze_join(
            engine=engine,
            left_table=left_table,
            right_table=right_table,
            left_key=keys[0].left_column,
            right_key=keys[0].right_column,
            schema=left_schema,
            sample_size=100000,
        )

        if analysis["match"]["matched_keys"] == 0:
            errors.append("No matching keys found in sample.")
        if analysis["cardinality"] == "many-to-many":
            warnings.append("Many-to-many join risk detected.")
        if analysis["match"]["left_match_rate"] < 0.5 or analysis["match"]["right_match_rate"] < 0.5:
            warnings.append("Low join coverage; expect many unmatched rows.")
        if analysis["left"]["null_pct"] > 0.2 or analysis["right"]["null_pct"] > 0.2:
            warnings.append("High null rate on join keys.")

        try:
            join_sql = self._build_join_sql(join_def)
        except ValueError as exc:
            errors.append(str(exc))
            join_sql = None

        return {
            "is_valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "join_definition": {
                "left_table": left_table,
                "left_schema": left_schema,
                "right_table": right_table,
                "right_schema": right_schema,
                "join_type": join_type,
                "join_keys": [{"left_column": k.left_column, "right_column": k.right_column} for k in keys],
            },
            "join_sql": join_sql,
        }

    def preview_join(self, engine: Engine, join_def: JoinDefinition, limit: int = 100) -> dict[str, Any]:
        validate_identifier(join_def.left_table, "table")
        validate_identifier(join_def.right_table, "table")
        validate_identifier(join_def.left_schema, "schema")
        validate_identifier(join_def.right_schema, "schema")

        sql = self._build_join_sql(join_def, limit=limit)
        try:
            with engine.connect() as conn:
                result = conn.execute(text(sql))
                rows = [dict(row._mapping) for row in result.fetchmany(limit)]
                columns = list(result.keys())
                left_count = _get_row_estimate(conn, join_def.left_schema, join_def.left_table)
                right_count = _get_row_estimate(conn, join_def.right_schema, join_def.right_table)
        except Exception as exc:
            return {
                "columns": [],
                "rows": [],
                "row_count": 0,
                "left_table_count": 0,
                "right_table_count": 0,
                "sql": sql,
                "error": str(exc),
                "status": "error",
            }

        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "left_table_count": left_count,
            "right_table_count": right_count,
            "sql": sql,
            "error": None,
            "status": "success",
        }

    def generate_cross_table_feature(
        self,
        numerator_col: str,
        denominator_col: str,
        operation: str,
        feature_name: str,
    ) -> str:
        validate_identifier(feature_name, "column")
        op = (operation or "ratio").strip().lower()
        if op == "ratio":
            return f'({numerator_col}) / NULLIF({denominator_col}, 0) AS "{feature_name}"'
        if op == "difference":
            return f'({numerator_col}) - ({denominator_col}) AS "{feature_name}"'
        raise ValueError("Invalid operation. Use ratio or difference.")


join_service = JoinService()
