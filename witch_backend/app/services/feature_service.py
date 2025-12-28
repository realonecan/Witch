"""
Feature Engineering Service
Suggests ML features based on database schema and generates dataset SQL.
"""

from typing import Any


class FeatureEngineer:
    """
    Analyzes database schemas and suggests features for ML.
    """

    def _normalize_columns(self, columns_detail: list | dict) -> dict[str, dict[str, str]]:
        """
        Normalize column details to a consistent dict format.
        
        Args:
            columns_detail: Either a list of column dicts or a dict with column names as keys.
            
        Returns:
            Dict mapping column names to their info.
        """
        if isinstance(columns_detail, dict):
            return columns_detail
        
        # Convert list format to dict format
        result = {}
        for col in columns_detail:
            if isinstance(col, dict) and "name" in col:
                result[col["name"]] = {
                    "type": col.get("type", ""),
                    "nullable": col.get("nullable", True),
                }
        return result

    def suggest_features(
        self, 
        schema_summary: str, 
        columns_detail: list | dict, 
        target_description: str
    ) -> list[dict[str, Any]]:
        """
        Analyze table columns and suggest ML features.

        Args:
            schema_summary: Text description of the table schema.
            columns_detail: List of column dicts or dict mapping column names to their types.
            target_description: What the user wants to predict (e.g., "Customer Churn").

        Returns:
            List of feature suggestions with name, logic, and relevance.
        """
        suggestions: list[dict[str, Any]] = []
        
        # Normalize column format
        columns_dict = self._normalize_columns(columns_detail)
        
        # Categorize columns
        numeric_cols = []
        date_cols = []
        categorical_cols = []
        id_cols = []
        
        for col_name, col_info in columns_dict.items():
            col_type = col_info.get("type", "").lower()
            col_name_lower = col_name.lower()
            
            # Detect VARCHAR columns that are likely dates based on naming
            is_varchar_date = (
                col_type in ["character varying", "varchar", "text"] and
                any(date_hint in col_name_lower for date_hint in ["date", "_dt", "created", "updated", "time"])
            )
            col_name_lower = col_name.lower()
            
            # Identify ID columns (likely grouping keys)
            if any(id_pattern in col_name_lower for id_pattern in ["_id", "id_", "userid", "user_id", "client_id", "clientid", "account_id", "accountid"]):
                id_cols.append(col_name)
                continue
            
            # Categorize by type
            if col_type in ["integer", "bigint", "smallint", "numeric", "double precision", "real", "float", "decimal"]:
                numeric_cols.append(col_name)
            elif col_type in ["date", "timestamp", "timestamp with time zone", "timestamp without time zone"]:
                date_cols.append(col_name)
            elif is_varchar_date:
                # VARCHAR column that looks like a date based on name
                date_cols.append(col_name)
            elif col_type in ["character varying", "varchar", "text", "character", "char"]:
                categorical_cols.append(col_name)
        
        # Find the best ID column for grouping
        grouping_col = self._find_grouping_column(id_cols)
        
        # === NUMERIC FEATURE SUGGESTIONS ===
        for col in numeric_cols[:5]:  # Limit to first 5 numeric columns
            # Aggregation features
            suggestions.append({
                "name": f"avg_{col.lower()}",
                "column": col,
                "logic": f"AVG({col})",
                "sql_template": f'COALESCE(AVG("{col}"), 0)',
                "type": "aggregation",
                "relevance": "high",
                "description": f"Average of {col} - useful for understanding typical values",
            })
            
            suggestions.append({
                "name": f"sum_{col.lower()}",
                "column": col,
                "logic": f"SUM({col})",
                "sql_template": f'COALESCE(SUM("{col}"), 0)',
                "type": "aggregation",
                "relevance": "high",
                "description": f"Total sum of {col} - captures overall volume",
            })
            
            suggestions.append({
                "name": f"max_{col.lower()}",
                "column": col,
                "logic": f"MAX({col})",
                "sql_template": f'COALESCE(MAX("{col}"), 0)',
                "type": "aggregation",
                "relevance": "medium",
                "description": f"Maximum value of {col} - identifies peak behavior",
            })
            
            suggestions.append({
                "name": f"stddev_{col.lower()}",
                "column": col,
                "logic": f"STDDEV({col})",
                "sql_template": f'COALESCE(STDDEV("{col}"), 0)',
                "type": "aggregation",
                "relevance": "medium",
                "description": f"Standard deviation of {col} - measures volatility",
            })
        
        # === DATE FEATURE SUGGESTIONS ===
        # Handle both true DATE columns and VARCHAR columns that contain dates
        for col in date_cols[:3]:  # Limit to first 3 date columns
            # Recency features - use ::DATE cast to handle VARCHAR dates
            suggestions.append({
                "name": f"days_since_last_{col.lower().replace('date', '').replace('_', '')}",
                "column": col,
                "logic": f"Days since last {col}",
                "sql_template": f'COALESCE(EXTRACT(DAY FROM AGE(CURRENT_DATE, MAX("{col}"::DATE))), 9999)::INTEGER',
                "type": "recency",
                "relevance": "high",
                "description": f"Days since last {col} - measures recency of activity",
            })
            
            # Frequency features
            suggestions.append({
                "name": f"count_events",
                "column": col,
                "logic": f"COUNT(*)",
                "sql_template": "COUNT(*)",
                "type": "frequency",
                "relevance": "high",
                "description": "Total count of events/records - measures engagement frequency",
            })
            
            # Time span features - use ::DATE cast and AGE() function
            suggestions.append({
                "name": f"days_active",
                "column": col,
                "logic": f"Days between first and last {col}",
                "sql_template": f'COALESCE(EXTRACT(DAY FROM AGE(MAX("{col}"::DATE), MIN("{col}"::DATE))), 0)::INTEGER',
                "type": "duration",
                "relevance": "medium",
                "description": f"Days between first and last {col} - measures relationship duration",
            })
        
        # === CATEGORICAL FEATURE SUGGESTIONS ===
        for col in categorical_cols[:3]:  # Limit to first 3 categorical columns
            suggestions.append({
                "name": f"distinct_{col.lower()}_count",
                "column": col,
                "logic": f"COUNT(DISTINCT {col})",
                "sql_template": f'COUNT(DISTINCT "{col}")',
                "type": "cardinality",
                "relevance": "medium",
                "description": f"Number of unique {col} values - measures diversity",
            })
            
            # MODE() WITHIN GROUP is valid PostgreSQL syntax for aggregate mode
            suggestions.append({
                "name": f"mode_{col.lower()}",
                "column": col,
                "logic": f"Most frequent {col} value",
                "sql_template": f'MODE() WITHIN GROUP (ORDER BY "{col}")',
                "type": "categorical",
                "relevance": "medium",
                "description": f"Most frequent {col} value - identifies dominant category",
            })
        
        # Add context about the target
        target_lower = target_description.lower()
        
        # Boost relevance for features commonly used for specific targets
        if "churn" in target_lower or "retention" in target_lower:
            for suggestion in suggestions:
                if suggestion["type"] == "recency":
                    suggestion["relevance"] = "critical"
                    suggestion["description"] += " [CRITICAL for churn prediction]"
                elif suggestion["type"] == "frequency":
                    suggestion["relevance"] = "high"
                    suggestion["description"] += " [Important for churn prediction]"
        
        if "credit" in target_lower or "risk" in target_lower or "default" in target_lower:
            for suggestion in suggestions:
                if "sum" in suggestion["name"] or "max" in suggestion["name"]:
                    suggestion["relevance"] = "critical"
                    suggestion["description"] += " [CRITICAL for credit risk]"
        
        if "fraud" in target_lower:
            for suggestion in suggestions:
                if suggestion["type"] == "aggregation" and "stddev" in suggestion["name"]:
                    suggestion["relevance"] = "critical"
                    suggestion["description"] += " [CRITICAL for fraud detection]"
        
        # Sort by relevance
        relevance_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        suggestions.sort(key=lambda x: relevance_order.get(x["relevance"], 3))
        
        return suggestions

    def _find_grouping_column(self, id_cols: list[str]) -> str | None:
        """Find the best column to use for GROUP BY."""
        priority_patterns = ["user_id", "client_id", "customer_id", "account_id", "id"]
        
        for pattern in priority_patterns:
            for col in id_cols:
                if pattern in col.lower():
                    return col
        
        return id_cols[0] if id_cols else None

    def generate_dataset_sql(
        self, 
        table_name: str, 
        selected_features: list[dict[str, Any]], 
        grouping_column: str | None = None
    ) -> str:
        """
        Generate a SQL query to create an ML-ready dataset.

        Args:
            table_name: Name of the source table.
            selected_features: List of feature definitions (with sql_template).
            grouping_column: Column to group by (e.g., user_id).

        Returns:
            PostgreSQL query string.
        """
        if not selected_features:
            return f'SELECT * FROM "{table_name}" LIMIT 1000;'
        
        # Build SELECT clause
        select_parts = []
        
        # Always include the grouping column first
        if grouping_column:
            select_parts.append(f'"{grouping_column}"')
        
        # Add each selected feature
        for feature in selected_features:
            sql_template = feature.get("sql_template", "")
            feature_name = feature.get("name", "unknown")
            
            if sql_template:
                select_parts.append(f'{sql_template} AS "{feature_name}"')
        
        if not select_parts:
            return f'SELECT * FROM "{table_name}" LIMIT 1000;'
        
        # Build the query
        select_clause = ",\n    ".join(select_parts)
        
        if grouping_column:
            query = f'''SELECT
    {select_clause}
FROM public."{table_name}"
GROUP BY "{grouping_column}"
ORDER BY "{grouping_column}";'''
        else:
            # No grouping - just compute features at table level
            query = f'''SELECT
    {select_clause}
FROM public."{table_name}";'''
        
        return query

    def detect_grouping_column(self, columns_detail: list | dict) -> str | None:
        """
        Automatically detect the best grouping column from table schema.

        Args:
            columns_detail: List of column dicts or dict mapping column names to their types.

        Returns:
            The best candidate column name for GROUP BY, or None.
        """
        # Normalize to dict format
        columns_dict = self._normalize_columns(columns_detail)
        
        priority_patterns = [
            ("user_id", 10),
            ("userid", 10),
            ("client_id", 9),
            ("clientid", 9),
            ("customer_id", 8),
            ("customerid", 8),
            ("account_id", 7),
            ("accountid", 7),
            ("id", 1),  # Generic "id" as last resort
        ]
        
        candidates = []
        
        for col_name in columns_dict.keys():
            col_lower = col_name.lower()
            
            for pattern, score in priority_patterns:
                if pattern in col_lower or col_lower == pattern:
                    candidates.append((col_name, score))
                    break
        
        if not candidates:
            return None
        
        # Return the highest scoring candidate
        candidates.sort(key=lambda x: -x[1])
        return candidates[0][0]



feature_engineer = FeatureEngineer()

