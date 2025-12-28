"""
Dataset Assembler Service


This service:
- Takes grain_sql + target_sql + feature SQLs as inputs
- Enforces join contracts on (entity_id, observation_date)
- Runs quality/joinability/time-leakage checks
- Outputs final dataset SQL + quality report

feature_service.py only suggests/generates feature SQL.
This service ASSEMBLES the final dataset.
"""

from dataclasses import dataclass, field
from typing import Any, Literal, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.services.grain_service import GrainDefinition, GrainService, validate_identifier
from app.services.target_service import TargetDefinition, TargetService


# =============================================================================
# Feature Definition (for assembly)
# =============================================================================


@dataclass
class FeatureSQL:
    """
    A feature SQL fragment to be assembled into the dataset.
    
    Requirements:
    - Must output (entity_id, observation_date, ...feature_columns)
    - Features must be calculated using data <= observation_date (no leakage)
    - For leakage verification, include max_source_time column showing MAX timestamp used
    """
    name: str  # Human-readable feature name (not SQL identifier)
    sql: str  # SQL that outputs (entity_id, observation_date, feature_col)
    feature_columns: list[str]  # Column names produced by this SQL
    source_table: str  # Source table for documentation
    time_column: Optional[str] = None  # DEPRECATED: use max_source_time_column
    max_source_time_column: Optional[str] = None  # Column with MAX timestamp for leakage check
    window_description: Optional[str] = None  # e.g., "30 days before observation"
    
    def __post_init__(self):
        """Validate feature definition."""
        # FIX 3: Don't validate name as SQL identifier (it's metadata)
        # Only validate feature_columns which ARE SQL identifiers
        for col in self.feature_columns:
            validate_identifier(col, f"feature_column in '{self.name}'")
        if not self.sql:
            raise ValueError(f"Feature SQL cannot be empty for '{self.name}'")
        if not self.feature_columns:
            raise ValueError(f"Feature '{self.name}' must produce at least one column")


@dataclass
class AssemblyResult:
    """Result of dataset assembly."""
    dataset_sql: str
    quality_report: dict[str, Any]
    warnings: list[dict[str, Any]]
    errors: list[str]
    status: Literal["success", "warning", "error"]
    
    # Metadata
    grain_rows_estimate: int = 0
    target_rows: int = 0
    feature_count: int = 0
    leakage_issues: list[dict[str, Any]] = field(default_factory=list)
    joinability_issues: list[dict[str, Any]] = field(default_factory=list)


# =============================================================================
# Dataset Assembler Service
# =============================================================================


class DatasetAssembler:
    """
    Assembles grain + target + features into a final ML dataset.
    
    
    """

    @staticmethod
    def validate_assembly_inputs(
        grain: GrainDefinition,
        target: TargetDefinition,
        features: list[FeatureSQL],
    ) -> dict[str, Any]:
        """
        Validate that all inputs are properly configured for assembly.
        
        Args:
            grain: Grain definition from previous step
            target: Target definition from previous step
            features: List of feature SQL fragments
            
        Returns:
            Validation result with errors/warnings.
        """
        result = {
            "errors": [],
            "warnings": [],
            "status": "valid",
        }
        
        # 1. Validate grain exists
        if not grain:
            result["errors"].append("Grain definition is required")
            result["status"] = "error"
        
        # 2. Validate target exists
        if not target:
            result["errors"].append("Target definition is required")
            result["status"] = "error"
        
        # 3. Validate features
        if not features:
            result["warnings"].append({
                "code": "NO_FEATURES",
                "message": "No features provided. Dataset will only have entity_id, observation_date, and target.",
            })
        
        # 4. Check feature names are unique
        feature_names = [f.name for f in features]
        if len(feature_names) != len(set(feature_names)):
            result["errors"].append("Feature names must be unique")
            result["status"] = "error"
        
        # 5. Check feature column names don't conflict with grain/target
        reserved_columns = {"entity_id", "observation_date", target.target_name.lower()}
        for feature in features:
            for col in feature.feature_columns:
                if col.lower() in reserved_columns:
                    result["errors"].append(
                        f"Feature column '{col}' conflicts with reserved column names"
                    )
                    result["status"] = "error"
        
        if result["warnings"]:
            if result["status"] == "valid":
                result["status"] = "warning"
        
        return result

    @staticmethod
    def enforce_join_contract(
        engine: Engine,
        sql: str,
        expected_columns: list[str],
        sql_name: str,
    ) -> dict[str, Any]:
        """
        Validate that a SQL fragment outputs the expected columns.
        
        The join contract requires:
        - All SQLs must output (entity_id, observation_date) for joining
        - Column types should be compatible
        
        Args:
            engine: SQLAlchemy engine
            sql: SQL to validate
            expected_columns: Columns that must be present
            sql_name: Name for error messages (e.g., "Grain SQL")
            
        Returns:
            Validation result.
        """
        result = {
            "sql_name": sql_name,
            "valid": True,
            "errors": [],
            "actual_columns": [],
        }
        
        try:
            # FIX 2: Strip trailing semicolons before embedding
            clean_sql = sql.strip().rstrip(";")
            
            # Wrap in subquery to get column info
            check_sql = f"""
                SELECT * FROM (
                    {clean_sql}
                ) _contract_check
                LIMIT 0
            """
            
            with engine.connect() as conn:
                res = conn.execute(text(check_sql))
                actual_columns = list(res.keys())
                result["actual_columns"] = actual_columns
                
                # Check expected columns exist
                actual_lower = {c.lower() for c in actual_columns}
                for expected in expected_columns:
                    if expected.lower() not in actual_lower:
                        result["errors"].append(
                            f"Missing required column '{expected}' in {sql_name}"
                        )
                        result["valid"] = False
                        
        except Exception as e:
            result["errors"].append(f"SQL execution error in {sql_name}: {str(e)[:200]}")
            result["valid"] = False
        
        return result

    @staticmethod
    def check_joinability(
        engine: Engine,
        grain_sql: str,
        other_sql: str,
        other_name: str,
        sample_limit: int = 10000,
    ) -> dict[str, Any]:
        """
        Check how well two SQLs join on (entity_id, observation_date).
        
        Args:
            engine: SQLAlchemy engine
            grain_sql: Grain SQL (the base)
            other_sql: Other SQL to check (target or feature)
            other_name: Name for reporting
            sample_limit: Max rows to sample for check
            
        Returns:
            Joinability report with match rates.
        """
        result = {
            "name": other_name,
            "grain_sample_size": 0,
            "matched_rows": 0,
            "unmatched_rows": 0,
            "match_rate": 0.0,
            "status": "checking",
            "warning": None,
        }
        
        try:
            # FIX 2: Strip trailing semicolons
            clean_grain_sql = grain_sql.strip().rstrip(";")
            clean_other_sql = other_sql.strip().rstrip(";")
            
            # FIX 5: Use DISTINCT to avoid duplicate explosions
            check_sql = f"""
                WITH grain_sample AS (
                    SELECT DISTINCT entity_id, observation_date
                    FROM ({clean_grain_sql}) g
                    LIMIT {sample_limit}
                ),
                other AS (
                    SELECT DISTINCT entity_id, observation_date
                    FROM ({clean_other_sql}) o
                ),
                join_check AS (
                    SELECT 
                        g.entity_id,
                        g.observation_date,
                        CASE WHEN o.entity_id IS NOT NULL THEN 1 ELSE 0 END AS matched
                    FROM grain_sample g
                    LEFT JOIN other o 
                        ON g.entity_id = o.entity_id 
                        AND g.observation_date = o.observation_date
                )
                SELECT 
                    COUNT(*) AS total,
                    SUM(matched) AS matched,
                    COUNT(*) - SUM(matched) AS unmatched
                FROM join_check
            """
            
            with engine.connect() as conn:
                res = conn.execute(text(check_sql))
                row = res.fetchone()
                
                if row:
                    total = int(row[0]) if row[0] else 0
                    matched = int(row[1]) if row[1] else 0
                    unmatched = int(row[2]) if row[2] else 0
                    
                    result["grain_sample_size"] = total
                    result["matched_rows"] = matched
                    result["unmatched_rows"] = unmatched
                    result["match_rate"] = round((matched / total) * 100, 2) if total > 0 else 0
                    
                    if result["match_rate"] == 0:
                        result["status"] = "error"
                        result["warning"] = f"{other_name} has 0% join match - check entity_id/observation_date columns"
                    elif result["match_rate"] < 50:
                        result["status"] = "warning"
                        result["warning"] = f"{other_name} has low join match ({result['match_rate']}%)"
                    else:
                        result["status"] = "ok"
                        
        except Exception as e:
            result["status"] = "error"
            result["warning"] = f"Join check failed: {str(e)[:100]}"
        
        return result

    @staticmethod
    def check_time_leakage(
        engine: Engine,
        grain_sql: str,
        feature: FeatureSQL,
        sample_limit: int = 1000,
    ) -> dict[str, Any]:
        """
        Check if a feature SQL has time leakage.
        
        Time leakage occurs when features use data from AFTER observation_date.
        This is a HARD rule - features must only use data <= observation_date.
        
        FIX 4: Uses max_source_time_column contract. Feature SQL must output
        a column showing MAX timestamp of source data used. We verify:
        max_source_time <= observation_date
        
        Args:
            engine: SQLAlchemy engine
            grain_sql: Grain SQL for reference
            feature: Feature SQL to check
            sample_limit: Max rows to sample
            
        Returns:
            Leakage check result.
        """
        result = {
            "feature_name": feature.name,
            "has_time_column": feature.max_source_time_column is not None,
            "leakage_detected": False,
            "leakage_count": 0,
            "sample_size": 0,
            "status": "checking",
            "message": None,
        }
        
        # FIX 4: If no max_source_time_column, we cannot verify - return warning
        if not feature.max_source_time_column:
            result["status"] = "unverifiable"
            result["message"] = (
                f"Feature '{feature.name}' has no max_source_time_column. "
                "Cannot verify time leakage. To enable verification, add a column like "
                "MAX(event_time) AS max_source_time to your feature SQL."
            )
            return result
        
        try:
            # FIX 2 & 3: Strip semicolons, validate column
            validate_identifier(feature.max_source_time_column, "max_source_time_column")
            clean_sql = feature.sql.strip().rstrip(";")
            
            # Check if max_source_time > observation_date anywhere
            # FIX 1: LIMIT inside subquery for proper sampling
            check_sql = f"""
                WITH feature_data AS (
                    {clean_sql}
                ),
                sample AS (
                    SELECT * FROM feature_data
                    LIMIT {sample_limit}
                )
                SELECT 
                    COUNT(*) AS total,
                    SUM(CASE WHEN "{feature.max_source_time_column}"::DATE > observation_date THEN 1 ELSE 0 END) AS leakage_count
                FROM sample
            """
            
            with engine.connect() as conn:
                res = conn.execute(text(check_sql))
                row = res.fetchone()
                
                if row:
                    total = int(row[0]) if row[0] else 0
                    leakage = int(row[1]) if row[1] else 0
                    
                    result["sample_size"] = total
                    result["leakage_count"] = leakage
                    result["leakage_detected"] = leakage > 0
                    
                    if leakage > 0:
                        result["status"] = "leakage"
                        result["message"] = (
                            f"TIME LEAKAGE DETECTED in '{feature.name}': "
                            f"{leakage}/{total} rows have {feature.max_source_time_column} > observation_date"
                        )
                    else:
                        result["status"] = "ok"
                        result["message"] = f"No time leakage detected in '{feature.name}'"
                        
        except Exception as e:
            result["status"] = "error"
            result["message"] = f"Leakage check failed for '{feature.name}': {str(e)[:100]}"
        
        return result

    @staticmethod
    def assemble_dataset_sql(
        grain: GrainDefinition,
        target: TargetDefinition,
        features: list[FeatureSQL],
    ) -> str:
        """
        Assemble the final dataset SQL.
        
        Structure:
        1. Grain CTE (entity_id, observation_date)
        2. Target CTE (entity_id, observation_date, target)
        3. Feature CTEs (entity_id, observation_date, feature_columns)
        4. Final SELECT joining all on (entity_id, observation_date)
        
        Args:
            grain: Grain definition
            target: Target definition
            features: List of feature SQL fragments
            
        Returns:
            Complete dataset SQL.
        """
        # Assembler owns the grain CTE - use embedded mode for target
        grain_sql = GrainService.generate_grain_sql(grain).strip().rstrip(";")
        
        # Get target in embedded mode (just label_events + target_calc CTEs)
        target_ctes = TargetService.generate_target_sql(
            target, grain, grain_sql=grain_sql, include_grain_cte=False
        ).strip().rstrip(";").strip().rstrip(",")
        
        # Build CTEs - one unified chain
        ctes = []
        
        # 1. Grain CTE - single source of truth
        ctes.append(f"grain AS (\n    {grain_sql}\n)")
        
        # 2. Target CTEs (label_events, target_calc) - append directly
        ctes.append(target_ctes)
        
        # 3. target_data - wrapper that selects from target_calc
        ctes.append(f"target_data AS (\n    SELECT entity_id, observation_date, {target.target_name}\n    FROM target_calc\n)")
        
        # 4. Feature CTEs - clean and append
        feature_aliases = []
        for i, feature in enumerate(features):
            alias = f"feature_{i}"
            feature_aliases.append((alias, feature))
            feature_sql_clean = feature.sql.strip().rstrip(";")
            ctes.append(f"{alias} AS (\n    -- {feature.name}: {feature.window_description or 'no time window specified'}\n    {feature_sql_clean}\n)")
        
        # Build final SELECT
        select_columns = ["g.entity_id", "g.observation_date", f"t.{target.target_name}"]
        joins = ["FROM grain g", "INNER JOIN target_data t ON g.entity_id = t.entity_id AND g.observation_date = t.observation_date"]
        
        for alias, feature in feature_aliases:
            for col in feature.feature_columns:
                select_columns.append(f"{alias}.{col}")
            joins.append(
                f"LEFT JOIN {alias} ON g.entity_id = {alias}.entity_id "
                f"AND g.observation_date = {alias}.observation_date"
            )
        
        # Assemble final SQL
        sql = f"""
-- ============================================================================
-- ML Dataset Assembly
-- Generated by DatasetAssembler 
-- ============================================================================
-- Grain: {grain.entity_type} from {grain.entity_table}
-- Target: {target.target_name} (window: {target.window_months} months)
-- Features: {len(features)} feature sets
-- ============================================================================

WITH {','.join(ctes)}

SELECT 
    {','.join(f'    {c}' for c in select_columns)}
{chr(10).join(joins)}
"""
        
        return sql.strip()

    @staticmethod
    def generate_quality_report(
        engine: Engine,
        grain: GrainDefinition,
        target: TargetDefinition,
        features: list[FeatureSQL],
    ) -> dict[str, Any]:
        """
        Generate a quality report for the assembled dataset.
        
        Runs all checks and provides summary.
        
        Args:
            engine: SQLAlchemy engine
            grain: Grain definition
            target: Target definition
            features: List of feature SQL fragments
            
        Returns:
            Quality report with checks and recommendations.
        """
        report = {
            "grain": {
                "entity_type": grain.entity_type,
                "entity_table": grain.entity_table,
                "dedup_rule": grain.deduplication_rule,
            },
            "target": {
                "name": target.target_name,
                "window_months": target.window_months,
                "maturity_months": target.maturity_months,
            },
            "features": {
                "count": len(features),
                "names": [f.name for f in features],
                "total_columns": sum(len(f.feature_columns) for f in features),
            },
            "checks": {
                "contract": [],
                "joinability": [],
                "leakage": [],
            },
            "overall_status": "checking",
            "errors": [],
            "warnings": [],
            "recommendations": [],
        }
        
        # FIX 3: Use same grain_sql and settings as assembly
        grain_sql = GrainService.generate_grain_sql(grain).strip().rstrip(";")
        target_sql = TargetService.generate_target_sql(
            target, grain, grain_sql=grain_sql, include_grain_cte=True
        ).strip().rstrip(";")
        
        # 1. Contract checks
        for sql, name, expected in [
            (grain_sql, "Grain", ["entity_id", "observation_date"]),
            (target_sql, "Target", ["entity_id", "observation_date", target.target_name]),
        ]:
            check = DatasetAssembler.enforce_join_contract(engine, sql, expected, name)
            report["checks"]["contract"].append(check)
            if not check["valid"]:
                report["errors"].extend(check["errors"])
        
        for feature in features:
            expected_cols = ["entity_id", "observation_date"] + feature.feature_columns
            check = DatasetAssembler.enforce_join_contract(
                engine, feature.sql, expected_cols, f"Feature: {feature.name}"
            )
            report["checks"]["contract"].append(check)
            if not check["valid"]:
                report["errors"].extend(check["errors"])
        
        # 2. Joinability checks (target and features against grain)
        target_join = DatasetAssembler.check_joinability(engine, grain_sql, target_sql, "Target")
        report["checks"]["joinability"].append(target_join)
        if target_join["warning"]:
            report["warnings"].append({"source": "Target", "message": target_join["warning"]})
        
        for feature in features:
            join_check = DatasetAssembler.check_joinability(
                engine, grain_sql, feature.sql, f"Feature: {feature.name}"
            )
            report["checks"]["joinability"].append(join_check)
            if join_check["warning"]:
                report["warnings"].append({"source": feature.name, "message": join_check["warning"]})
        
        # 3. Time leakage checks
        for feature in features:
            leakage_check = DatasetAssembler.check_time_leakage(engine, grain_sql, feature)
            report["checks"]["leakage"].append(leakage_check)
            if leakage_check["leakage_detected"]:
                report["errors"].append(leakage_check["message"])
            # FIX 2: Changed from no_time_column to unverifiable
            elif leakage_check["status"] == "unverifiable":
                report["warnings"].append({"source": feature.name, "message": leakage_check["message"]})
        
        # 4. Generate recommendations
        if report["errors"]:
            report["overall_status"] = "error"
            report["recommendations"].append("Fix all errors before using this dataset for ML.")
        elif report["warnings"]:
            report["overall_status"] = "warning"
            report["recommendations"].append("Review warnings. Low join rates may indicate data issues.")
        else:
            report["overall_status"] = "ok"
            report["recommendations"].append("Dataset assembly looks good! Proceed to model training.")
        
        return report

    @staticmethod
    def assemble(
        engine: Engine,
        grain: GrainDefinition,
        target: TargetDefinition,
        features: list[FeatureSQL],
        run_checks: bool = True,
    ) -> AssemblyResult:
        """
        Full dataset assembly with optional quality checks.
        
        This is the main entry point .
        
        Args:
            engine: SQLAlchemy engine
            grain: Grain definition from previous step
            target: Target definition from previous step
            features: List of feature SQL fragments
            run_checks: Whether to run quality checks
            
        Returns:
            AssemblyResult with dataset SQL and quality report.
        """
        # Validate inputs
        validation = DatasetAssembler.validate_assembly_inputs(grain, target, features)
        if validation["status"] == "error":
            return AssemblyResult(
                dataset_sql="",
                quality_report={},
                warnings=validation.get("warnings", []),
                errors=validation["errors"],
                status="error",
            )
        
        # Generate dataset SQL
        try:
            dataset_sql = DatasetAssembler.assemble_dataset_sql(grain, target, features)
        except Exception as e:
            return AssemblyResult(
                dataset_sql="",
                quality_report={},
                warnings=[],
                errors=[f"Failed to generate dataset SQL: {str(e)}"],
                status="error",
            )
        
        # Run quality checks if requested
        quality_report = {}
        leakage_issues = []
        joinability_issues = []
        
        if run_checks:
            try:
                quality_report = DatasetAssembler.generate_quality_report(
                    engine, grain, target, features
                )
                
                # Extract issues for easy access
                for check in quality_report.get("checks", {}).get("leakage", []):
                    if check.get("leakage_detected"):
                        leakage_issues.append(check)
                
                for check in quality_report.get("checks", {}).get("joinability", []):
                    if check.get("status") in ("warning", "error"):
                        joinability_issues.append(check)
                
            except Exception as e:
                quality_report = {"error": str(e)}
        
        # Determine overall status
        if quality_report.get("errors"):
            status = "error"
        elif quality_report.get("warnings") or validation.get("warnings"):
            status = "warning"
        else:
            status = "success"
        
        return AssemblyResult(
            dataset_sql=dataset_sql,
            quality_report=quality_report,
            warnings=validation.get("warnings", []) + quality_report.get("warnings", []),
            errors=quality_report.get("errors", []),
            status=status,
            feature_count=len(features),
            leakage_issues=leakage_issues,
            joinability_issues=joinability_issues,
        )



dataset_assembler = DatasetAssembler()
