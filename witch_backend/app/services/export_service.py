"""
Export Service


This is the output + traceability + reproducibility layer.
It does NOT regenerate SQL, does NOT validate, does NOT add features.
It only exports already-validated dataset_sql from session.

Outputs:
- Dataset CSV file
- Metadata JSON file (for reproducibility)
"""

import csv
import json
import os
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine


# =============================================================================
# Export Configuration
# =============================================================================


# Default export directory (relative to app root)
DEFAULT_EXPORT_DIR = "exports"


# =============================================================================
# Export Result Types
# =============================================================================


@dataclass
class ExportResult:
    """Result of a dataset export operation."""
    status: str
    file_path: str
    metadata_path: str
    row_count: int
    error: str | None = None


@dataclass
class ExportMetadata:
    """Metadata for an exported dataset (for reproducibility)."""
    session_id: str
    exported_at: str
    row_count: int
    columns: list[str]
    grain_definition: dict[str, Any] | None
    target_definition: dict[str, Any] | None
    features: list[dict[str, Any]]
    missing_strategies: list[dict[str, str]]
    validation_summary: dict[str, int]


# =============================================================================
# Export Service
# =============================================================================


class ExportService:
    """
    Exports validated ML dataset to file with metadata.
    
    Safety rules:
    - Uses only pre-validated dataset_sql from session
    - Wraps SQL safely: SELECT * FROM (<sql>) export_data
    - Never allows INSERT/CREATE TABLE/COPY TO PROGRAM
    - Does not regenerate SQL
    """

    @staticmethod
    def _ensure_export_dir(base_dir: str = DEFAULT_EXPORT_DIR) -> str:
        """Ensure export directory exists and return absolute path."""
        abs_dir = os.path.abspath(base_dir)
        os.makedirs(abs_dir, exist_ok=True)
        return abs_dir

    @staticmethod
    def _generate_filename(session_id: str, timestamp: str, extension: str) -> str:
        """Generate deterministic filename."""
        # Clean session_id for filename safety
        safe_session = session_id.replace("-", "")[:16]
        return f"dataset_{safe_session}_{timestamp}.{extension}"

    @staticmethod
    def _wrap_sql_safely(dataset_sql: str, row_limit: int | None = None) -> str:
        """
        Wrap dataset SQL for safe export.
        
        Returns: SELECT * FROM (<dataset_sql>) export_data [LIMIT n]
        """
        sql_clean = dataset_sql.strip().rstrip(";")
        
        wrapped = f"""SELECT * FROM (
    {sql_clean}
) export_data"""
        
        if row_limit and row_limit > 0:
            wrapped += f" LIMIT {int(row_limit)}"
        
        return wrapped

    @staticmethod
    def _extract_session_metadata(session: dict[str, Any]) -> tuple[
        dict | None,  # grain_definition
        dict | None,  # target_definition
        list[dict],   # features
        list[dict],   # missing_strategies
        dict,         # validation_summary
    ]:
        """Extract metadata from session for reproducibility."""
        # Grain
        grain = session.get("grain_definition")
        grain_dict = None
        if grain:
            grain_dict = {
                "entity_type": getattr(grain, "entity_type", None),
                "entity_table": getattr(grain, "entity_table", None),
                "entity_id_column": getattr(grain, "entity_id_column", None),
                "observation_date_column": getattr(grain, "observation_date_column", None),
                "deduplication_rule": getattr(grain, "deduplication_rule", None),
            }
        
        # Target
        target = session.get("target_definition")
        target_dict = None
        if target:
            target_dict = {
                "target_name": getattr(target, "target_name", None),
                "label_table": getattr(target, "label_table", None),
                "window_months": getattr(target, "window_months", None),
            }
        
        # Features (from session if stored)
        features = session.get("assembled_features", [])
        features_list = []
        for f in features:
            features_list.append({
                "name": f.get("name", "") if isinstance(f, dict) else getattr(f, "name", ""),
                "feature_columns": f.get("feature_columns", []) if isinstance(f, dict) else getattr(f, "feature_columns", []),
                "window_description": f.get("window_description", "") if isinstance(f, dict) else getattr(f, "window_description", ""),
                "max_source_time_column": f.get("max_source_time_column", "") if isinstance(f, dict) else getattr(f, "max_source_time_column", ""),
            })
        
        # Missing strategies
        missing = session.get("missing_strategies", [])
        
        # Validation summary
        validation = session.get("validation_result", {})
        validation_summary = {
            "errors": validation.get("error_count", 0) if isinstance(validation, dict) else 0,
            "warnings": validation.get("warning_count", 0) if isinstance(validation, dict) else 0,
        }
        
        return grain_dict, target_dict, features_list, missing, validation_summary

    @staticmethod
    def export_dataset(
        engine: Engine,
        dataset_sql: str,
        session_id: str,
        session: dict[str, Any],
        export_format: str = "csv",
        row_limit: int | None = None,
        include_metadata: bool = True,
        export_dir: str = DEFAULT_EXPORT_DIR,
    ) -> ExportResult:
        """
        Export validated dataset to file with metadata.
        
        Args:
            engine: SQLAlchemy engine
            dataset_sql: Pre-validated dataset SQL (from session)
            session_id: Session identifier
            session: Full session dict for metadata extraction
            export_format: Output format (csv only for now)
            row_limit: Optional row limit
            include_metadata: Whether to generate metadata JSON
            export_dir: Directory for export files
            
        Returns:
            ExportResult with file paths and row count
        """
        # Validate format
        if export_format.lower() != "csv":
            return ExportResult(
                status="error",
                file_path="",
                metadata_path="",
                row_count=0,
                error=f"Unsupported format: {export_format}. Only 'csv' is supported.",
            )
        
        # Generate timestamp and filenames
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        abs_export_dir = ExportService._ensure_export_dir(export_dir)
        
        csv_filename = ExportService._generate_filename(session_id, timestamp, "csv")
        csv_path = os.path.join(abs_export_dir, csv_filename)
        
        metadata_filename = ExportService._generate_filename(session_id, timestamp, "metadata.json")
        metadata_path = os.path.join(abs_export_dir, metadata_filename)
        
        # Wrap SQL safely
        export_sql = ExportService._wrap_sql_safely(dataset_sql, row_limit)
        
        try:
            # Execute and stream to CSV
            row_count = 0
            columns = []
            
            with engine.connect() as conn:
                result = conn.execute(text(export_sql))
                columns = list(result.keys())
                
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    
                    # Write header
                    writer.writerow(columns)
                    
                    # Write data rows
                    for row in result:
                        writer.writerow(row)
                        row_count += 1
            
            # Generate metadata JSON if requested
            if include_metadata:
                grain_dict, target_dict, features_list, missing, validation_summary = \
                    ExportService._extract_session_metadata(session)
                
                metadata = ExportMetadata(
                    session_id=session_id,
                    exported_at=datetime.utcnow().isoformat() + "Z",
                    row_count=row_count,
                    columns=columns,
                    grain_definition=grain_dict,
                    target_definition=target_dict,
                    features=features_list,
                    missing_strategies=missing,
                    validation_summary=validation_summary,
                )
                
                with open(metadata_path, "w", encoding="utf-8") as f:
                    json.dump(asdict(metadata), f, indent=2, default=str)
            else:
                metadata_path = ""
            
            return ExportResult(
                status="success",
                file_path=csv_path,
                metadata_path=metadata_path,
                row_count=row_count,
            )
            
        except Exception as e:
            return ExportResult(
                status="error",
                file_path="",
                metadata_path="",
                row_count=0,
                error=f"Export failed: {str(e)[:200]}",
            )



export_service = ExportService()
