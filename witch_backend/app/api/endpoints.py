"""
API Endpoints
Contains route handlers for the Witch application.
"""

import os
import uuid
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel
from sqlalchemy import text

from app.core.config import settings
from app.services.data_manager import DataSession, get_session, sessions
from app.services.db_service import (
    db_connector,
    ConnectionConfig,
    DBConnector,
    SchemaDiscovery,
    RelationshipDetector,
    AvailabilityChecker,
    RelevanceIdentifier,
)
from app.services.feature_service import feature_engineer
from app.services.grain_service import GrainDefinition, GrainService
from app.services.llm_service import llm_client
from app.services.quality_service import quality_auditor
from app.services.target_service import target_engineer, TargetDefinition, TargetService
from app.services.dataset_assembler_service import DatasetAssembler, FeatureSQL
from app.services.observation_aware_feature_service import (
    ObservationAwareFeatureService, FeatureDefinition, FeatureTemplateType
)
from app.services.schema_service import (
    schema_service, TableInfo as SchemaTableInfo, EntityColumn, TableProfile, CostEstimate
)
from app.services.join_service import suggest_join_keys, analyze_join, fetch_fk_graph

router = APIRouter()


# ============================================================================
# Pydantic Models
# ============================================================================


class ChatRequest(BaseModel):
    """Request model for chat endpoint."""

    session_id: str
    message: str


class ChatResponse(BaseModel):
    """Response model for chat endpoint."""

    result: str | None = None
    plot_json: str | None = None
    status: str


class UploadResponse(BaseModel):
    """Response model for upload endpoint."""

    session_id: str
    filename: str
    preview: dict[str, Any]


class ResetRequest(BaseModel):
    """Request model for reset endpoint."""

    session_id: str


class ResetResponse(BaseModel):
    """Response model for reset endpoint."""

    status: str
    message: str


class UndoRequest(BaseModel):
    """Request model for undo endpoint."""

    session_id: str


class UndoResponse(BaseModel):
    """Response model for undo endpoint."""

    status: str
    message: str
    row_count: int | None = None


# ============================================================================

# ============================================================================


class DBConnectRequest(BaseModel):
    """Request model for database connection."""

    host: str
    port: int = 5432
    user: str
    password: str
    database: str
    db_type: str = "postgres"
    ssl_mode: str = "prefer"
    schema_whitelist: list[str] | None = None
    statement_timeout_seconds: int = 30


class DBConnectResponse(BaseModel):
    """Response model for database connection."""

    session_id: str
    tables: list[str]
    status: str
    message: str
    db_version: str | None = None
    accessible_schemas: list[str] | None = None


class DiscoverTablesRequest(BaseModel):
    """Request for schema discovery."""

    session_id: str
    schemas: list[str] | None = None


class ColumnInfo(BaseModel):
    """Column metadata."""

    name: str
    type: str
    nullable: bool
    is_primary_key: bool = False
    is_unique: bool = False
    is_date_like: bool = False


class TableInfo(BaseModel):
    """Table metadata."""

    schema_name: str
    name: str
    type: str
    row_count_estimate: int
    column_count: int
    columns: list[ColumnInfo]
    primary_key: list[str]
    date_columns: list[str]
    freshness: dict[str, Any] | None = None


class DiscoverTablesResponse(BaseModel):
    """Response with discovered tables."""

    tables: list[TableInfo]
    total_count: int
    schemas_scanned: list[str]
    status: str


class DetectRelationshipsRequest(BaseModel):
    """Request for relationship detection."""

    session_id: str
    schemas: list[str] | None = None


class RelationshipInfo(BaseModel):
    """Relationship between tables."""

    type: str  # "confirmed" or "suggested"
    parent_schema: str
    parent_table: str
    parent_column: str
    child_schema: str
    child_table: str
    child_column: str
    cardinality: str
    confidence: float
    reason: str | None = None


class DetectRelationshipsResponse(BaseModel):
    """Response with detected relationships."""

    confirmed: list[RelationshipInfo]
    suggested: list[RelationshipInfo]
    total_confirmed: int
    total_suggested: int
    status: str


class CheckAvailabilityRequest(BaseModel):
    """Request for availability check."""

    session_id: str
    table_names: list[str] | None = None
    freshness_threshold_days: int = 90


class AvailabilityIssue(BaseModel):
    """Issue found during availability check."""

    type: str
    message: str


class TableAvailability(BaseModel):
    """Availability report for a table."""

    schema_name: str
    table: str
    row_count_estimate: int
    access: str
    freshness: list[dict[str, Any]]
    issues: list[AvailabilityIssue]
    status: str


class CheckAvailabilityResponse(BaseModel):
    """Response with availability report."""

    reports: list[TableAvailability]
    summary: dict[str, int]
    status: str


class IdentifyRelevantRequest(BaseModel):
    """Request for relevant data identification."""

    session_id: str
    use_case: str | None = None  # "churn", "fraud", "default"
    custom_description: str | None = None


class IdentifyRelevantResponse(BaseModel):
    """Response with relevant data suggestions."""

    use_case: str
    description: str | None
    entity_table: dict[str, Any] | None
    label_candidates: list[dict[str, Any]]
    feature_candidates: list[dict[str, Any]]
    time_candidates: list[dict[str, Any]]
    status: str


class DBChatRequest(BaseModel):
    """Request model for database chat endpoint."""

    session_id: str
    message: str


class DBChatResponse(BaseModel):
    """Response model for database chat endpoint."""

    sql_query: str | None = None
    result: str | None = None
    data: list[dict] | None = None
    status: str


class AuditTableRequest(BaseModel):
    """Request model for table audit endpoint."""

    session_id: str
    table_name: str
    sample_size: int | None = None


class AuditTableResponse(BaseModel):
    """Response model for table audit endpoint."""

    table_name: str
    row_count: int
    columns: dict[str, Any]
    alerts: list[dict[str, Any]]
    summary: dict[str, Any]
    status: str


# Global storage for database sessions
db_sessions: dict[str, dict[str, Any]] = {}


# ============================================================================
# Routes
# ============================================================================


@router.post("/upload", response_model=UploadResponse)
async def upload_file(file: UploadFile = File(...)):
    """
    Upload a CSV or Excel file to start a new data session.

    Args:
        file: The uploaded file (CSV or Excel).

    Returns:
        Session ID, filename, and data preview.
    """
    # Validate file extension
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")

    allowed_extensions = (".csv", ".xlsx", ".xls")
    if not file.filename.lower().endswith(allowed_extensions):
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type. Allowed: {', '.join(allowed_extensions)}",
        )

    # Ensure upload directory exists
    os.makedirs(settings.UPLOAD_DIR, exist_ok=True)

    # Generate unique filename to avoid collisions
    file_extension = os.path.splitext(file.filename)[1]
    unique_filename = f"{uuid.uuid4()}{file_extension}"
    file_path = os.path.join(settings.UPLOAD_DIR, unique_filename)

    # Save the file
    try:
        contents = await file.read()
        with open(file_path, "wb") as f:
            f.write(contents)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")

    # Create a new data session
    try:
        session = DataSession(file_path)
    except ValueError as e:
        # Clean up the file if session creation fails
        os.remove(file_path)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        os.remove(file_path)
        raise HTTPException(status_code=500, detail=f"Failed to process file: {str(e)}")

    # Generate session ID and store
    session_id = str(uuid.uuid4())
    sessions[session_id] = session

    return UploadResponse(
        session_id=session_id,
        filename=file.filename,
        preview=session.get_preview(),
    )


@router.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    Process a natural language query against the uploaded data.

    Args:
        request: Contains session_id and user message.

    Returns:
        Result text, plot JSON (if any), and status.
    """
    # Retrieve session
    session = get_session(request.session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found")

    
    session.add_message("user", request.message)

    
    history_str = session.get_chat_history_str()
    preview = session.get_preview()
    preview_str = f"Columns: {preview['columns']}\nDtypes: {preview['dtypes']}\nSample rows: {preview['rows']}"

    
    try:
        generated_code = await llm_client.generate_code(
            data_preview=preview_str,
            user_query=request.message,
            chat_history=history_str,
        )
    except Exception as e:
        return ChatResponse(
            result=f"Failed to generate code: {str(e)}",
            plot_json=None,
            status="error",
        )

    
    execution_result = session.execute_code(generated_code)

    
    if execution_result["status"] == "error":
        try:
            fixed_code = await llm_client.fix_code(
                broken_code=generated_code,
                error_message=execution_result["result"] or "Unknown error",
                data_preview=preview_str,
                chat_history=history_str,
            )
            # Try executing the fixed code
            execution_result = session.execute_code(fixed_code)
        except Exception as e:
            # If fixing also fails, return the original error
            return ChatResponse(
                result=f"Code execution failed and auto-fix failed: {str(e)}",
                plot_json=None,
                status="error",
            )

    
    result_text = execution_result.get("result") or ""
    if result_text:
        session.add_message("assistant", result_text)

    
    return ChatResponse(
        result=execution_result.get("result"),
        plot_json=execution_result.get("plot_json"),
        status=execution_result["status"],
    )


@router.post("/reset", response_model=ResetResponse)
async def reset_session(request: ResetRequest):
    """
    Reset a data session to its original state.

    Args:
        request: Contains the session_id to reset.

    Returns:
        Success message.
    """
    session = get_session(request.session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found")

    session.reset()

    return ResetResponse(
        status="success",
        message="Session has been reset to original data state",
    )


@router.post("/undo", response_model=UndoResponse)
async def undo_action(request: UndoRequest):
    """
    Undo the last dataframe modification.

    Args:
        request: Contains the session_id.

    Returns:
        Success or error message with current row count.
    """
    session = get_session(request.session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found")

    success = session.undo()

    if success:
        return UndoResponse(
            status="success",
            message=f"Undid last action. {len(session.df_active):,} rows now.",
            row_count=len(session.df_active),
        )
    else:
        return UndoResponse(
            status="error",
            message="Nothing to undo",
            row_count=None,
        )


# ============================================================================
# Database Routes
# ============================================================================


def _format_db_result(data: list[dict], columns: list[str], user_query: str) -> str:
    """
    Format database query results into a user-friendly message.

    Args:
        data: List of row dictionaries from the query.
        columns: List of column names.
        user_query: The original user question (for context).

    Returns:
        A natural language summary of the results.
    """
    row_count = len(data)

    # Case 1: No results
    if row_count == 0:
        return "No results found matching your query."

    # Case 2: Single value (1 row, 1 column) - return as a sentence
    if row_count == 1 and len(columns) == 1:
        col_name = columns[0]
        value = data[0][col_name]

        # Format the value nicely
        if isinstance(value, (int, float)):
            # Format numbers with commas
            if isinstance(value, float):
                formatted_value = f"{value:,.2f}"
            else:
                formatted_value = f"{value:,}"
        else:
            formatted_value = str(value)

        # Create a readable column name
        readable_col = col_name.replace("_", " ").title()

        return f"The {readable_col.lower()} is **{formatted_value}**."

    # Case 3: Single row, multiple columns - summarize the row
    if row_count == 1:
        return "Here is the result based on your query."

    # Case 4: Multiple rows
    if row_count <= 5:
        return f"Found {row_count} results."
    elif row_count <= 100:
        return f"Found {row_count:,} rows matching your query."
    else:
        return f"Found {row_count:,} rows. Showing the first 100 results."


# ============================================================================

# ============================================================================


@router.post("/connect-db", response_model=DBConnectResponse)
async def connect_database(request: DBConnectRequest):
    """
    1.1 CONNECT - Connect to a database with safety limits.

    Args:
        request: Database connection details including SSL and limits.

    Returns:
        Session ID, list of tables, connection status, and database info.
    """
    try:
        # Build connection config with safety limits
        config = ConnectionConfig(
            host=request.host,
            port=request.port,
            user=request.user,
            password=request.password,
            database=request.database,
            db_type=request.db_type,
            ssl_mode=request.ssl_mode,
            schema_whitelist=request.schema_whitelist,
            statement_timeout_seconds=request.statement_timeout_seconds,
        )

        # Create engine with limits
        engine = DBConnector.create_engine_from_config(config)

        # Test connection and get info
        connection_info = DBConnector.test_connection(engine)

        # Scan schema (legacy format for backward compatibility)
        schema_info = db_connector.scan_schema(engine)

        # Generate session ID
        session_id = str(uuid.uuid4())

        # Determine schemas to use
        schemas_to_use = request.schema_whitelist or ["public"]

        # Store session info (enhanced)
        db_sessions[session_id] = {
            "engine": engine,
            "config": config,
            "schema_summary": schema_info["schema_summary"],
            "table_list": schema_info["table_list"],
            "tables_detail": schema_info["tables_detail"],
            "conversation_history": [],
            "db_version": connection_info["version"],
            "accessible_schemas": connection_info["accessible_schemas"],
            "schemas_in_use": schemas_to_use,
            "discovered_tables": None,  # Will be populated by discover endpoint
            "relationships": None,  # Will be populated by relationships endpoint
        }

        return DBConnectResponse(
            session_id=session_id,
            tables=schema_info["table_list"],
            status="connected",
            message=f"Successfully connected to {request.database}. Found {len(schema_info['table_list'])} tables.",
            db_version=connection_info["version"],
            accessible_schemas=connection_info["accessible_schemas"],
        )

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to connect to database: {str(e)}",
        )


@router.post("/discover-tables", response_model=DiscoverTablesResponse)
async def discover_tables(request: DiscoverTablesRequest):
    """
    1.2 DISCOVER - Discover tables, views, columns, and metadata.

    Returns detailed table information including:
    - Table type (table, view, materialized view)
    - Row count estimates
    - Primary keys and unique constraints
    - Date-like columns
    - Freshness per date column
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    engine = db_session["engine"]
    schemas = request.schemas or db_session.get("schemas_in_use", ["public"])

    try:
        result = SchemaDiscovery.discover_tables(engine, schemas)

        # Store in session for later use
        db_session["discovered_tables"] = result["tables"]

        # Convert to response format
        tables = []
        for t in result["tables"]:
            columns = [
                ColumnInfo(
                    name=c["name"],
                    type=c["type"],
                    nullable=c["nullable"],
                    is_primary_key=c.get("is_primary_key", False),
                    is_unique=c.get("is_unique", False),
                    is_date_like=c.get("is_date_like", False),
                )
                for c in t["columns"]
            ]
            tables.append(
                TableInfo(
                    schema_name=t["schema"],
                    name=t["name"],
                    type=t["type"],
                    row_count_estimate=t["row_count_estimate"],
                    column_count=t["column_count"],
                    columns=columns,
                    primary_key=t["primary_key"],
                    date_columns=t["date_columns"],
                    freshness=t.get("freshness"),
                )
            )

        return DiscoverTablesResponse(
            tables=tables,
            total_count=result["total_count"],
            schemas_scanned=result["schemas_scanned"],
            status="success",
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Discovery failed: {str(e)}",
        )


@router.post("/detect-relationships", response_model=DetectRelationshipsResponse)
async def detect_relationships(request: DetectRelationshipsRequest):
    """
    1.3 RELATIONSHIPS - Detect relationships between tables.

    Returns:
    - Confirmed relationships (from foreign keys)
    - Suggested relationships (inferred from patterns)
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    engine = db_session["engine"]
    schemas = request.schemas or db_session.get("schemas_in_use", ["public"])

    # Need discovered tables first
    tables = db_session.get("discovered_tables")
    if not tables:
        # Run discovery first
        result = SchemaDiscovery.discover_tables(engine, schemas)
        tables = result["tables"]
        db_session["discovered_tables"] = tables

    try:
        result = RelationshipDetector.detect_relationships(engine, tables, schemas)

        # Store in session
        db_session["relationships"] = result

        # Convert to response format
        confirmed = [
            RelationshipInfo(
                type=r["type"],
                parent_schema=r["parent_schema"],
                parent_table=r["parent_table"],
                parent_column=r["parent_column"],
                child_schema=r["child_schema"],
                child_table=r["child_table"],
                child_column=r["child_column"],
                cardinality=r["cardinality"],
                confidence=r["confidence"],
                reason=r.get("reason"),
            )
            for r in result["confirmed"]
        ]

        suggested = [
            RelationshipInfo(
                type=r["type"],
                parent_schema=r["parent_schema"],
                parent_table=r["parent_table"],
                parent_column=r["parent_column"],
                child_schema=r["child_schema"],
                child_table=r["child_table"],
                child_column=r["child_column"],
                cardinality=r["cardinality"],
                confidence=r["confidence"],
                reason=r.get("reason"),
            )
            for r in result["suggested"]
        ]

        return DetectRelationshipsResponse(
            confirmed=confirmed,
            suggested=suggested,
            total_confirmed=result["total_confirmed"],
            total_suggested=result["total_suggested"],
            status="success",
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Relationship detection failed: {str(e)}",
        )


@router.post("/check-availability", response_model=CheckAvailabilityResponse)
async def check_availability(request: CheckAvailabilityRequest):
    """
    1.5 AVAILABILITY - Check data availability and freshness.

    Checks:
    - Access permissions
    - Row counts
    - Data freshness per date column
    - Issues (empty, stale, access denied)
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    engine = db_session["engine"]

    # Get tables to check
    all_tables = db_session.get("discovered_tables", [])
    if request.table_names:
        tables = [t for t in all_tables if t["name"] in request.table_names]
    else:
        tables = all_tables

    if not tables:
        raise HTTPException(status_code=400, detail="No tables to check")

    try:
        result = AvailabilityChecker.check_availability(
            engine, tables, request.freshness_threshold_days
        )

        # Convert to response format
        reports = [
            TableAvailability(
                schema_name=r["schema"],
                table=r["table"],
                row_count_estimate=r["row_count_estimate"],
                access=r["access"],
                freshness=r["freshness"],
                issues=[AvailabilityIssue(type=i["type"], message=i["message"]) for i in r["issues"]],
                status=r["status"],
            )
            for r in result["reports"]
        ]

        return CheckAvailabilityResponse(
            reports=reports,
            summary=result["summary"],
            status="success",
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Availability check failed: {str(e)}",
        )


@router.post("/identify-relevant", response_model=IdentifyRelevantResponse)
async def identify_relevant(request: IdentifyRelevantRequest):
    """
    1.4 IDENTIFY - Identify relevant tables for a use case.

    Supports predefined use cases (churn, fraud, default) or custom.
    Returns suggestions for entity table, labels, features, and time columns.
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    # Get discovered tables
    tables = db_session.get("discovered_tables")
    if not tables:
        engine = db_session["engine"]
        schemas = db_session.get("schemas_in_use", ["public"])
        result = SchemaDiscovery.discover_tables(engine, schemas)
        tables = result["tables"]
        db_session["discovered_tables"] = tables

    try:
        result = RelevanceIdentifier.suggest_relevant_data(
            tables,
            use_case=request.use_case,
            custom_description=request.custom_description,
        )

        return IdentifyRelevantResponse(
            use_case=result["use_case"],
            description=result["description"],
            entity_table=result["entity_table"],
            label_candidates=result["label_candidates"],
            feature_candidates=result["feature_candidates"],
            time_candidates=result["time_candidates"],
            status="success",
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Identification failed: {str(e)}",
        )


# ============================================================================

# ============================================================================


class DefineGrainRequest(BaseModel):
    """Request for defining dataset grain."""

    session_id: str
    entity_type: str  # "customer", "account", "transaction", "loan"
    entity_table: str
    entity_id_column: str
    observation_date_column: str
    observation_date_type: str = "column"  # "column" or "fixed"
    observation_date_value: str | None = None  # Required if type="fixed"
    deduplication_rule: str = "keep_latest"  # "keep_first", "keep_latest", "keep_all", "error"
    dedup_order_by: str | None = None  # Column to order by for dedup
    dedup_tiebreaker: str | None = None  # Secondary ordering column


class GrainStats(BaseModel):
    """Statistics about the grain."""

    total_rows_estimate: int  # May be estimate from pg_class or exact count
    total_rows_is_estimate: bool = True  # True if from pg_class estimate, False if exact COUNT(*)
    unique_entities: int
    duplicate_entity_count: int  # Entities with multiple rows
    duplicate_entity_obs_count: int = 0  # For keep_all: (entity, obs_date) pairs with duplicates
    null_entity_count: int
    null_obs_date_count: int
    obs_date_min: str | None
    obs_date_max: str | None
    days_since_max_obs: int | None = None  # Days since most recent observation


class DefineGrainResponse(BaseModel):
    """Response with grain definition and validation."""

    grain_definition: dict[str, Any]
    stats: GrainStats | None
    warnings: list[str]
    errors: list[str]
    status: str  # "valid", "warning", "invalid"


class PreviewGrainRequest(BaseModel):
    """Request for previewing grain."""

    session_id: str
    limit: int = 100


class PreviewGrainResponse(BaseModel):
    """Response with grain preview."""

    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    sql: str
    status: str


# ============================================================================

# ============================================================================


@router.post("/define-grain", response_model=DefineGrainResponse)
async def define_grain_legacy(request: DefineGrainRequest):
    """
    DEPRECATED: Use /grain/define for enhanced temporal features.

    2.1 DEFINE GRAIN - Define the entity and observation point.

    This is the foundation for the entire dataset:
    - What is "one row"? (entity)
    - When do we observe it? (observation_date)
    - How to handle duplicates?
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    engine = db_session["engine"]

    # Create grain definition
    grain = GrainDefinition(
        entity_type=request.entity_type,
        entity_table=request.entity_table,
        entity_id_column=request.entity_id_column,
        observation_date_column=request.observation_date_column,
        observation_date_type=request.observation_date_type,
        observation_date_value=request.observation_date_value,
        deduplication_rule=request.deduplication_rule,
        dedup_order_by=request.dedup_order_by or request.observation_date_column,
        dedup_tiebreaker=request.dedup_tiebreaker,
        schema="public",
    )

    try:
        # Validate grain
        result = GrainService.validate_grain(engine, grain)

        # Store in session for later steps
        if result["status"] != "invalid":
            db_session["grain_definition"] = grain
            db_session["grain_sql"] = GrainService.generate_grain_sql(grain)

        # Convert stats
        stats = None
        if result.get("stats"):
            s = result["stats"]
            stats = GrainStats(
                total_rows_estimate=s.get("total_rows_estimate", 0),
                total_rows_is_estimate=s.get("total_rows_is_estimate", True),
                unique_entities=s.get("unique_entities", 0),
                duplicate_entity_count=s.get("duplicate_entity_count", 0),
                duplicate_entity_obs_count=s.get("duplicate_entity_obs_count", 0),
                null_entity_count=s.get("null_entity_count", 0),
                null_obs_date_count=s.get("null_obs_date_count", 0),
                obs_date_min=s.get("obs_date_min"),
                obs_date_max=s.get("obs_date_max"),
                days_since_max_obs=s.get("days_since_max_obs"),
            )

        return DefineGrainResponse(
            grain_definition=result["grain_definition"],
            stats=stats,
            warnings=result["warnings"],
            errors=result["errors"],
            status=result["status"],
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Grain definition failed: {str(e)}",
        )


@router.post("/preview-grain", response_model=PreviewGrainResponse)
async def preview_grain_legacy(request: PreviewGrainRequest):
    """
    DEPRECATED: Use /grain/preview for enhanced temporal features.

    Preview the grain (first N rows after applying grain logic).

    Requires grain to be defined first via /define-grain.
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    grain = db_session.get("grain_definition")
    if grain is None:
        raise HTTPException(status_code=400, detail="Grain not defined. Call /define-grain first.")

    engine = db_session["engine"]

    try:
        result = GrainService.preview_grain(engine, grain, request.limit)

        return PreviewGrainResponse(
            columns=result["columns"],
            rows=result["rows"],
            row_count=result["row_count"],
            sql=result["sql"],
            status="success",
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Preview failed: {str(e)}",
        )


# ============================================================================

# ============================================================================


class DefineTargetRequest(BaseModel):
    """Request for defining target variable."""

    session_id: str
    label_table: str
    label_join_column: str
    label_event_column: str
    label_event_time_column: str
    positive_values: list[str]
    window_type: str = "fixed"  # "fixed" or "variable"
    window_months: int = 12
    window_end_column: str | None = None
    maturity_months: int = 0
    extraction_date: str | None = None  # YYYY-MM-DD for reproducibility
    target_name: str | None = None
    schema: str = "public"


class TargetStats(BaseModel):
    """Statistics about the target."""

    label_table_rows: int = 0
    event_date_min: str | None = None
    event_date_max: str | None = None


class DefineTargetResponse(BaseModel):
    """Response with target definition and validation."""

    target_definition: dict[str, Any]
    stats: TargetStats | None
    warnings: list[dict[str, Any]]
    errors: list[str]
    status: str  # "valid", "warning", "invalid"


class TargetDistributionRequest(BaseModel):
    """Request for target distribution."""

    session_id: str


class TargetDistributionResponse(BaseModel):
    """Response with target distribution."""

    target_name: str
    total_samples: int
    class_0_count: int
    class_1_count: int
    class_0_pct: float
    class_1_pct: float
    distribution: list[dict[str, Any]]
    warnings: list[dict[str, Any]]
    is_usable: bool
    status: str


class CohortAnalysisRequest(BaseModel):
    """Request for cohort analysis."""

    session_id: str
    period: str = "month"  # "month" or "quarter"


class CohortAnalysisResponse(BaseModel):
    """Response with cohort analysis."""

    target_name: str
    period: str
    cohorts: list[dict[str, Any]]
    avg_positive_rate: float
    std_dev: float
    coefficient_of_variation: float
    stability: str
    stability_message: str
    status: str


# ============================================================================

# ============================================================================


@router.post("/define-target", response_model=DefineTargetResponse)
async def define_target(request: DefineTargetRequest):
    """
    2.3 DEFINE TARGET - Define the target variable with time windows.

    This is the label/outcome for ML:
    - What event are we predicting? (label_event_column, positive_values)
    - When does it need to happen? (window_months after observation_date)
    - Wait for maturity? (maturity_months)

    Requires grain to be defined first via /define-grain.
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    grain = db_session.get("grain_definition")
    if grain is None:
        raise HTTPException(status_code=400, detail="Grain not defined. Call /define-grain first.")

    engine = db_session["engine"]

    # Create target definition
    try:
        target = TargetDefinition(
            label_table=request.label_table,
            label_join_column=request.label_join_column,
            label_event_column=request.label_event_column,
            label_event_time_column=request.label_event_time_column,
            positive_values=request.positive_values,
            window_type=request.window_type,
            window_months=request.window_months,
            window_end_column=request.window_end_column,
            maturity_months=request.maturity_months,
            extraction_date=request.extraction_date,
            target_name=request.target_name,
            schema=request.schema,
        )
    except ValueError as e:
        return DefineTargetResponse(
            target_definition={},
            stats=None,
            warnings=[],
            errors=[str(e)],
            status="invalid",
        )

    try:
        # Validate target
        result = TargetService.validate_target(engine, target, grain)

        # Store in session for later steps
        if result["status"] != "invalid":
            db_session["target_definition"] = target
            db_session["target_sql"] = TargetService.generate_target_sql(target, grain)

        # Convert stats
        stats = None
        if result.get("stats"):
            s = result["stats"]
            stats = TargetStats(
                label_table_rows=s.get("label_table_rows", 0),
                event_date_min=s.get("event_date_min"),
                event_date_max=s.get("event_date_max"),
            )

        return DefineTargetResponse(
            target_definition=result["target_definition"],
            stats=stats,
            warnings=result["warnings"],
            errors=result["errors"],
            status=result["status"],
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Target definition failed: {str(e)}",
        )


@router.post("/target-distribution", response_model=TargetDistributionResponse)
async def target_distribution(request: TargetDistributionRequest):
    """
    Get target variable distribution with imbalance warnings.

    Requires target to be defined first via /define-target.
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    grain = db_session.get("grain_definition")
    if grain is None:
        raise HTTPException(status_code=400, detail="Grain not defined. Call /define-grain first.")

    target = db_session.get("target_definition")
    if target is None:
        raise HTTPException(status_code=400, detail="Target not defined. Call /define-target first.")

    engine = db_session["engine"]

    try:
        result = TargetService.get_distribution(engine, target, grain)

        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result.get("error", "Unknown error"))

        return TargetDistributionResponse(
            target_name=result["target_name"],
            total_samples=result["total_samples"],
            class_0_count=result["class_0_count"],
            class_1_count=result["class_1_count"],
            class_0_pct=result["class_0_pct"],
            class_1_pct=result["class_1_pct"],
            distribution=result["distribution"],
            warnings=result["warnings"],
            is_usable=result["is_usable"],
            status=result["status"],
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Distribution calculation failed: {str(e)}",
        )


@router.post("/cohort-analysis", response_model=CohortAnalysisResponse)
async def cohort_analysis(request: CohortAnalysisRequest):
    """
    Analyze target distribution by time cohort.

    Checks if target rate is stable over time (important for ML).

    Requires target to be defined first via /define-target.
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    grain = db_session.get("grain_definition")
    if grain is None:
        raise HTTPException(status_code=400, detail="Grain not defined. Call /define-grain first.")

    target = db_session.get("target_definition")
    if target is None:
        raise HTTPException(status_code=400, detail="Target not defined. Call /define-target first.")

    engine = db_session["engine"]

    try:
        result = TargetService.get_cohort_analysis(
            engine, target, grain, 
            period=request.period if request.period in ("month", "quarter") else "month"
        )

        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result.get("error", "Unknown error"))

        return CohortAnalysisResponse(
            target_name=result["target_name"],
            period=result["period"],
            cohorts=result["cohorts"],
            avg_positive_rate=result["avg_positive_rate"],
            std_dev=result["std_dev"],
            coefficient_of_variation=result["coefficient_of_variation"],
            stability=result["stability"],
            stability_message=result["stability_message"],
            status=result["status"],
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Cohort analysis failed: {str(e)}",
        )


# ============================================================================

# ============================================================================


class FeatureSQLInput(BaseModel):
    """Input for a single feature SQL."""

    name: str
    sql: str
    feature_columns: list[str]
    source_table: str
    time_column: str | None = None  # DEPRECATED: use max_source_time_column
    max_source_time_column: str | None = None  # For leakage verification
    window_description: str | None = None


class AssembleDatasetRequest(BaseModel):
    """Request for dataset assembly."""

    session_id: str
    features: list[FeatureSQLInput] = []
    run_quality_checks: bool = True


class JoinabilityCheck(BaseModel):
    """Result of a joinability check."""

    name: str
    grain_sample_size: int
    matched_rows: int
    unmatched_rows: int
    match_rate: float
    status: str
    warning: str | None


class LeakageCheck(BaseModel):
    """Result of a time leakage check."""

    feature_name: str
    has_time_column: bool
    leakage_detected: bool
    leakage_count: int
    sample_size: int
    status: str
    message: str | None


class QualityReport(BaseModel):
    """Quality report for assembled dataset."""

    grain: dict[str, Any]
    target: dict[str, Any]
    features: dict[str, Any]
    joinability_checks: list[JoinabilityCheck]
    leakage_checks: list[LeakageCheck]
    overall_status: str
    errors: list[str]
    warnings: list[dict[str, Any]]
    recommendations: list[str]


class AssembleDatasetResponse(BaseModel):
    """Response with assembled dataset SQL and quality report."""

    dataset_sql: str
    quality_report: QualityReport | None
    warnings: list[dict[str, Any]]
    errors: list[str]
    status: str
    feature_count: int


class CheckLeakageRequest(BaseModel):
    """Request for checking time leakage in features."""

    session_id: str
    features: list[FeatureSQLInput]


class CheckLeakageResponse(BaseModel):
    """Response with leakage check results."""

    leakage_checks: list[LeakageCheck]
    has_leakage: bool
    status: str


# ============================================================================

# ============================================================================


@router.post("/assemble-dataset", response_model=AssembleDatasetResponse)
async def assemble_dataset(request: AssembleDatasetRequest):
    """
    2.2 ASSEMBLE DATASET - Combine grain + target + features into final dataset.

    This is the central assembly point:
    - Takes grain SQL from /define-grain
    - Takes target SQL from /define-target
    - Takes feature SQLs from request
    - Enforces join contracts on (entity_id, observation_date)
    - Runs quality/joinability/time-leakage checks
    - Outputs final dataset SQL + quality report

    Requires grain and target to be defined first.
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    grain = db_session.get("grain_definition")
    if grain is None:
        raise HTTPException(status_code=400, detail="Grain not defined. Call /define-grain first.")

    target = db_session.get("target_definition")
    if target is None:
        raise HTTPException(status_code=400, detail="Target not defined. Call /define-target first.")

    engine = db_session["engine"]

    # Convert input features to FeatureSQL objects
    try:
        features = [
            FeatureSQL(
                name=f.name,
                sql=f.sql,
                feature_columns=f.feature_columns,
                source_table=f.source_table,
                time_column=f.time_column,
                max_source_time_column=f.max_source_time_column,
                window_description=f.window_description,
            )
            for f in request.features
        ]
    except ValueError as e:
        return AssembleDatasetResponse(
            dataset_sql="",
            quality_report=None,
            warnings=[],
            errors=[str(e)],
            status="error",
            feature_count=0,
        )

    try:
        # Run assembly
        result = DatasetAssembler.assemble(
            engine=engine,
            grain=grain,
            target=target,
            features=features,
            run_checks=request.run_quality_checks,
        )

        # Convert quality report if present
        quality_report = None
        if result.quality_report and result.quality_report.get("checks"):
            qr = result.quality_report
            quality_report = QualityReport(
                grain=qr.get("grain", {}),
                target=qr.get("target", {}),
                features=qr.get("features", {}),
                joinability_checks=[
                    JoinabilityCheck(**c) for c in qr.get("checks", {}).get("joinability", [])
                ],
                leakage_checks=[
                    LeakageCheck(**c) for c in qr.get("checks", {}).get("leakage", [])
                ],
                overall_status=qr.get("overall_status", "unknown"),
                errors=qr.get("errors", []),
                warnings=qr.get("warnings", []),
                recommendations=qr.get("recommendations", []),
            )

        # Store assembled SQL in session
        if result.status != "error":
            db_session["dataset_sql"] = result.dataset_sql
            db_session["features"] = features

        return AssembleDatasetResponse(
            dataset_sql=result.dataset_sql,
            quality_report=quality_report,
            warnings=result.warnings,
            errors=result.errors,
            status=result.status,
            feature_count=result.feature_count,
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Dataset assembly failed: {str(e)}",
        )


@router.post("/check-dataset-leakage", response_model=CheckLeakageResponse)
async def check_dataset_leakage(request: CheckLeakageRequest):
    """
    Check for time leakage in feature SQLs.

    Time leakage occurs when features use data from AFTER observation_date.
    This is critical for ML - features must only use data <= observation_date.

    Requires grain to be defined first.
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    grain = db_session.get("grain_definition")
    if grain is None:
        raise HTTPException(status_code=400, detail="Grain not defined. Call /define-grain first.")

    engine = db_session["engine"]
    grain_sql = GrainService.generate_grain_sql(grain)

    # Convert input features to FeatureSQL objects
    try:
        features = [
            FeatureSQL(
                name=f.name,
                sql=f.sql,
                feature_columns=f.feature_columns,
                source_table=f.source_table,
                time_column=f.time_column,
                max_source_time_column=f.max_source_time_column,
                window_description=f.window_description,
            )
            for f in request.features
        ]
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        leakage_checks = []
        has_leakage = False

        for feature in features:
            check = DatasetAssembler.check_time_leakage(engine, grain_sql, feature)
            leakage_checks.append(LeakageCheck(**check))
            if check.get("leakage_detected"):
                has_leakage = True

        return CheckLeakageResponse(
            leakage_checks=leakage_checks,
            has_leakage=has_leakage,
            status="warning" if has_leakage else "ok",
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Leakage check failed: {str(e)}",
        )


# ============================================================================

# ============================================================================


class GenerateFeatureRequest(BaseModel):
    """Request for generating observation-aware feature SQL."""

    session_id: str
    name: str  # Human-readable feature name
    key: str   # SQL-safe identifier for column names
    template_type: str  # rolling_count, rolling_sum, rolling_avg, recency, distinct_count
    source_table: str
    join_column: str
    time_column: str
    value_column: str | None = None
    window_days: int = 30
    source_schema: str = "public"
    include_grain_cte: bool = True  # False for embedded mode in assembler


class GenerateFeatureResponse(BaseModel):
    """Response with generated feature SQL."""

    sql: str
    feature_columns: list[str]
    max_source_time_column: str
    window_description: str
    status: str


class FeatureTemplateInfo(BaseModel):
    """Information about a feature template."""

    type: str
    name: str
    description: str
    requires_value_column: bool
    requires_window_days: bool


class ListFeatureTemplatesResponse(BaseModel):
    """Response with available feature templates."""

    templates: list[FeatureTemplateInfo]


# ============================================================================

# ============================================================================


@router.post("/generate-feature", response_model=GenerateFeatureResponse)
async def generate_feature(request: GenerateFeatureRequest):
    """
    DEPRECATED: Use /feature/generate for full template support.

    2.4 GENERATE FEATURE - Generate observation-date aware feature SQL.

    Features enforce:
    - Time constraint: event_time::DATE <= observation_date
    - Output contract: entity_id, observation_date, feature_columns, max_source_time
    - Join contract: GROUP BY entity_id, observation_date

    Requires grain to be defined first via /define-grain.
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    grain = db_session.get("grain_definition")
    if grain is None:
        raise HTTPException(status_code=400, detail="Grain not defined. Call /define-grain first.")

    # Validate template type
    try:
        template_type = FeatureTemplateType(request.template_type)
    except ValueError:
        valid_types = [t.value for t in FeatureTemplateType]
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid template_type. Must be one of: {valid_types}"
        )

    # Create feature definition
    try:
        feature_def = FeatureDefinition(
            name=request.name,
            key=request.key,
            template_type=template_type,
            source_table=request.source_table,
            join_column=request.join_column,
            time_column=request.time_column,
            value_column=request.value_column,
            window_days=request.window_days,
            source_schema=request.source_schema,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        result = ObservationAwareFeatureService.generate_feature_sql(
            feature_def, grain, include_grain_cte=request.include_grain_cte
        )

        return GenerateFeatureResponse(
            sql=result["sql"],
            feature_columns=result["feature_columns"],
            max_source_time_column=result["max_source_time_column"],
            window_description=result["window_description"],
            status="success",
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Feature generation failed: {str(e)}",
        )


@router.get("/list-feature-templates", response_model=ListFeatureTemplatesResponse)
async def list_feature_templates():
    """
    DEPRECATED: Use /feature/templates.
    """
    templates = ObservationAwareFeatureService.list_templates()
    return ListFeatureTemplatesResponse(
        templates=[FeatureTemplateInfo(**t) for t in templates]
    )


# ============================================================================

# ============================================================================


class MissingStrategyInput(BaseModel):
    """Input for a column's missing value strategy."""

    column_name: str
    strategy: str = "zero"  # zero, null, sentinel, mean
    add_indicator: bool = False
    sentinel_value: int = 99999


class ApplyMissingRequest(BaseModel):
    """Request for applying missing value strategies."""

    session_id: str
    feature_key: str
    feature_name: str
    source_alias: str  # e.g., feature_0
    columns: list[MissingStrategyInput]


class MissingColumnResult(BaseModel):
    """Result for a column with missing handling applied."""

    original_column: str
    sql_expression: str
    indicator_column: str | None = None
    indicator_sql: str | None = None


class ApplyMissingResponse(BaseModel):
    """Response with missing value handling SQL expressions."""

    columns: list[MissingColumnResult]
    wrapper_cte: str
    post_sql_impute: list[dict[str, str]]  # [{"column": "x", "strategy": "mean"}]
    status: str


class RecommendMissingRequest(BaseModel):
    """Request for recommended missing strategies."""

    template_type: str


class RecommendMissingResponse(BaseModel):
    """Response with recommended missing strategy."""

    template_type: str
    strategy: str
    add_indicator: bool
    reason: str


class MissingStrategyInfo(BaseModel):
    """Information about a missing value strategy."""

    strategy: str
    description: str
    sql_example: str
    best_for: list[str]


class ListMissingStrategiesResponse(BaseModel):
    """Response with available missing strategies."""

    strategies: list[MissingStrategyInfo]


# ============================================================================

# ============================================================================

from app.services.missing_service import (
    MissingValueService,
    MissingStrategy,
    FeatureColumnConfig,
    FeatureMissingConfig,
)


@router.post("/apply-missing-strategy", response_model=ApplyMissingResponse)
async def apply_missing_strategy(request: ApplyMissingRequest):
    """
    2.5 APPLY MISSING STRATEGY - Generate SQL for handling missing values.

    Applies specified strategies to feature columns:
    - zero: COALESCE(x, 0)
    - null: keep NULL
    - sentinel: COALESCE(x, 99999)
    - mean: marker for post-SQL imputation

    Also generates is_missing_<col> indicator columns when requested.
    """
    try:
        # Build column configs
        col_configs = []
        for col in request.columns:
            try:
                strategy = MissingStrategy(col.strategy.lower())
            except ValueError:
                valid = [s.value for s in MissingStrategy]
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid strategy '{col.strategy}'. Must be one of: {valid}",
                )
            
            col_configs.append(FeatureColumnConfig(
                column_name=col.column_name,
                strategy=strategy,
                add_indicator=col.add_indicator,
                sentinel_value=col.sentinel_value,
            ))
        
        # Build feature config
        config = FeatureMissingConfig(
            feature_name=request.feature_name,
            feature_key=request.feature_key,
            columns=col_configs,
            source_alias=request.source_alias,
        )
        
        # Generate column expressions and track post-SQL imputation
        column_results = []
        post_sql_impute = []
        
        for col_config in col_configs:
            sql_expr = MissingValueService.apply_strategy(
                col_config.column_name,
                col_config.strategy,
                request.source_alias,
                col_config.sentinel_value,
            )
            
            # Track columns that need post-SQL mean imputation
            if col_config.strategy == MissingStrategy.MEAN:
                post_sql_impute.append({
                    "column": col_config.column_name,
                    "strategy": "mean",
                })
            
            ind_col = None
            ind_sql = None
            if col_config.add_indicator:
                ind_col, ind_sql = MissingValueService.generate_indicator_column(
                    col_config.column_name, request.source_alias
                )
            
            column_results.append(MissingColumnResult(
                original_column=col_config.column_name,
                sql_expression=sql_expr,
                indicator_column=ind_col,
                indicator_sql=ind_sql,
            ))
        
        # Generate wrapper CTE
        wrapper_alias = f"{request.source_alias}_handled"
        wrapper_cte = MissingValueService.wrap_feature_cte(wrapper_alias, config)
        
        return ApplyMissingResponse(
            columns=column_results,
            wrapper_cte=wrapper_cte,
            post_sql_impute=post_sql_impute,
            status="success",
        )
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Missing strategy failed: {str(e)}")


@router.post("/recommend-missing-strategy", response_model=RecommendMissingResponse)
async def recommend_missing_strategy(request: RecommendMissingRequest):
    """
    Get recommended missing value strategy for a feature template type.
    """
    rec = MissingValueService.get_recommended_strategy(request.template_type)
    
    return RecommendMissingResponse(
        template_type=request.template_type,
        strategy=rec.get("strategy", MissingStrategy.NULL).value,
        add_indicator=rec.get("add_indicator", True),
        reason=rec.get("reason", "Unknown template type"),
    )


@router.get("/list-missing-strategies", response_model=ListMissingStrategiesResponse)
async def list_missing_strategies():
    """
    List available missing value handling strategies.
    """
    strategies = MissingValueService.list_strategies()
    return ListMissingStrategiesResponse(
        strategies=[MissingStrategyInfo(**s) for s in strategies]
    )


# ============================================================================

# ============================================================================


class ValidationIssueInfo(BaseModel):
    """A single validation issue."""

    severity: str  # error, warning, info
    code: str
    message: str
    location: str = ""
    suggestion: str = ""


class ValidateDatasetRequest(BaseModel):
    """Request for dataset SQL validation."""

    session_id: str
    dataset_sql: str
    feature_sqls: list[FeatureSQLInput] | None = None
    post_sql_impute: list[dict[str, str]] | None = None


class ValidateDatasetResponse(BaseModel):
    """Response with validation results."""

    valid: bool
    errors: list[ValidationIssueInfo]
    warnings: list[ValidationIssueInfo]
    info: list[ValidationIssueInfo]
    status: str


# ============================================================================

# ============================================================================

from app.services.validation_service import ValidationService, ValidationSeverity


@router.post("/validate-dataset-sql", response_model=ValidateDatasetResponse)
async def validate_dataset_sql(request: ValidateDatasetRequest):
    """
    2.6 VALIDATE DATASET SQL - Comprehensive validation of assembled dataset SQL.

    Checks:
    - SQL syntax via EXPLAIN
    - Forbidden keywords (DROP, DELETE, INSERT, UPDATE, ALTER)
    - Required output columns (entity_id, observation_date)
    - Feature column declarations
    - Mean imputation type compatibility

    Requires database connection via session_id.
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    engine = db_session.get("engine")
    if engine is None:
        raise HTTPException(status_code=400, detail="No database connection")

    try:
        # Convert feature inputs if provided
        feature_dicts = None
        if request.feature_sqls:
            feature_dicts = [
                {
                    "sql": f.sql,
                    "feature_columns": f.feature_columns,
                }
                for f in request.feature_sqls
            ]

        # Run full validation
        result = ValidationService.validate_dataset_sql(
            engine=engine,
            dataset_sql=request.dataset_sql,
            feature_sqls=feature_dicts,
            post_sql_impute=request.post_sql_impute,
        )

        db_session["validation_result"] = {"valid": result.valid}

        # Convert issues to response format
        errors = [
            ValidationIssueInfo(
                severity=i.severity.value, code=i.code, message=i.message,
                location=i.location, suggestion=i.suggestion
            )
            for i in result.issues if i.severity == ValidationSeverity.ERROR
        ]
        warnings = [
            ValidationIssueInfo(
                severity=i.severity.value, code=i.code, message=i.message,
                location=i.location, suggestion=i.suggestion
            )
            for i in result.issues if i.severity == ValidationSeverity.WARNING
        ]
        info = [
            ValidationIssueInfo(
                severity=i.severity.value, code=i.code, message=i.message,
                location=i.location, suggestion=i.suggestion
            )
            for i in result.issues if i.severity == ValidationSeverity.INFO
        ]

        return ValidateDatasetResponse(
            valid=result.valid,
            errors=errors,
            warnings=warnings,
            info=info,
            status="success",
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")


# ============================================================================

# ============================================================================


class ExportDatasetRequest(BaseModel):
    """Request for exporting dataset to file."""

    session_id: str
    format: str = "csv"  # Only csv supported for now
    row_limit: int | None = None


class ExportDatasetResponse(BaseModel):
    """Response with export result."""

    status: str
    file_path: str
    metadata_path: str
    row_count: int
    error: str | None = None


# ============================================================================

# ============================================================================

from app.services.export_service import ExportService


@router.post("/export-dataset", response_model=ExportDatasetResponse)
async def export_dataset(request: ExportDatasetRequest):
    """
    2.7 EXPORT DATASET - Export validated dataset to file with metadata.

    Exports the assembled and validated dataset SQL to:
    - CSV file with deterministic filename
    - Metadata JSON for reproducibility

    Requires:
    - Database connection via session_id
    - Dataset SQL stored in session (from /assemble-dataset)
    - Validation passed (from /validate-dataset-sql)

    Does NOT regenerate SQL - uses pre-validated dataset_sql from session.
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    engine = db_session.get("engine")
    if engine is None:
        raise HTTPException(status_code=400, detail="No database connection")

    # Get dataset SQL from session
    dataset_sql = db_session.get("dataset_sql")
    if dataset_sql is None:
        raise HTTPException(
            status_code=400,
            detail="No dataset SQL found. Call /assemble-dataset first.",
        )

    # Check validation status
    validation_result = db_session.get("validation_result", {})
    if isinstance(validation_result, dict) and not validation_result.get("valid", True):
        raise HTTPException(
            status_code=400,
            detail="Dataset validation failed. Fix errors before exporting.",
        )

    try:
        result = ExportService.export_dataset(
            engine=engine,
            dataset_sql=dataset_sql,
            session_id=request.session_id,
            session=db_session,
            export_format=request.format,
            row_limit=request.row_limit,
            include_metadata=True,
        )

        if result.status == "error":
            raise HTTPException(status_code=500, detail=result.error)

        return ExportDatasetResponse(
            status=result.status,
            file_path=result.file_path,
            metadata_path=result.metadata_path,
            row_count=result.row_count,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@router.post("/db-chat", response_model=DBChatResponse)
async def db_chat(request: DBChatRequest):
    """
    Process a natural language query against a connected database.

    Args:
        request: Contains session_id and user message.

    Returns:
        Generated SQL query, result, and data.
    """
    # Retrieve session
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    # Get schema context and history
    schema_context = db_session["schema_summary"]
    history = db_session.get("conversation_history", [])
    history_str = "\n".join([f"{h['role']}: {h['content']}" for h in history[-10:]])

    # Add user message to history
    db_session["conversation_history"].append({
        "role": "user",
        "content": request.message,
    })

    # Generate SQL query (or interpretation)
    try:
        llm_response = await llm_client.generate_sql(
            schema_context=schema_context,
            user_question=request.message,
            chat_history=history_str,
        )
    except Exception as e:
        return DBChatResponse(
            sql_query=None,
            result=f"Failed to generate SQL: {str(e)}",
            data=None,
            status="error",
        )

    # Check if LLM returned an interpretation instead of SQL
    if llm_response.strip().startswith("INTERPRET:"):
        interpretation = llm_response.strip()[10:].strip()  # Remove "INTERPRET:" prefix
        
        # Add to conversation history
        db_session["conversation_history"].append({
            "role": "assistant",
            "content": interpretation,
        })
        
        return DBChatResponse(
            sql_query=None,
            result=interpretation,
            data=None,
            status="success",
        )

    # Otherwise, treat it as SQL
    sql_query = llm_response

    # Execute the SQL query
    try:
        engine = db_session["engine"]
        with engine.connect() as conn:
            result = conn.execute(text(sql_query))
            rows = result.fetchall()
            columns = list(result.keys())

            # Convert to list of dicts
            data = [dict(zip(columns, row)) for row in rows]

            # Create user-friendly result message
            result_text = _format_db_result(data, columns, request.message)

        # Add assistant response to history (keep SQL for context)
        db_session["conversation_history"].append({
            "role": "assistant",
            "content": result_text,
        })

        # Return all rows if ≤ 100, otherwise limit to 100
        max_rows = len(data) if len(data) <= 100 else 100

        return DBChatResponse(
            sql_query=sql_query,
            result=result_text,
            data=data[:max_rows],
            status="success",
        )

    except Exception as e:
        # Try to fix the SQL
        try:
            fixed_sql = await llm_client.fix_sql(
                broken_sql=sql_query,
                error_message=str(e),
                schema_context=schema_context,
            )

            # Retry with fixed SQL
            with engine.connect() as conn:
                result = conn.execute(text(fixed_sql))
                rows = result.fetchall()
                columns = list(result.keys())
                data = [dict(zip(columns, row)) for row in rows]

                # Create user-friendly result message
                result_text = _format_db_result(data, columns, request.message)

            db_session["conversation_history"].append({
                "role": "assistant",
                "content": result_text,
            })

            # Return all rows if ≤ 100, otherwise limit to 100
            max_rows = len(data) if len(data) <= 100 else 100

            return DBChatResponse(
                sql_query=fixed_sql,
                result=result_text,
                data=data[:max_rows],
                status="success",
            )

        except Exception as fix_error:
            return DBChatResponse(
                sql_query=sql_query,
                result=f"SQL execution failed: {str(e)}",
                data=None,
                status="error",
            )


@router.post("/audit-table", response_model=AuditTableResponse)
async def audit_table(request: AuditTableRequest):
    """
    Audit a database table for data quality and ML suitability.

    Args:
        request: Contains session_id and table_name.

    Returns:
        Quality report with statistics and alerts.
    """
    # Retrieve session
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    # Verify table exists
    table_list = db_session.get("table_list", [])
    if request.table_name not in table_list:
        raise HTTPException(
            status_code=404,
            detail=f"Table '{request.table_name}' not found. Available tables: {', '.join(table_list[:10])}",
        )

    # Run the audit
    try:
        engine = db_session["engine"]
        report = quality_auditor.analyze_table(
            engine,
            request.table_name,
            sample_size=request.sample_size,
        )

        # Save audit to session history
        if "audit_history" not in db_session:
            db_session["audit_history"] = []
        
        # Add timestamp to report
        from datetime import datetime
        report["audited_at"] = datetime.now().isoformat()
        
        # Check if this table was already audited, if so update it
        existing_idx = next(
            (i for i, a in enumerate(db_session["audit_history"]) if a["table_name"] == request.table_name),
            None
        )
        if existing_idx is not None:
            db_session["audit_history"][existing_idx] = report
        else:
            db_session["audit_history"].append(report)

        return AuditTableResponse(
            table_name=report["table_name"],
            row_count=report["row_count"],
            columns=report["columns"],
            alerts=report["alerts"],
            summary=report["summary"],
            status="success",
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Audit failed: {str(e)}",
        )


class AuditHistoryRequest(BaseModel):
    """Request model for audit history endpoint."""

    session_id: str


class AuditHistoryResponse(BaseModel):
    """Response model for audit history endpoint."""

    audits: list[dict[str, Any]]
    status: str


class SuggestFeaturesRequest(BaseModel):
    """Request model for feature suggestion endpoint."""

    session_id: str
    table_name: str
    target_goal: str


class FeatureSuggestion(BaseModel):
    """A single feature suggestion."""

    name: str
    column: str
    logic: str
    sql_template: str
    type: str
    relevance: str
    description: str


class SuggestFeaturesResponse(BaseModel):
    """Response model for feature suggestion endpoint."""

    table_name: str
    target_goal: str
    grouping_column: str | None
    suggestions: list[FeatureSuggestion]
    status: str


class GenerateDatasetRequest(BaseModel):
    """Request model for dataset generation endpoint."""

    session_id: str
    table_name: str
    selected_features: list[dict[str, Any]]
    grouping_column: str | None = None


class GenerateDatasetResponse(BaseModel):
    """Response model for dataset generation endpoint."""

    sql_query: str
    feature_count: int
    status: str
    validation: dict[str, Any] | None = None


class ValidateSqlRequest(BaseModel):
    """Request model for SQL validation."""

    session_id: str
    sql_query: str


class ValidateSqlResponse(BaseModel):
    """Response model for SQL validation."""

    valid: bool
    error: str | None = None
    warning: str | None = None


class TableColumnsRequest(BaseModel):
    """Request model for getting table columns."""

    session_id: str
    table_name: str
    include_stats: bool = False  # Whether to include column statistics


class ColumnStats(BaseModel):
    """Statistics for a column."""

    null_count: int | None = None
    null_percentage: float | None = None
    distinct_count: int | None = None
    sample_values: list[str] | None = None


class ColumnInfoWithStats(BaseModel):
    """Column information with optional statistics."""

    name: str
    type: str
    stats: ColumnStats | None = None


class TableColumnsResponse(BaseModel):
    """Response model for table columns."""

    table_name: str
    columns: list[ColumnInfoWithStats]
    row_count: int | None = None
    status: str


class JoinGraphRequest(BaseModel):
    """Request for join graph."""
    session_id: str
    schema: str = "public"
    include_inferred: bool = False


class JoinGraphNode(BaseModel):
    """Join graph node."""
    id: str
    table_name: str
    schema: str
    row_estimate: int


class JoinGraphEdge(BaseModel):
    """Join graph edge."""
    id: str
    constraint_name: str
    left_schema: str
    left_table: str
    right_schema: str
    right_table: str
    left_columns: list[str]
    right_columns: list[str]
    is_unique: bool
    is_primary: bool
    left_estimate: int
    right_estimate: int
    source: str = "fk"
    confidence: float | None = None
    reason: str | None = None


class JoinGraphResponse(BaseModel):
    """Response for join graph."""
    nodes: list[JoinGraphNode]
    edges: list[JoinGraphEdge]
    status: str


class JoinSuggestRequest(BaseModel):
    """Request for join key suggestions."""
    session_id: str
    left_table: str
    right_table: str
    schema: str = "public"


class JoinKeyCandidate(BaseModel):
    """Candidate join key pair."""
    left_column: str
    right_column: str
    left_type: str
    right_type: str
    score: int
    reason: str


class JoinSuggestResponse(BaseModel):
    """Response with join key candidates."""
    left_table: str
    right_table: str
    candidates: list[JoinKeyCandidate]
    status: str


class JoinAnalyzeRequest(BaseModel):
    """Request for join analysis."""
    session_id: str
    left_table: str
    right_table: str
    left_key: str
    right_key: str
    schema: str = "public"
    sample_size: int | None = 100000


class JoinSideStats(BaseModel):
    """Join stats for a side of the join."""
    total_rows: int
    null_pct: float
    distinct_keys: int
    duplicate_rate: float


class JoinMatchStats(BaseModel):
    """Join match stats."""
    matched_keys: int
    left_key_count: int
    right_key_count: int
    left_match_rate: float
    right_match_rate: float


class JoinAnalyzeResponse(BaseModel):
    """Response for join analysis."""
    left_table: str
    right_table: str
    left_key: str
    right_key: str
    left: JoinSideStats
    right: JoinSideStats
    match: JoinMatchStats
    cardinality: str
    sampled: bool
    left_sample_percent: float | None = None
    right_sample_percent: float | None = None
    sample_size: int | None = None
    status: str


# ============================================================================
# Target Engineering Models (Data-Driven)
# ============================================================================


class DetectTargetColumnsRequest(BaseModel):
    """Request for detecting target column candidates."""

    session_id: str
    table_name: str


class TargetColumnCandidate(BaseModel):
    """A candidate column for target definition."""

    column_name: str
    column_type: str
    distinct_count: int
    is_status_like: bool
    priority: int


class DetectTargetColumnsResponse(BaseModel):
    """Response with detected target column candidates."""

    table_name: str
    candidates: list[TargetColumnCandidate]
    status: str


class GetColumnValuesRequest(BaseModel):
    """Request for getting column values."""

    session_id: str
    table_name: str
    column_name: str
    schema: str = "public"
    limit: int = 50


class ColumnValue(BaseModel):
    """A distinct value in a column with count."""

    value: str
    count: int
    percentage: float
    is_null: bool


class GetColumnValuesResponse(BaseModel):
    """Response with column values."""

    column_name: str
    table_name: str
    total_records: int
    distinct_count: int
    values: list[ColumnValue]
    sampled: bool = False
    sample_size: int | None = None
    sample_percent: float | None = None
    status: str


class GenerateTargetRequest(BaseModel):
    """Request for generating target from selected values."""

    session_id: str
    table_name: str
    column_name: str
    selected_values: list[str]
    target_name: str | None = None
    grouping_column: str | None = None


class GenerateTargetResponse(BaseModel):
    """Response with generated target SQL."""

    target_name: str
    sql_logic: str
    description: str
    column_name: str
    selected_values: list[str]
    status: str


class PreviewTargetRequest(BaseModel):
    """Request model for previewing target distribution."""

    session_id: str
    table_name: str
    sql_logic: str
    target_name: str
    grouping_column: str | None = None


class TargetDistribution(BaseModel):
    """Distribution of a target class."""

    value: int
    count: int
    percentage: float


class PreviewTargetResponse(BaseModel):
    """Response model for target distribution preview."""

    target_name: str
    total_records: int
    distribution: list[TargetDistribution]
    warnings: list[dict[str, Any]]
    is_usable: bool
    recommendation: str | None = None
    status: str


@router.post("/audit-history", response_model=AuditHistoryResponse)
async def get_audit_history(request: AuditHistoryRequest):
    """
    Get all audit reports for a database session.

    Args:
        request: Contains session_id.

    Returns:
        List of all audit reports.
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    audit_history = db_session.get("audit_history", [])

    # Return simplified version for the list (not full column details)
    simplified = []
    for audit in audit_history:
        simplified.append({
            "table_name": audit.get("table_name"),
            "row_count": audit.get("row_count"),
            "health_score": audit.get("summary", {}).get("health_score", 0),
            "total_columns": audit.get("summary", {}).get("total_columns", 0),
            "critical_count": len([a for a in audit.get("alerts", []) if a.get("level") == "critical"]),
            "warning_count": len([a for a in audit.get("alerts", []) if a.get("level") == "warning"]),
            "audited_at": audit.get("audited_at"),
        })

    return AuditHistoryResponse(
        audits=simplified,
        status="success",
    )


# ============================================================================
# Feature Engineering Routes
# ============================================================================


@router.post("/table-columns", response_model=TableColumnsResponse)
async def get_table_columns(request: TableColumnsRequest):
    """
    Get column information for a specific table, optionally with statistics.

    Args:
        request: Contains session_id, table_name, and optional include_stats flag.

    Returns:
        List of columns with their types and optional statistics.
    """
    # Retrieve session
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    # Verify table exists
    table_list = db_session.get("table_list", [])
    if request.table_name not in table_list:
        raise HTTPException(
            status_code=404,
            detail=f"Table '{request.table_name}' not found.",
        )

    # Get column details from stored schema
    tables_detail = db_session.get("tables_detail", {})
    columns_detail = tables_detail.get(request.table_name, [])

    # Normalize format - could be list or dict
    raw_columns = []
    if isinstance(columns_detail, list):
        raw_columns = columns_detail
    elif isinstance(columns_detail, dict):
        raw_columns = [{"name": k, "type": v.get("type", "")} for k, v in columns_detail.items()]

    # Build ColumnInfo list
    columns = []
    row_count = None

    # If stats requested, query the database
    if request.include_stats and raw_columns:
        engine = db_session.get("engine")
        if engine:
            try:
                # Build stats query dynamically
                stats_parts = []
                for col in raw_columns:
                    col_name = col.get("name", "")
                    if col_name:
                        # Escape column name
                        safe_col = col_name.replace('"', '""')
                        stats_parts.append(f'COUNT(*) FILTER (WHERE "{safe_col}" IS NULL) AS "{safe_col}_nulls"')
                        stats_parts.append(f'COUNT(DISTINCT "{safe_col}") AS "{safe_col}_distinct"')

                stats_sql = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    {', '.join(stats_parts)}
                FROM public."{request.table_name}"
                """

                # Execute stats query
                with engine.connect() as conn:
                    result = conn.execute(text(stats_sql))
                    row = result.fetchone()

                if row:
                    row_count = int(row[0]) if row[0] else 0
                    stats_dict = {}
                    
                    # Parse results into dict
                    col_idx = 1  # Start after total_rows
                    for col in raw_columns:
                        col_name = col.get("name", "")
                        if col_name:
                            null_count = int(row[col_idx]) if row[col_idx] else 0
                            distinct_count = int(row[col_idx + 1]) if row[col_idx + 1] else 0
                            null_pct = round((null_count / row_count) * 100, 2) if row_count > 0 else 0.0
                            
                            stats_dict[col_name] = ColumnStats(
                                null_count=null_count,
                                null_percentage=null_pct,
                                distinct_count=distinct_count,
                                sample_values=None,  # Skip samples for now
                            )
                            col_idx += 2

                    # Build columns with stats
                    for col in raw_columns:
                        col_name = col.get("name", "")
                        col_type = col.get("type", "")
                        columns.append(ColumnInfoWithStats(
                            name=col_name,
                            type=col_type,
                            stats=stats_dict.get(col_name),
                        ))

            except Exception as e:
                # If stats query fails, return columns without stats
                print(f"Stats query failed: {e}")
                for col in raw_columns:
                    columns.append(ColumnInfoWithStats(
                        name=col.get("name", ""),
                        type=col.get("type", ""),
                        stats=None,
                    ))
    else:
        # No stats requested
        for col in raw_columns:
            columns.append(ColumnInfoWithStats(
                name=col.get("name", ""),
                type=col.get("type", ""),
                stats=None,
            ))

    return TableColumnsResponse(
        table_name=request.table_name,
        columns=columns,
        row_count=row_count,
        status="success",
    )


@router.post("/join/graph", response_model=JoinGraphResponse)
async def get_join_graph(request: JoinGraphRequest):
    """
    Return FK-based join graph for the schema.
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    engine = db_session["engine"]
    table_list = db_session.get("table_list", [])

    try:
        graph = fetch_fk_graph(engine, schema=request.schema, tables=table_list)
        edges = list(graph.get("edges", []))
        nodes = list(graph.get("nodes", []))

        def _edge_key(left_table, right_table, left_cols, right_cols):
            return f"{left_table}:{right_table}:{','.join(left_cols)}:{','.join(right_cols)}"

        existing_keys = {
            _edge_key(
                edge.get("left_table", ""),
                edge.get("right_table", ""),
                edge.get("left_columns", []),
                edge.get("right_columns", []),
            )
            for edge in edges
        }

        include_inferred = request.include_inferred or len(edges) == 0
        if include_inferred:
            tables = db_session.get("discovered_tables")
            if not tables:
                result = SchemaDiscovery.discover_tables(engine, [request.schema])
                tables = result["tables"]
                db_session["discovered_tables"] = tables

            relationships = RelationshipDetector.detect_relationships(engine, tables, [request.schema])
            suggested = relationships.get("suggested", [])
            table_lookup = {(t["schema"], t["name"]): t for t in tables}

            def _column_meta(table_info, column_name):
                for col in table_info.get("columns", []):
                    if col.get("name") == column_name:
                        return col
                return {}

            inferred_edges = []
            for rel in suggested:
                left_table = rel["child_table"]
                right_table = rel["parent_table"]
                left_column = rel["child_column"]
                right_column = rel["parent_column"]
                key = _edge_key(left_table, right_table, [left_column], [right_column])
                if key in existing_keys:
                    continue

                parent_info = table_lookup.get((rel["parent_schema"], rel["parent_table"]), {})
                child_info = table_lookup.get((rel["child_schema"], rel["child_table"]), {})
                parent_col = _column_meta(parent_info, right_column)

                inferred_edges.append({
                    "id": f"inferred:{left_table}:{right_table}:{left_column}:{right_column}",
                    "constraint_name": "inferred",
                    "left_schema": rel["child_schema"],
                    "left_table": left_table,
                    "right_schema": rel["parent_schema"],
                    "right_table": right_table,
                    "left_columns": [left_column],
                    "right_columns": [right_column],
                    "is_unique": bool(parent_col.get("is_unique")) or bool(parent_col.get("is_primary_key")),
                    "is_primary": bool(parent_col.get("is_primary_key")),
                    "left_estimate": int(child_info.get("row_count_estimate") or 0),
                    "right_estimate": int(parent_info.get("row_count_estimate") or 0),
                    "source": "inferred",
                    "confidence": float(rel.get("confidence", 0.0)),
                    "reason": rel.get("reason"),
                })

            if inferred_edges:
                if edges:
                    edges.extend(inferred_edges)
                else:
                    edges = inferred_edges
                existing_tables = {n.get("table_name") for n in nodes}
                for table in tables:
                    if table["name"] not in existing_tables:
                        nodes.append({
                            "id": table["name"],
                            "table_name": table["name"],
                            "schema": table["schema"],
                            "row_estimate": int(table.get("row_count_estimate") or 0),
                        })

        return JoinGraphResponse(
            nodes=[JoinGraphNode(**n) for n in nodes],
            edges=[JoinGraphEdge(**e) for e in edges],
            status="success",
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load join graph: {str(e)}")


@router.post("/join/suggest-keys", response_model=JoinSuggestResponse)
async def suggest_join_keys_endpoint(request: JoinSuggestRequest):
    """
    Suggest join key pairs between two tables using schema heuristics.
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    table_list = db_session.get("table_list", [])
    if request.left_table not in table_list or request.right_table not in table_list:
        raise HTTPException(status_code=404, detail="Table not found in session")

    tables_detail = db_session.get("tables_detail", {})
    left_columns = tables_detail.get(request.left_table, [])
    right_columns = tables_detail.get(request.right_table, [])

    try:
        candidates = suggest_join_keys(
            request.left_table,
            request.right_table,
            left_columns,
            right_columns,
        )
        return JoinSuggestResponse(
            left_table=request.left_table,
            right_table=request.right_table,
            candidates=[JoinKeyCandidate(**c) for c in candidates],
            status="success",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to suggest keys: {str(e)}")


@router.post("/join/analyze", response_model=JoinAnalyzeResponse)
async def analyze_join_endpoint(request: JoinAnalyzeRequest):
    """
    Analyze join quality between two tables using sampling.
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    table_list = db_session.get("table_list", [])
    if request.left_table not in table_list or request.right_table not in table_list:
        raise HTTPException(status_code=404, detail="Table not found in session")

    engine = db_session["engine"]

    try:
        result = analyze_join(
            engine,
            request.left_table,
            request.right_table,
            request.left_key,
            request.right_key,
            schema=request.schema,
            sample_size=request.sample_size,
        )
        return JoinAnalyzeResponse(
            left_table=result["left_table"],
            right_table=result["right_table"],
            left_key=result["left_key"],
            right_key=result["right_key"],
            left=JoinSideStats(**result["left"]),
            right=JoinSideStats(**result["right"]),
            match=JoinMatchStats(**result["match"]),
            cardinality=result["cardinality"],
            sampled=result["sampled"],
            left_sample_percent=result.get("left_sample_percent"),
            right_sample_percent=result.get("right_sample_percent"),
            sample_size=result.get("sample_size"),
            status="success",
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to analyze join: {str(e)}")


@router.post("/suggest-features", response_model=SuggestFeaturesResponse)
async def suggest_features(request: SuggestFeaturesRequest):
    """
    Suggest ML features based on table schema and target goal.

    Args:
        request: Contains session_id, table_name, and target_goal.

    Returns:
        List of feature suggestions with SQL templates.
    """
    # Retrieve session
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    # Verify table exists
    table_list = db_session.get("table_list", [])
    if request.table_name not in table_list:
        raise HTTPException(
            status_code=404,
            detail=f"Table '{request.table_name}' not found.",
        )

    # Get table column details
    tables_detail = db_session.get("tables_detail", {})
    columns_detail = tables_detail.get(request.table_name, {})

    if not columns_detail:
        raise HTTPException(
            status_code=400,
            detail=f"No column information found for table '{request.table_name}'.",
        )

    # Get schema summary for context
    schema_summary = db_session.get("schema_summary", "")

    # Generate suggestions
    suggestions = feature_engineer.suggest_features(
        schema_summary=schema_summary,
        columns_detail=columns_detail,
        target_description=request.target_goal,
    )

    # Detect grouping column
    grouping_column = feature_engineer.detect_grouping_column(columns_detail)

    return SuggestFeaturesResponse(
        table_name=request.table_name,
        target_goal=request.target_goal,
        grouping_column=grouping_column,
        suggestions=[FeatureSuggestion(**s) for s in suggestions],
        status="success",
    )


@router.post("/generate-dataset", response_model=GenerateDatasetResponse)
async def generate_dataset(request: GenerateDatasetRequest):
    """
    Generate a SQL query to create an ML-ready dataset.

    Args:
        request: Contains session_id, table_name, selected_features, and grouping_column.

    Returns:
        SQL query string for creating the dataset.
    """
    # Retrieve session
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    # Verify table exists
    table_list = db_session.get("table_list", [])
    if request.table_name not in table_list:
        raise HTTPException(
            status_code=404,
            detail=f"Table '{request.table_name}' not found.",
        )

    # Determine grouping column
    grouping_column = request.grouping_column
    if not grouping_column:
        # Auto-detect if not provided
        tables_detail = db_session.get("tables_detail", {})
        columns_detail = tables_detail.get(request.table_name, {})
        grouping_column = feature_engineer.detect_grouping_column(columns_detail)

    # Generate SQL
    sql_query = feature_engineer.generate_dataset_sql(
        table_name=request.table_name,
        selected_features=request.selected_features,
        grouping_column=grouping_column,
    )

    # Validate the SQL using EXPLAIN
    validation = {"valid": True, "error": None, "warning": None}
    try:
        engine = db_session["engine"]
        with engine.connect() as conn:
            # Use EXPLAIN to validate without executing
            explain_query = text(f"EXPLAIN {sql_query}")
            conn.execute(explain_query)
    except Exception as e:
        error_msg = str(e)
        validation = {
            "valid": False,
            "error": error_msg,
            "warning": "The generated SQL has syntax errors. You may need to adjust the selected features.",
        }

    return GenerateDatasetResponse(
        sql_query=sql_query,
        feature_count=len(request.selected_features),
        status="success" if validation["valid"] else "warning",
        validation=validation,
    )


class SmartFeaturesRequest(BaseModel):
    """Request model for LLM-powered feature suggestion."""

    session_id: str
    table_name: str
    target_goal: str


class SmartFeaturesResponse(BaseModel):
    """Response model for LLM-powered feature suggestion."""

    table_name: str
    target_goal: str
    grouping_column: str | None
    suggestions: list[dict[str, Any]]
    status: str
    source: str = "llm"


@router.post("/suggest-features-smart", response_model=SmartFeaturesResponse)
async def suggest_features_smart(request: SmartFeaturesRequest):
    """
    Use LLM to suggest intelligent ML features based on schema and target goal.

    Args:
        request: Contains session_id, table_name, and target_goal.

    Returns:
        List of AI-generated feature suggestions.
    """
    import json

    # Retrieve session
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    # Verify table exists
    table_list = db_session.get("table_list", [])
    if request.table_name not in table_list:
        raise HTTPException(
            status_code=404,
            detail=f"Table '{request.table_name}' not found.",
        )

    # Get schema context
    schema_summary = db_session.get("schema_summary", "")

    # Get column details for grouping column detection
    tables_detail = db_session.get("tables_detail", {})
    columns_detail = tables_detail.get(request.table_name, {})
    grouping_column = feature_engineer.detect_grouping_column(columns_detail)

    try:
        # Call LLM for suggestions with grouping column context
        llm_response = await llm_client.suggest_features_llm(
            schema_context=schema_summary,
            target_goal=request.target_goal,
            table_name=request.table_name,
            grouping_column=grouping_column,
        )

        # Parse JSON response
        suggestions = json.loads(llm_response)

        return SmartFeaturesResponse(
            table_name=request.table_name,
            target_goal=request.target_goal,
            grouping_column=grouping_column,
            suggestions=suggestions,
            status="success",
            source="llm",
        )

    except json.JSONDecodeError as e:
        # Fall back to rule-based suggestions
        suggestions = feature_engineer.suggest_features(
            schema_summary=schema_summary,
            columns_detail=columns_detail,
            target_description=request.target_goal,
        )

        return SmartFeaturesResponse(
            table_name=request.table_name,
            target_goal=request.target_goal,
            grouping_column=grouping_column,
            suggestions=suggestions,
            status="success",
            source="fallback",
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Feature suggestion failed: {str(e)}",
        )


# ============================================================================
# Target Engineering Routes (Data-Driven)
# ============================================================================


@router.post("/detect-target-columns", response_model=DetectTargetColumnsResponse)
async def detect_target_columns(request: DetectTargetColumnsRequest):
    """
    Detect columns that are likely candidates for target variable definition.
    
    Looks for status/state columns with low cardinality that could define
    binary targets (e.g., state_name with values like 'Active', 'Closed').
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    table_list = db_session.get("table_list", [])
    if request.table_name not in table_list:
        raise HTTPException(status_code=404, detail=f"Table '{request.table_name}' not found.")

    engine = db_session["engine"]
    tables_detail = db_session.get("tables_detail", {})
    columns = tables_detail.get(request.table_name, [])

    # Normalize columns format
    if isinstance(columns, dict):
        columns = [{"name": k, "type": v.get("type", "")} for k, v in columns.items()]

    try:
        candidates = target_engineer.detect_target_columns(engine, request.table_name, columns)
        
        return DetectTargetColumnsResponse(
            table_name=request.table_name,
            candidates=[TargetColumnCandidate(**c) for c in candidates],
            status="success",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Detection failed: {str(e)}")


@router.post("/get-column-values", response_model=GetColumnValuesResponse)
async def get_column_values(request: GetColumnValuesRequest):
    """
    Get distinct values and counts for a column.
    
    Used to show users the actual values in a column so they can
    select which ones represent the positive class.
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    table_list = db_session.get("table_list", [])
    if request.table_name not in table_list:
        raise HTTPException(status_code=404, detail=f"Table '{request.table_name}' not found.")

    engine = db_session["engine"]

    try:
        result = target_engineer.get_column_values(
            engine,
            request.table_name,
            request.column_name,
            schema=request.schema,
            limit=request.limit,
        )
        
        if result.get("status") == "error":
            raise HTTPException(status_code=500, detail=result.get("error", "Failed to get values"))
        
        return GetColumnValuesResponse(
            column_name=result["column_name"],
            table_name=result["table_name"],
            total_records=result["total_records"],
            distinct_count=result["distinct_count"],
            values=[ColumnValue(**v) for v in result["values"]],
            sampled=result.get("sampled", False),
            sample_size=result.get("sample_size"),
            sample_percent=result.get("sample_percent"),
            status="success",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get values: {str(e)}")


@router.post("/generate-target", response_model=GenerateTargetResponse)
async def generate_target(request: GenerateTargetRequest):
    """
    DEPRECATED: Legacy value-based target. Use /define-target.

    Generate target SQL from selected column values.
    
    User picks which values represent the positive class (1),
    and the system generates the CASE WHEN SQL logic.
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    table_list = db_session.get("table_list", [])
    if request.table_name not in table_list:
        raise HTTPException(status_code=404, detail=f"Table '{request.table_name}' not found.")

    if not request.selected_values:
        raise HTTPException(status_code=400, detail="No values selected for positive class")

    try:
        result = target_engineer.generate_target_from_values(
            column_name=request.column_name,
            selected_values=request.selected_values,
            target_name=request.target_name,
            grouping_column=request.grouping_column,
        )
        
        if result.get("error"):
            raise HTTPException(status_code=400, detail=result.get("error"))
        
        return GenerateTargetResponse(
            target_name=result["target_name"],
            sql_logic=result["sql_logic"],
            description=result["description"],
            column_name=result["column_name"],
            selected_values=result["selected_values"],
            status="success",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Target generation failed: {str(e)}")


@router.post("/preview-target", response_model=PreviewTargetResponse)
async def preview_target(request: PreviewTargetRequest):
    """
    DEPRECATED: Legacy value-based target preview. Use /target-distribution.

    Preview the distribution of a target variable before finalizing.

    Executes the target SQL logic and returns class distribution with warnings.
    """
    db_session = db_sessions.get(request.session_id)
    if db_session is None:
        raise HTTPException(status_code=404, detail="Database session not found")

    table_list = db_session.get("table_list", [])
    if request.table_name not in table_list:
        raise HTTPException(status_code=404, detail=f"Table '{request.table_name}' not found.")

    engine = db_session["engine"]

    try:
        result = target_engineer.preview_target_distribution(
            engine=engine,
            table_name=request.table_name,
            sql_logic=request.sql_logic,
            target_name=request.target_name,
            grouping_column=request.grouping_column,
        )
        
        if result.get("status") == "error":
            raise HTTPException(status_code=500, detail=result.get("error", "Preview failed"))
        
        return PreviewTargetResponse(
            target_name=result["target_name"],
            total_records=result["total_records"],
            distribution=[TargetDistribution(**d) for d in result["distribution"]],
            warnings=result["warnings"],
            is_usable=result["is_usable"],
            recommendation=result.get("recommendation"),
            status="success" if result["is_usable"] else "warning",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Preview failed: {str(e)}")


# ============================================================================
#  Schema Discovery Endpoints
# ============================================================================


class SchemaTablesRequest(BaseModel):
    """Request for listing tables."""
    session_id: str
    schema: str = "public"


class SchemaTableResponse(BaseModel):
    """Response table info."""
    schema_name: str
    table_name: str
    row_count: int
    column_count: int
    min_date: str | None = None
    max_date: str | None = None
    date_column: str | None = None
    has_entity_column: bool = False
    entity_columns: list[str] = []


class SchemaTablesResponse(BaseModel):
    """Response for listing tables."""
    tables: list[SchemaTableResponse]
    total_count: int
    status: str


@router.post("/schema/tables", response_model=SchemaTablesResponse)
async def get_schema_tables(request: SchemaTablesRequest):
    """
    Get all tables in schema with row counts and metadata.
    
     Foundation - Screen 1 data
    """
    db_session = db_sessions.get(request.session_id)
    if not db_session or "engine" not in db_session:
        raise HTTPException(status_code=404, detail="Session not found or no DB connected")
    
    engine = db_session["engine"]
    
    try:
        tables = schema_service.get_all_tables(engine, request.schema)
        
        return SchemaTablesResponse(
            tables=[
                SchemaTableResponse(
                    schema_name=t.schema_name,
                    table_name=t.table_name,
                    row_count=t.row_count,
                    column_count=t.column_count,
                    min_date=t.min_date,
                    max_date=t.max_date,
                    date_column=t.date_column,
                    has_entity_column=t.has_entity_column,
                    entity_columns=t.entity_columns,
                )
                for t in tables
            ],
            total_count=len(tables),
            status="success",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get tables: {str(e)}")


class EntityColumnsRequest(BaseModel):
    """Request for entity detection."""
    session_id: str
    schema: str = "public"


class EntityColumnResponse(BaseModel):
    """Response entity column info."""
    column_name: str
    tables: list[str]
    total_unique: int
    confidence: float


class EntityColumnsResponse(BaseModel):
    """Response for entity detection."""
    entities: list[EntityColumnResponse]
    total_count: int
    status: str


@router.post("/schema/entities", response_model=EntityColumnsResponse)
async def get_entity_columns(request: EntityColumnsRequest):
    """
    Detect potential entity ID columns across all tables.
    
     Foundation - Screen 1 entity selector
    """
    db_session = db_sessions.get(request.session_id)
    if not db_session or "engine" not in db_session:
        raise HTTPException(status_code=404, detail="Session not found or no DB connected")
    
    engine = db_session["engine"]
    
    try:
        entities = schema_service.detect_entity_columns(engine, request.schema)
        
        return EntityColumnsResponse(
            entities=[
                EntityColumnResponse(
                    column_name=e.column_name,
                    tables=e.tables,
                    total_unique=e.total_unique,
                    confidence=e.confidence,
                )
                for e in entities
            ],
            total_count=len(entities),
            status="success",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to detect entities: {str(e)}")


class ProfileTableRequest(BaseModel):
    """Request for table profiling."""
    session_id: str
    table_name: str
    schema: str = "public"


class ColumnProfileResponse(BaseModel):
    """Response column profile info."""
    name: str
    data_type: str
    is_nullable: bool
    null_count: int
    null_percent: float
    distinct_count: int
    min_value: str | None = None
    max_value: str | None = None


class TableProfileResponse(BaseModel):
    """Response for table profiling."""
    schema_name: str
    table_name: str
    row_count: int
    columns: list[ColumnProfileResponse]
    total_null_percent: float
    date_columns: list[str]
    id_columns: list[str]
    min_date: str | None = None
    max_date: str | None = None
    status: str


class HistogramRequest(BaseModel):
    """Request for numeric histogram."""
    session_id: str
    table_name: str
    column_name: str
    schema: str = "public"
    bins: int = 12
    sample_size: int = 100000


class HistogramBin(BaseModel):
    """Histogram bucket."""
    bucket: int
    count: int


class HistogramResponse(BaseModel):
    """Response for numeric histogram."""
    table_name: str
    column_name: str
    min: float | None
    max: float | None
    bins: int
    total_count: int
    sampled: bool
    sample_percent: float | None = None
    sample_size: int | None = None
    histogram: list[HistogramBin]
    status: str


@router.post("/schema/profile", response_model=TableProfileResponse)
async def profile_table(request: ProfileTableRequest):
    """
    Get detailed profile of a single table.
    
     Foundation - Table quality metrics
    """
    db_session = db_sessions.get(request.session_id)
    if not db_session or "engine" not in db_session:
        raise HTTPException(status_code=404, detail="Session not found or no DB connected")
    
    engine = db_session["engine"]
    
    try:
        profile = schema_service.profile_table(engine, request.table_name, request.schema)
        
        return TableProfileResponse(
            schema_name=profile.schema_name,
            table_name=profile.table_name,
            row_count=profile.row_count,
            columns=[
                ColumnProfileResponse(
                    name=c.name,
                    data_type=c.data_type,
                    is_nullable=c.is_nullable,
                    null_count=c.null_count,
                    null_percent=c.null_percent,
                    distinct_count=c.distinct_count,
                    min_value=c.min_value,
                    max_value=c.max_value,
                )
                for c in profile.columns
            ],
            total_null_percent=profile.total_null_percent,
            date_columns=profile.date_columns,
            id_columns=profile.id_columns,
            min_date=profile.min_date,
            max_date=profile.max_date,
            status="success",
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to profile table: {str(e)}")


@router.post("/schema/histogram", response_model=HistogramResponse)
async def get_histogram(request: HistogramRequest):
    """
    Get numeric histogram for a column.
    """
    db_session = db_sessions.get(request.session_id)
    if not db_session or "engine" not in db_session:
        raise HTTPException(status_code=404, detail="Session not found or no DB connected")

    engine = db_session["engine"]

    try:
        result = schema_service.get_numeric_histogram(
            engine,
            request.table_name,
            request.column_name,
            request.schema,
            request.bins,
            request.sample_size,
        )

        return HistogramResponse(
            table_name=result["table_name"],
            column_name=result["column_name"],
            min=result.get("min"),
            max=result.get("max"),
            bins=result["bins"],
            total_count=result["total_count"],
            sampled=result["sampled"],
            sample_percent=result.get("sample_percent"),
            sample_size=result.get("sample_size"),
            histogram=[HistogramBin(**b) for b in result["histogram"]],
            status="success",
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to build histogram: {str(e)}")


class EstimateCostRequest(BaseModel):
    """Request for cost estimation."""
    row_count: int
    feature_count: int
    window_sizes: list[int] = [30]


class EstimateCostResponse(BaseModel):
    """Response for cost estimation."""
    estimated_rows: int
    estimated_seconds: float
    estimated_memory_gb: float
    warning: str | None = None
    recommendation: str | None = None
    status: str


@router.post("/schema/estimate-cost", response_model=EstimateCostResponse)
async def estimate_cost(request: EstimateCostRequest):
    """
    Estimate computational cost for dataset generation.
    
     Foundation - Performance warnings
    """
    try:
        estimate = schema_service.estimate_cost(
            request.row_count,
            request.feature_count,
            request.window_sizes,
        )
        
        return EstimateCostResponse(
            estimated_rows=estimate.estimated_rows,
            estimated_seconds=estimate.estimated_seconds,
            estimated_memory_gb=estimate.estimated_memory_gb,
            warning=estimate.warning,
            recommendation=estimate.recommendation,
            status="success",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to estimate cost: {str(e)}")


# ============================================================================
#  Grain Endpoints
# ============================================================================


class GrainDefineRequest(BaseModel):
    """Request for defining grain."""
    session_id: str
    entity_type: str
    entity_table: str
    entity_id_column: str
    observation_date_column: str
    observation_date_type: str = "column"
    observation_date_value: str | None = None
    deduplication_rule: str = "keep_latest"
    dedup_order_by: str | None = None
    dedup_tiebreaker: str | None = None
    schema: str = "public"
    # Temporal split fields
    snapshot_strategy: str = "column"
    start_date: str | None = None
    end_date: str | None = None
    min_history_days: int = 30
    train_end_date: str | None = None
    valid_end_date: str | None = None
    include_split: bool = False


class GrainDefineResponse(BaseModel):
    """Response for grain definition."""
    grain_sql: str
    has_split_column: bool
    snapshot_strategy: str
    status: str
    grain_definition: dict[str, Any] | None = None
    stats: GrainStats | None = None
    warnings: list[str] = []
    errors: list[str] = []


@router.post("/grain/define", response_model=GrainDefineResponse)
async def define_grain_v2(request: GrainDefineRequest):
    """
    Define grain and generate SQL.
    
     Supports snapshot strategies and temporal splits.
    """
    db_session = db_sessions.get(request.session_id)
    if not db_session or "engine" not in db_session:
        raise HTTPException(status_code=404, detail="Session not found or no DB connected")
    
    engine = db_session["engine"]
    
    try:
        grain = GrainDefinition(
            entity_type=request.entity_type,
            entity_table=request.entity_table,
            entity_id_column=request.entity_id_column,
            observation_date_column=request.observation_date_column,
            observation_date_type=request.observation_date_type,
            observation_date_value=request.observation_date_value,
            deduplication_rule=request.deduplication_rule,
            dedup_order_by=request.dedup_order_by or request.observation_date_column,
            dedup_tiebreaker=request.dedup_tiebreaker,
            schema=request.schema,
            snapshot_strategy=request.snapshot_strategy,
            start_date=request.start_date,
            end_date=request.end_date,
            min_history_days=request.min_history_days,
            train_end_date=request.train_end_date,
            valid_end_date=request.valid_end_date,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    validation = GrainService.validate_grain(engine, grain)
    stats = None
    if validation.get("stats"):
        s = validation["stats"]
        stats = GrainStats(
            total_rows_estimate=s.get("total_rows_estimate", 0),
            total_rows_is_estimate=s.get("total_rows_is_estimate", True),
            unique_entities=s.get("unique_entities", 0),
            duplicate_entity_count=s.get("duplicate_entity_count", 0),
            duplicate_entity_obs_count=s.get("duplicate_entity_obs_count", 0),
            null_entity_count=s.get("null_entity_count", 0),
            null_obs_date_count=s.get("null_obs_date_count", 0),
            obs_date_min=s.get("obs_date_min"),
            obs_date_max=s.get("obs_date_max"),
            days_since_max_obs=s.get("days_since_max_obs"),
        )
    if validation["status"] == "invalid":
        return GrainDefineResponse(
            grain_sql="",
            has_split_column=False,
            snapshot_strategy=request.snapshot_strategy,
            status="invalid",
            grain_definition=validation["grain_definition"],
            stats=stats,
            warnings=validation.get("warnings", []),
            errors=validation.get("errors", []),
        )
    
    sql = GrainService.generate_grain_sql(grain, include_split=request.include_split)
    
    db_session["grain_definition"] = grain
    db_session["grain_sql"] = sql
    
    return GrainDefineResponse(
        grain_sql=sql,
        has_split_column=request.include_split and bool(request.train_end_date),
        snapshot_strategy=request.snapshot_strategy,
        status=validation["status"],
        grain_definition=validation["grain_definition"],
        stats=stats,
        warnings=validation.get("warnings", []),
        errors=validation.get("errors", []),
    )

class ValidateSplitRequest(BaseModel):
    """Request for validating temporal split."""
    train_end_date: str | None = None
    valid_end_date: str | None = None
    start_date: str | None = None
    end_date: str | None = None


class ValidateSplitResponse(BaseModel):
    """Response for split validation."""
    is_valid: bool
    warnings: list[str]
    status: str


@router.post("/grain/validate-split", response_model=ValidateSplitResponse)
async def validate_split(request: ValidateSplitRequest):
    """
    Validate temporal split configuration.
    
     Ensures train < valid < test dates.
    """
    try:
        warnings = GrainService.validate_temporal_split(
            train_end_date=request.train_end_date,
            valid_end_date=request.valid_end_date,
            start_date=request.start_date,
            end_date=request.end_date,
        )
        
        is_valid = not any("INVALID" in w for w in warnings)
        
        return ValidateSplitResponse(
            is_valid=is_valid,
            warnings=warnings,
            status="success" if is_valid else "warning",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to validate split: {str(e)}")


class GrainPreviewRequest(BaseModel):
    """Request for grain preview."""
    session_id: str
    entity_type: str
    entity_table: str
    entity_id_column: str
    observation_date_column: str
    observation_date_type: str = "column"
    observation_date_value: str | None = None
    deduplication_rule: str = "keep_latest"
    dedup_order_by: str | None = None
    dedup_tiebreaker: str | None = None
    schema: str = "public"
    snapshot_strategy: str = "column"
    start_date: str | None = None
    end_date: str | None = None
    min_history_days: int = 30
    train_end_date: str | None = None
    valid_end_date: str | None = None
    include_split: bool = False
    limit: int = 100


class GrainPreviewResponse(BaseModel):
    """Response for grain preview."""
    columns: list[str]
    rows: list[dict]
    row_count: int
    sql: str
    status: str


@router.post("/grain/preview", response_model=GrainPreviewResponse)
async def preview_grain_v2(request: GrainPreviewRequest):
    """
    Preview grain data.
    
     Supports snapshot strategies and temporal splits.
    """
    db_session = db_sessions.get(request.session_id)
    if not db_session or "engine" not in db_session:
        raise HTTPException(status_code=404, detail="Session not found or no DB connected")
    
    engine = db_session["engine"]
    
    try:
        grain = GrainDefinition(
            entity_type=request.entity_type,
            entity_table=request.entity_table,
            entity_id_column=request.entity_id_column,
            observation_date_column=request.observation_date_column,
            observation_date_type=request.observation_date_type,
            observation_date_value=request.observation_date_value,
            deduplication_rule=request.deduplication_rule,
            dedup_order_by=request.dedup_order_by or request.observation_date_column,
            dedup_tiebreaker=request.dedup_tiebreaker,
            schema=request.schema,
            snapshot_strategy=request.snapshot_strategy,
            start_date=request.start_date,
            end_date=request.end_date,
            min_history_days=request.min_history_days,
            train_end_date=request.train_end_date,
            valid_end_date=request.valid_end_date,
        )
        
        preview = GrainService.preview_grain(
            engine,
            grain,
            limit=request.limit,
            include_split=request.include_split,
        )
        
        return GrainPreviewResponse(
            columns=preview["columns"],
            rows=preview["rows"],
            row_count=preview["row_count"],
            sql=preview["sql"],
            status="success",
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to preview grain: {str(e)}")


# ============================================================================
#  Feature Generation Endpoints
# ============================================================================

from app.services.sql_validator import sql_validator, ValidationResult


class FeatureTemplateResponse(BaseModel):
    """Response with available feature templates."""
    templates: list[dict]
    count: int
    status: str


@router.get("/feature/templates", response_model=FeatureTemplateResponse)
async def get_feature_templates():
    """
    Get available feature templates.
    
     Returns 10 aggregation types.
    """
    templates = ObservationAwareFeatureService.list_templates()
    return FeatureTemplateResponse(
        templates=templates,
        count=len(templates),
        status="success",
    )


class FeatureGenerateRequest(BaseModel):
    """Request for feature SQL generation."""
    session_id: str
    # Feature definition
    feature_name: str
    feature_key: str
    template_type: str
    source_table: str
    join_column: str
    time_column: str
    value_column: str | None = None
    window_days: int = 30
    source_schema: str = "public"
    # Grain definition (simplified)
    entity_table: str
    entity_id_column: str
    observation_date_column: str
    grain_schema: str = "public"


class FeatureGenerateResponse(BaseModel):
    """Response with generated feature SQL."""
    sql: str
    feature_columns: list[str]
    max_source_time_column: str
    window_description: str
    status: str


@router.post("/feature/generate", response_model=FeatureGenerateResponse)
async def generate_feature_sql(request: FeatureGenerateRequest):
    """
    Generate observation-aware feature SQL.
    
     Supports 10 aggregation types with leakage prevention.
    """
    db_session = db_sessions.get(request.session_id)
    if not db_session or "engine" not in db_session:
        raise HTTPException(status_code=404, detail="Session not found or no DB connected")
    
    try:
        # Build grain definition
        grain = GrainDefinition(
            entity_type="entity",
            entity_table=request.entity_table,
            entity_id_column=request.entity_id_column,
            observation_date_column=request.observation_date_column,
            schema=request.grain_schema,
        )
        
        # Build feature definition
        feature = FeatureDefinition(
            name=request.feature_name,
            key=request.feature_key,
            template_type=FeatureTemplateType(request.template_type),
            source_table=request.source_table,
            join_column=request.join_column,
            time_column=request.time_column,
            value_column=request.value_column,
            window_days=request.window_days,
            source_schema=request.source_schema,
        )
        
        result = ObservationAwareFeatureService.generate_feature_sql(
            feature=feature,
            grain=grain,
            include_grain_cte=True,
        )
        
        return FeatureGenerateResponse(
            sql=result["sql"],
            feature_columns=result["feature_columns"],
            max_source_time_column=result["max_source_time_column"],
            window_description=result["window_description"],
            status="success",
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate feature SQL: {str(e)}")


class ValidateSQLRequest(BaseModel):
    """Request for SQL validation."""
    session_id: str
    sql: str
    limit: int = 1000


class ValidateSQLResponse(BaseModel):
    """Response with SQL validation result."""
    is_valid: bool
    sample_rows: list[dict] = []
    row_count: int = 0
    column_names: list[str] = []
    error_message: str | None = None
    error_type: str | None = None
    leakage_warnings: list[str] = []
    status: str


@router.post("/feature/validate-sample", response_model=ValidateSQLResponse)
async def validate_sql_sample(request: ValidateSQLRequest):
    """
    Validate SQL by running on a sample.
    
     Tests SQL on limited rows before full execution.
    """
    db_session = db_sessions.get(request.session_id)
    if not db_session or "engine" not in db_session:
        raise HTTPException(status_code=404, detail="Session not found or no DB connected")
    
    engine = db_session["engine"]
    
    try:
        # Run validation
        result = sql_validator.validate_sql_on_sample(
            engine=engine,
            sql=request.sql,
            limit=request.limit,
        )
        
        # Check for leakage
        leakage_warnings = sql_validator.check_leakage_prevention(request.sql)
        
        return ValidateSQLResponse(
            is_valid=result.is_valid,
            sample_rows=result.sample_rows,
            row_count=result.row_count,
            column_names=result.column_names,
            error_message=result.error_message,
            error_type=result.error_type,
            leakage_warnings=leakage_warnings,
            status="success" if result.is_valid else "error",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")


# ============================================================================
#  Join Endpoints
# ============================================================================

from app.services.join_service import join_service, JoinDefinition, JoinKey


class JoinDefineRequest(BaseModel):
    """Request for defining a join."""
    session_id: str
    left_table: str
    right_table: str
    join_keys: list[dict]  # [{"left_column": "x", "right_column": "y"}]
    join_type: str = "left"
    left_schema: str = "public"
    right_schema: str = "public"


class JoinDefineResponse(BaseModel):
    """Response for join definition."""
    is_valid: bool
    errors: list[str]
    warnings: list[str]
    join_definition: dict | None = None
    join_sql: str | None = None
    status: str


@router.post("/join/define", response_model=JoinDefineResponse)
async def define_join(request: JoinDefineRequest):
    """
    Define and validate a table join.
    
     Checks for Cartesian products and row explosion.
    """
    db_session = db_sessions.get(request.session_id)
    if not db_session or "engine" not in db_session:
        raise HTTPException(status_code=404, detail="Session not found or no DB connected")
    
    engine = db_session["engine"]
    
    try:
        join_keys = [(k["left_column"], k["right_column"]) for k in request.join_keys]
        
        result = join_service.define_join(
            engine=engine,
            left_table=request.left_table,
            right_table=request.right_table,
            join_keys=join_keys,
            join_type=request.join_type,
            left_schema=request.left_schema,
            right_schema=request.right_schema,
        )
        
        return JoinDefineResponse(
            is_valid=result["is_valid"],
            errors=result["errors"],
            warnings=result["warnings"],
            join_definition=result.get("join_definition"),
            join_sql=result.get("join_sql"),
            status="success" if result["is_valid"] else "error",
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to define join: {str(e)}")


class JoinPreviewRequest(BaseModel):
    """Request for previewing a join."""
    session_id: str
    left_table: str
    right_table: str
    join_keys: list[dict]
    join_type: str = "left"
    left_schema: str = "public"
    right_schema: str = "public"
    limit: int = 100


class JoinPreviewResponse(BaseModel):
    """Response for join preview."""
    columns: list[str]
    rows: list[dict]
    row_count: int
    left_table_count: int = 0
    right_table_count: int = 0
    sql: str | None = None
    error: str | None = None
    status: str


@router.post("/join/preview", response_model=JoinPreviewResponse)
async def preview_join(request: JoinPreviewRequest):
    """
    Preview join results with sample data.
    
     Shows sample joined rows.
    """
    db_session = db_sessions.get(request.session_id)
    if not db_session or "engine" not in db_session:
        raise HTTPException(status_code=404, detail="Session not found or no DB connected")
    
    engine = db_session["engine"]
    
    try:
        keys = [JoinKey(left_column=k["left_column"], right_column=k["right_column"]) 
                for k in request.join_keys]
        
        join_def = JoinDefinition(
            left_table=request.left_table,
            left_schema=request.left_schema,
            right_table=request.right_table,
            right_schema=request.right_schema,
            join_keys=keys,
            join_type=request.join_type,
        )
        
        result = join_service.preview_join(engine, join_def, limit=request.limit)
        
        return JoinPreviewResponse(
            columns=result["columns"],
            rows=result["rows"],
            row_count=result["row_count"],
            left_table_count=result.get("left_table_count", 0),
            right_table_count=result.get("right_table_count", 0),
            sql=result.get("sql"),
            error=result.get("error"),
            status=result["status"],
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to preview join: {str(e)}")


class CrossTableFeatureRequest(BaseModel):
    """Request for cross-table feature expression."""
    numerator_col: str
    denominator_col: str
    operation: str = "ratio"  # "ratio" or "difference"
    feature_name: str


class CrossTableFeatureResponse(BaseModel):
    """Response with cross-table feature SQL."""
    sql_expression: str
    status: str


@router.post("/join/cross-table-feature", response_model=CrossTableFeatureResponse)
async def generate_cross_table_feature(request: CrossTableFeatureRequest):
    """
    Generate SQL for cross-table feature (ratio/difference).
    
     Creates features like amount_sum/credit_limit.
    """
    try:
        sql_expr = join_service.generate_cross_table_feature(
            numerator_col=request.numerator_col,
            denominator_col=request.denominator_col,
            operation=request.operation,
            feature_name=request.feature_name,
        )
        
        return CrossTableFeatureResponse(
            sql_expression=sql_expr,
            status="success",
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate feature: {str(e)}")


# ============================================================================
#  Quality Check Endpoints
# ============================================================================

from app.services.quality_checks import quality_checker, MISSING_STRATEGIES


class EdaRequest(BaseModel):
    """Request for feature EDA."""
    session_id: str
    sql: str
    feature_columns: list[str]
    target_column: str | None = None
    sample_limit: int = 100000


class EdaResponse(BaseModel):
    """Response with EDA results."""
    feature_stats: list[dict]
    high_correlation_features: list
    warnings: list[str]
    feature_count: int
    status: str


@router.post("/quality/eda", response_model=EdaResponse)
async def run_feature_eda(request: EdaRequest):
    """
    Run exploratory data analysis on features.
    
     NULL rates, distinct counts, correlations.
    """
    db_session = db_sessions.get(request.session_id)
    if not db_session or "engine" not in db_session:
        raise HTTPException(status_code=404, detail="Session not found or no DB connected")
    
    engine = db_session["engine"]
    
    try:
        result = quality_checker.run_feature_eda(
            engine=engine,
            sql=request.sql,
            feature_columns=request.feature_columns,
            target_column=request.target_column,
            sample_limit=request.sample_limit,
        )
        
        return EdaResponse(
            feature_stats=result["feature_stats"],
            high_correlation_features=result["high_correlation_features"],
            warnings=result["warnings"],
            feature_count=result["feature_count"],
            status=result["status"],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"EDA failed: {str(e)}")


class LeakageScanRequest(BaseModel):
    """Request for leakage scanning."""
    session_id: str
    sql: str
    feature_columns: list[str]
    target_column: str
    correlation_threshold: float = 0.9


class LeakageScanResponse(BaseModel):
    """Response with leakage scan results."""
    suspicious_features: list[dict]
    total_checked: int
    leakage_detected: bool
    threshold: float
    status: str


@router.post("/quality/leakage-scan", response_model=LeakageScanResponse)
async def scan_for_leakage(request: LeakageScanRequest):
    """
    Scan features for potential data leakage.
    
     Detects features highly correlated with target.
    """
    db_session = db_sessions.get(request.session_id)
    if not db_session or "engine" not in db_session:
        raise HTTPException(status_code=404, detail="Session not found or no DB connected")
    
    engine = db_session["engine"]
    
    try:
        result = quality_checker.scan_for_leakage(
            engine=engine,
            sql=request.sql,
            feature_columns=request.feature_columns,
            target_column=request.target_column,
            correlation_threshold=request.correlation_threshold,
        )
        
        return LeakageScanResponse(
            suspicious_features=result["suspicious_features"],
            total_checked=result["total_checked"],
            leakage_detected=result["leakage_detected"],
            threshold=result["threshold"],
            status=result["status"],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Leakage scan failed: {str(e)}")


class ImputationRequest(BaseModel):
    """Request for imputation SQL."""
    column: str
    strategy: str
    add_indicator: bool = True


class ImputationResponse(BaseModel):
    """Response with imputation SQL."""
    imputed_expr: str
    indicator_expr: str | None = None
    status: str


@router.post("/quality/imputation-sql", response_model=ImputationResponse)
async def get_imputation_sql(request: ImputationRequest):
    """
    Generate SQL for missing value imputation.
    
     ZERO, MEAN, MEDIAN, MODE, UNKNOWN strategies.
    """
    try:
        result = quality_checker.generate_imputation_sql(
            column=request.column,
            strategy=request.strategy,
            add_indicator=request.add_indicator,
        )
        
        return ImputationResponse(
            imputed_expr=result["imputed_expr"],
            indicator_expr=result.get("indicator_expr"),
            status="success",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Imputation generation failed: {str(e)}")


class MissingStrategiesResponse(BaseModel):
    """Response with available missing strategies."""
    strategies: dict[str, list[str]]
    status: str


@router.get("/quality/missing-strategies", response_model=MissingStrategiesResponse)
async def get_missing_strategies():
    """
    Get available missing value strategies.
    
     By column type (numeric, categorical, boolean).
    """
    return MissingStrategiesResponse(
        strategies=MISSING_STRATEGIES,
        status="success",
    )


# ============================================================================
#  Export + Validation Endpoints
# ============================================================================

from app.services.dataset_validator import dataset_validator
from app.services.export_service import export_service


class ValidateDatasetRequest(BaseModel):
    """Request for dataset validation."""
    session_id: str
    sql: str
    expected_columns: list[str]
    entity_column: str = "entity_id"
    observation_column: str = "observation_date"
    target_column: str | None = None
    train_end: str | None = None
    valid_end: str | None = None


class ValidateDatasetResponse(BaseModel):
    """Response with validation results."""
    is_valid: bool
    checks: list[dict]
    passed_count: int
    failed_count: int
    total_checks: int
    status: str


@router.post("/export/validate", response_model=ValidateDatasetResponse)
async def validate_dataset(request: ValidateDatasetRequest):
    """
    DEPRECATED: Use /validate-dataset-sql for DB wizard flow.

    Validate dataset before export.
    
     8 pre-export checks.
    """
    db_session = db_sessions.get(request.session_id)
    if not db_session or "engine" not in db_session:
        raise HTTPException(status_code=404, detail="Session not found or no DB connected")
    
    engine = db_session["engine"]
    
    try:
        result = dataset_validator.validate(
            engine=engine,
            sql=request.sql,
            expected_columns=request.expected_columns,
            entity_column=request.entity_column,
            observation_column=request.observation_column,
            target_column=request.target_column,
            train_end=request.train_end,
            valid_end=request.valid_end,
        )
        
        return ValidateDatasetResponse(
            is_valid=result["is_valid"],
            checks=result["checks"],
            passed_count=result["passed_count"],
            failed_count=result["failed_count"],
            total_checks=result["total_checks"],
            status=result["status"],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")


class ExportDatasetRequest(BaseModel):
    """Request for dataset export."""
    session_id: str
    export_format: str = "csv"
    row_limit: int | None = None
    include_metadata: bool = True


class ExportDatasetResponse(BaseModel):
    """Response with export results."""
    status: str
    file_path: str | None = None
    metadata_path: str | None = None
    row_count: int = 0
    error: str | None = None


@router.post("/export/dataset", response_model=ExportDatasetResponse)
async def export_dataset(request: ExportDatasetRequest):
    """
    DEPRECATED: Use /export-dataset for DB wizard flow.

    Export dataset to file.
    
     CSV export with metadata.
    """
    db_session = db_sessions.get(request.session_id)
    if not db_session or "engine" not in db_session:
        raise HTTPException(status_code=404, detail="Session not found or no DB connected")
    
    # Check for dataset_sql in session
    session_data = sessions.get(request.session_id, {})
    dataset_sql = session_data.get("dataset_sql")
    
    if not dataset_sql:
        raise HTTPException(
            status_code=400, 
            detail="No dataset SQL found in session. Build dataset first."
        )
    
    engine = db_session["engine"]
    
    try:
        result = export_service.export_dataset(
            engine=engine,
            dataset_sql=dataset_sql,
            session_id=request.session_id,
            session=session_data,
            export_format=request.export_format,
            row_limit=request.row_limit,
            include_metadata=request.include_metadata,
        )
        
        return ExportDatasetResponse(
            status=result.status,
            file_path=result.file_path,
            metadata_path=result.metadata_path,
            row_count=result.row_count,
            error=result.error,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")
