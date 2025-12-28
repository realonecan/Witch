import { useEffect, useMemo, useState } from 'react';
import api from '../../api/client';

const ENTITY_TYPES = ['entity', 'customer', 'account', 'transaction', 'loan'];
const SNAPSHOT_STRATEGIES = [
    { value: 'column', label: 'Use observation column' },
    { value: 'daily', label: 'Daily' },
    { value: 'weekly', label: 'Weekly' },
    { value: 'monthly', label: 'Monthly' },
];

const DEDUP_RULES = [
    { value: 'keep_latest', label: 'Keep latest' },
    { value: 'keep_first', label: 'Keep first' },
    { value: 'keep_all', label: 'Keep all' },
    { value: 'error', label: 'Error on duplicates' },
];

const isDateLike = (type = '') => {
    const lower = type.toLowerCase();
    return lower.includes('date') || lower.includes('time');
};

export default function GrainDefiner({
    sessionId,
    selectedEntity,
    selectedTables,
    onGrainDefine,
    onBack
}) {
    const [entityType, setEntityType] = useState('entity');
    const [entityTable, setEntityTable] = useState('');
    const [columns, setColumns] = useState([]);
    const [observationColumn, setObservationColumn] = useState('');
    const [snapshotStrategy, setSnapshotStrategy] = useState('column');
    const [startDate, setStartDate] = useState('');
    const [endDate, setEndDate] = useState('');
    const [minHistory, setMinHistory] = useState(30);
    const [dedupRule, setDedupRule] = useState('keep_latest');
    const [includeSplit, setIncludeSplit] = useState(true);
    const [trainEnd, setTrainEnd] = useState('');
    const [validEnd, setValidEnd] = useState('');
    const [splitWarnings, setSplitWarnings] = useState([]);
    const [preview, setPreview] = useState(null);
    const [defineResult, setDefineResult] = useState(null);
    const [loadingPreview, setLoadingPreview] = useState(false);
    const [saving, setSaving] = useState(false);
    const [error, setError] = useState(null);

    const entityTables = useMemo(() => {
        if (selectedEntity?.tables?.length) {
            return selectedEntity.tables;
        }
        return (selectedTables || []).map((t) => t.name);
    }, [selectedEntity, selectedTables]);

    useEffect(() => {
        if (!entityTable && entityTables.length) {
            setEntityTable(entityTables[0]);
        }
    }, [entityTable, entityTables]);

    useEffect(() => {
        if (entityTable) {
            loadColumns();
        }
    }, [entityTable]);

    useEffect(() => {
        if (!includeSplit) {
            setSplitWarnings([]);
            return;
        }
        validateSplit();
    }, [includeSplit, trainEnd, validEnd, startDate, endDate]);

    const loadColumns = async () => {
        setError(null);
        try {
            const res = await api.post('/table-columns', {
                session_id: sessionId,
                table_name: entityTable,
            });
            const cols = res.data.columns || [];
            setColumns(cols);

            const dateCols = cols.filter((col) => isDateLike(col.type));
            if (!observationColumn && dateCols.length) {
                setObservationColumn(dateCols[0].name);
            }
        } catch (err) {
            setError(err.response?.data?.detail || 'Failed to load table columns');
        }
    };

    const validateSplit = async () => {
        try {
            const res = await api.post('/grain/validate-split', {
                train_end_date: trainEnd || null,
                valid_end_date: validEnd || null,
                start_date: startDate || null,
                end_date: endDate || null,
            });
            setSplitWarnings(res.data.warnings || []);
        } catch (err) {
            setSplitWarnings(['Failed to validate split']);
        }
    };

    const requiresDateRange = snapshotStrategy !== 'column';

    const buildRequest = () => ({
        session_id: sessionId,
        entity_type: entityType,
        entity_table: entityTable,
        entity_id_column: selectedEntity?.column_name,
        observation_date_column: observationColumn,
        observation_date_type: 'column',
        deduplication_rule: dedupRule,
        snapshot_strategy: snapshotStrategy,
        start_date: requiresDateRange ? startDate || null : null,
        end_date: requiresDateRange ? endDate || null : null,
        min_history_days: minHistory,
        train_end_date: includeSplit ? trainEnd || null : null,
        valid_end_date: includeSplit ? validEnd || null : null,
        include_split: includeSplit,
    });

    const canSubmit = () => {
        if (!selectedEntity?.column_name || !entityTable || !observationColumn) {
            return false
        }
        if (requiresDateRange && (!startDate || !endDate)) {
            return false
        }
        if (includeSplit && (!trainEnd || !validEnd)) {
            return false
        }
        return true
    };

    const handlePreview = async () => {
        if (!canSubmit()) {
            setError('Fill in required fields before previewing.');
            return;
        }
        setLoadingPreview(true);
        setError(null);
        try {
            const res = await api.post('/grain/preview', {
                ...buildRequest(),
                limit: 100,
            });
            setPreview(res.data);
        } catch (err) {
            setError(err.response?.data?.detail || 'Failed to preview grain');
        } finally {
            setLoadingPreview(false);
        }
    };

    const handleContinue = async () => {
        if (!canSubmit()) {
            setError('Fill in required fields before continuing.');
            return;
        }
        setSaving(true);
        setError(null);
        try {
            const res = await api.post('/grain/define', buildRequest());
            setDefineResult(res.data);

            if (res.data.status === 'invalid') {
                return;
            }

            onGrainDefine({
                definition: res.data.grain_definition,
                stats: res.data.stats,
                warnings: res.data.warnings,
                errors: res.data.errors,
                grain_sql: res.data.grain_sql,
                snapshot_strategy: res.data.snapshot_strategy,
                has_split_column: res.data.has_split_column,
                config: buildRequest(),
            });
        } catch (err) {
            setError(err.response?.data?.detail || 'Failed to define grain');
        } finally {
            setSaving(false);
        }
    };

    const canContinue = canSubmit() && !saving;

    return (
        <div className="p-6 space-y-6">
            {/* Header */}
            <div className="terminal-header">
                STEP 3 OF 7: DEFINE GRAIN
            </div>

            {/* Entity Table */}
            <div className="terminal-panel p-4">
                <div className="grid grid-cols-3 gap-4">
                    <div>
                        <label className="data-label block mb-1">Entity Type</label>
                        <select
                            value={entityType}
                            onChange={(e) => setEntityType(e.target.value)}
                            className="input-terminal w-full"
                        >
                            {ENTITY_TYPES.map((type) => (
                                <option key={type} value={type}>{type}</option>
                            ))}
                        </select>
                    </div>
                    <div className="col-span-2">
                        <label className="data-label block mb-1">Entity Table</label>
                        <select
                            value={entityTable}
                            onChange={(e) => setEntityTable(e.target.value)}
                            className="input-terminal w-full"
                        >
                            <option value="">Select table...</option>
                            {entityTables.map((table) => (
                                <option key={table} value={table}>{table}</option>
                            ))}
                        </select>
                    </div>
                </div>
                <div className="mt-3 text-xs text-[var(--color-terminal-text-dim)]">
                    Entity ID column: {selectedEntity?.column_name || 'Not selected'}
                </div>
            </div>

            {/* Observation Column */}
            <div className="terminal-panel p-4">
                <div className="grid grid-cols-2 gap-4">
                    <div>
                        <label className="data-label block mb-1">Observation Date Column</label>
                        <select
                            value={observationColumn}
                            onChange={(e) => setObservationColumn(e.target.value)}
                            className="input-terminal w-full"
                        >
                            <option value="">Select column...</option>
                            {columns.map((col) => (
                                <option key={col.name} value={col.name}>
                                    {col.name} {isDateLike(col.type) ? '' : ''}
                                </option>
                            ))}
                        </select>
                    </div>
                    <div>
                        <label className="data-label block mb-1">Dedup Rule</label>
                        <select
                            value={dedupRule}
                            onChange={(e) => setDedupRule(e.target.value)}
                            className="input-terminal w-full"
                        >
                            {DEDUP_RULES.map((rule) => (
                                <option key={rule.value} value={rule.value}>{rule.label}</option>
                            ))}
                        </select>
                    </div>
                </div>
            </div>

            {/* Snapshot Strategy */}
            <div className="terminal-panel p-4">
                <div className="data-label mb-3">SNAPSHOT STRATEGY</div>
                <div className="flex flex-wrap gap-4">
                    {SNAPSHOT_STRATEGIES.map((strategy) => (
                        <label key={strategy.value} className="flex items-center gap-2 cursor-pointer">
                            <input
                                type="radio"
                                name="snapshot_strategy"
                                value={strategy.value}
                                checked={snapshotStrategy === strategy.value}
                                onChange={() => setSnapshotStrategy(strategy.value)}
                                className="accent-[var(--color-terminal-orange)]"
                            />
                            <span className={snapshotStrategy === strategy.value ? 'data-value-highlight' : 'data-value'}>
                                {strategy.label}
                            </span>
                        </label>
                    ))}
                </div>
                <div className="mt-2 text-xs text-[var(--color-terminal-text-dim)]">
                    Monthly/weekly/daily snapshots require a date range.
                </div>
            </div>

            {/* Date Range */}
            {requiresDateRange && (
                <div className="terminal-panel p-4">
                    <div className="data-label mb-3">DATE RANGE</div>
                    <div className="grid grid-cols-2 gap-4">
                        <div>
                            <label className="text-xs text-[var(--color-terminal-text-dim)] mb-1 block">Start Date</label>
                            <input
                                type="date"
                                value={startDate}
                                onChange={(e) => setStartDate(e.target.value)}
                                className="input-terminal w-full"
                            />
                        </div>
                        <div>
                            <label className="text-xs text-[var(--color-terminal-text-dim)] mb-1 block">End Date</label>
                            <input
                                type="date"
                                value={endDate}
                                onChange={(e) => setEndDate(e.target.value)}
                                className="input-terminal w-full"
                            />
                        </div>
                    </div>
                </div>
            )}

            {/* Minimum History */}
            <div className="terminal-panel p-4">
                <div className="flex items-center gap-4">
                    <span className="data-label">MINIMUM HISTORY:</span>
                    <input
                        type="number"
                        value={minHistory}
                        onChange={(e) => setMinHistory(Number(e.target.value))}
                        className="input-terminal w-20 text-center"
                        min={0}
                    />
                    <span className="text-[var(--color-terminal-text-dim)]">days</span>
                    <span className="text-[var(--color-terminal-text-dim)] text-xs ml-4">
                        ?"~ Skip entities with less than {minHistory} days data
                    </span>
                </div>
            </div>

            {/* Train/Valid/Test Split */}
            <div className="terminal-panel p-4">
                <div className="flex items-center gap-3 mb-3">
                    <input
                        type="checkbox"
                        checked={includeSplit}
                        onChange={(e) => setIncludeSplit(e.target.checked)}
                        className="accent-[var(--color-terminal-orange)]"
                    />
                    <span className="data-label">INCLUDE TRAIN/VALID/TEST SPLIT</span>
                </div>

                {includeSplit && (
                    <div className="grid grid-cols-2 gap-4">
                        <div>
                            <label className="text-xs text-[var(--color-terminal-text-dim)] mb-1 block">Train End Date</label>
                            <input
                                type="date"
                                value={trainEnd}
                                onChange={(e) => setTrainEnd(e.target.value)}
                                className="input-terminal w-full"
                            />
                        </div>
                        <div>
                            <label className="text-xs text-[var(--color-terminal-text-dim)] mb-1 block">Validation End Date</label>
                            <input
                                type="date"
                                value={validEnd}
                                onChange={(e) => setValidEnd(e.target.value)}
                                className="input-terminal w-full"
                            />
                        </div>
                    </div>
                )}

                {splitWarnings.length > 0 && includeSplit && (
                    <div className="mt-3 space-y-1">
                        {splitWarnings.map((warning, idx) => (
                            <div key={idx} className="text-[var(--color-terminal-yellow)] text-xs">
                                ?s,? {warning}
                            </div>
                        ))}
                    </div>
                )}
            </div>

            {/* Validation Results */}
            {defineResult && (defineResult.warnings?.length || defineResult.errors?.length) && (
                <div className="terminal-panel p-4">
                    <div className="terminal-header">GRAIN VALIDATION</div>
                    {defineResult.errors?.length > 0 && (
                        <div className="text-[var(--color-terminal-red)] text-sm space-y-1">
                            {defineResult.errors.map((err, idx) => (
                                <div key={idx}>?s,? {err}</div>
                            ))}
                        </div>
                    )}
                    {defineResult.warnings?.length > 0 && (
                        <div className="text-[var(--color-terminal-yellow)] text-sm space-y-1 mt-2">
                            {defineResult.warnings.map((warn, idx) => (
                                <div key={idx}>?s,? {warn}</div>
                            ))}
                        </div>
                    )}
                </div>
            )}

            {defineResult?.stats && (
                <div className="terminal-panel p-4">
                    <div className="terminal-header">GRAIN STATS</div>
                    <div className="grid grid-cols-2 gap-4 text-sm">
                        <div>Rows: {defineResult.stats.total_rows_estimate?.toLocaleString()}</div>
                        <div>Unique Entities: {defineResult.stats.unique_entities?.toLocaleString()}</div>
                        <div>Duplicates: {defineResult.stats.duplicate_entity_count?.toLocaleString()}</div>
                        <div>Null IDs: {defineResult.stats.null_entity_count?.toLocaleString()}</div>
                    </div>
                </div>
            )}

            {/* Preview */}
            <div className="terminal-panel p-4">
                <div className="flex items-center justify-between">
                    <div className="data-label">PREVIEW GRAIN</div>
                    <button
                        onClick={handlePreview}
                        disabled={loadingPreview}
                        className="btn-terminal text-xs"
                    >
                        {loadingPreview ? 'LOADING...' : 'PREVIEW'}
                    </button>
                </div>
                {preview && (
                    <div className="mt-4">
                        <div className="text-xs text-[var(--color-terminal-text-dim)] mb-2">
                            {preview.row_count} rows (sample)
                        </div>
                        <div className="overflow-x-auto">
                            <table className="terminal-table">
                                <thead>
                                    <tr>
                                        {preview.columns.map((col) => (
                                            <th key={col}>{col}</th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody>
                                    {preview.rows.map((row, idx) => (
                                        <tr key={idx}>
                                            {preview.columns.map((col) => (
                                                <td key={col}>{row[col]}</td>
                                            ))}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </div>
                )}
            </div>

            {error && (
                <div className="terminal-panel p-4 border-[var(--color-terminal-red)] bg-[#1a0a0a] text-[var(--color-terminal-red)] text-sm">
                    ?s,? {error}
                </div>
            )}

            {/* Navigation */}
            <div className="flex justify-between">
                <button onClick={onBack} className="btn-terminal">
                    ?+? BACK
                </button>
                <button
                    onClick={handleContinue}
                    disabled={!canContinue}
                    className={`btn-terminal-primary px-6 ${!canContinue ? 'opacity-50 cursor-not-allowed' : ''}`}
                >
                    {saving ? 'SAVING...' : 'CONTINUE'} ?+'
                </button>
            </div>
        </div>
    );
}
