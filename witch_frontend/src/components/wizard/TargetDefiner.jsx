import { useEffect, useMemo, useState } from 'react';
import api from '../../api/client';

const isDateLike = (type = '') => {
    const lower = type.toLowerCase();
    return lower.includes('date') || lower.includes('time');
};

export default function TargetDefiner({
    sessionId,
    selectedTables,
    grainDefinition,
    onTargetDefine,
    onBack
}) {
    const [labelTable, setLabelTable] = useState('');
    const [columns, setColumns] = useState([]);
    const [joinColumn, setJoinColumn] = useState('');
    const [eventColumn, setEventColumn] = useState('');
    const [eventTimeColumn, setEventTimeColumn] = useState('');
    const [columnValues, setColumnValues] = useState([]);
    const [positiveValues, setPositiveValues] = useState([]);
    const [windowMonths, setWindowMonths] = useState(12);
    const [maturityMonths, setMaturityMonths] = useState(0);
    const [targetName, setTargetName] = useState('');
    const [extractionDate, setExtractionDate] = useState('');
    const [defineResult, setDefineResult] = useState(null);
    const [distribution, setDistribution] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    const tableOptions = useMemo(() => (selectedTables || []).map((t) => t.name), [selectedTables]);

    useEffect(() => {
        if (!labelTable && tableOptions.length) {
            setLabelTable(tableOptions[0]);
        }
    }, [labelTable, tableOptions]);

    useEffect(() => {
        if (labelTable) {
            loadColumns();
        }
    }, [labelTable]);

    useEffect(() => {
        if (eventColumn) {
            loadColumnValues();
        } else {
            setColumnValues([]);
            setPositiveValues([]);
        }
    }, [eventColumn]);

    const loadColumns = async () => {
        setError(null);
        try {
            const res = await api.post('/table-columns', {
                session_id: sessionId,
                table_name: labelTable,
            });
            const cols = res.data.columns || [];
            setColumns(cols);

            const entityColumn = grainDefinition?.entity_id_column;
            if (entityColumn && cols.some((col) => col.name === entityColumn)) {
                setJoinColumn(entityColumn);
            } else if (cols.length) {
                setJoinColumn(cols[0].name);
            }

            const dateCol = cols.find((col) => isDateLike(col.type));
            if (dateCol) {
                setEventTimeColumn(dateCol.name);
            } else if (cols.length) {
                setEventTimeColumn(cols[0].name);
            }
        } catch (err) {
            setError(err.response?.data?.detail || 'Failed to load columns');
        }
    };

    const loadColumnValues = async () => {
        setError(null);
        try {
            const res = await api.post('/get-column-values', {
                session_id: sessionId,
                table_name: labelTable,
                column_name: eventColumn,
                limit: 30,
            });
            setColumnValues(res.data.values || []);
        } catch (err) {
            setError(err.response?.data?.detail || 'Failed to load values');
        }
    };

    const toggleValue = (value) => {
        setPositiveValues(prev =>
            prev.includes(value)
                ? prev.filter(v => v !== value)
                : [...prev, value]
        );
    };

    const canDefine = () => {
        if (!grainDefinition) return false;
        if (!labelTable || !joinColumn || !eventColumn || !eventTimeColumn) return false;
        if (!positiveValues.length) return false;
        return true;
    };

    const handleDefine = async () => {
        if (!canDefine()) {
            setError('Fill in required fields before defining the target.');
            return;
        }
        setLoading(true);
        setError(null);
        try {
            const res = await api.post('/define-target', {
                session_id: sessionId,
                label_table: labelTable,
                label_join_column: joinColumn,
                label_event_column: eventColumn,
                label_event_time_column: eventTimeColumn,
                positive_values: positiveValues,
                window_type: 'fixed',
                window_months: Number(windowMonths) || 12,
                maturity_months: Number(maturityMonths) || 0,
                extraction_date: extractionDate || null,
                target_name: targetName || null,
                schema: 'public',
            });

            setDefineResult(res.data);

            if (res.data.status === 'invalid') {
                return;
            }

            const distRes = await api.post('/target-distribution', { session_id: sessionId });
            setDistribution(distRes.data);

        } catch (err) {
            setError(err.response?.data?.detail || 'Failed to define target');
        } finally {
            setLoading(false);
        }
    };


    const handleContinue = () => {
        if (!defineResult || defineResult.status == 'invalid' || !distribution) {
            setError('Define the target and load distribution before continuing.');
            return;
        }
        onTargetDefine({
            definition: defineResult.target_definition,
            stats: defineResult.stats,
            warnings: defineResult.warnings,
            distribution,
            config: {
                label_table: labelTable,
                label_join_column: joinColumn,
                label_event_column: eventColumn,
                label_event_time_column: eventTimeColumn,
                positive_values: positiveValues,
                window_months: Number(windowMonths) || 12,
                maturity_months: Number(maturityMonths) || 0,
                target_name: targetName || null,
                extraction_date: extractionDate || null,
            },
        });
    };

    const canContinue = !loading && defineResult?.status !== 'invalid' && Boolean(distribution);

    return (
        <div className="p-6 space-y-6">
            {/* Header */}
            <div className="terminal-header">
                STEP 4 OF 7: DEFINE TARGET
            </div>

            {!grainDefinition && (
                <div className="terminal-panel p-4 border-[var(--color-terminal-red)] bg-[#1a0a0a] text-[var(--color-terminal-red)] text-sm">
                    Grain definition is missing. Go back and define grain first.
                </div>
            )}

            {/* Target Table & Columns */}
            <div className="terminal-panel p-4 space-y-4">
                <div className="grid grid-cols-2 gap-4">
                    <div>
                        <label className="data-label block mb-1">Label Table</label>
                        <select
                            value={labelTable}
                            onChange={(e) => setLabelTable(e.target.value)}
                            className="input-terminal w-full"
                        >
                            <option value="">Select table...</option>
                            {tableOptions.map((table) => (
                                <option key={table} value={table}>{table}</option>
                            ))}
                        </select>
                    </div>
                    <div>
                        <label className="data-label block mb-1">Join Column</label>
                        <select
                            value={joinColumn}
                            onChange={(e) => setJoinColumn(e.target.value)}
                            className="input-terminal w-full"
                            disabled={!columns.length}
                        >
                            <option value="">Select column...</option>
                            {columns.map((col) => (
                                <option key={col.name} value={col.name}>{col.name}</option>
                            ))}
                        </select>
                    </div>
                </div>

                <div className="grid grid-cols-2 gap-4">
                    <div>
                        <label className="data-label block mb-1">Event Column</label>
                        <select
                            value={eventColumn}
                            onChange={(e) => setEventColumn(e.target.value)}
                            className="input-terminal w-full"
                            disabled={!columns.length}
                        >
                            <option value="">Select column...</option>
                            {columns.map((col) => (
                                <option key={col.name} value={col.name}>{col.name}</option>
                            ))}
                        </select>
                    </div>
                    <div>
                        <label className="data-label block mb-1">Event Time Column</label>
                        <select
                            value={eventTimeColumn}
                            onChange={(e) => setEventTimeColumn(e.target.value)}
                            className="input-terminal w-full"
                            disabled={!columns.length}
                        >
                            <option value="">Select column...</option>
                            {columns.map((col) => (
                                <option key={col.name} value={col.name}>
                                    {col.name} {isDateLike(col.type) ? '(date)' : ''}
                                </option>
                            ))}
                        </select>
                    </div>
                </div>
            </div>

            {/* Positive Values */}
            <div className="terminal-panel">
                <div className="terminal-header">POSITIVE CLASS VALUES</div>
                <div className="p-4">
                    {columnValues.length === 0 ? (
                        <div className="text-[var(--color-terminal-text-dim)] text-sm">
                            Select an event column to load values.
                        </div>
                    ) : (
                        <div className="space-y-2 max-h-64 overflow-y-auto">
                            {columnValues.map((val) => (
                                <label
                                    key={val.value}
                                    className={`
                  flex items-center gap-4 p-3 cursor-pointer border
                  ${positiveValues.includes(val.value)
                                        ? 'border-[var(--color-terminal-orange)] bg-[var(--color-terminal-bg-light)]'
                                        : 'border-transparent hover:bg-[var(--color-terminal-bg-light)]'
                                    }
                `}
                                    onClick={() => toggleValue(val.value)}
                                >
                                    <input
                                        type="checkbox"
                                        checked={positiveValues.includes(val.value)}
                                        onChange={() => toggleValue(val.value)}
                                        className="accent-[var(--color-terminal-orange)]"
                                    />
                                    <span className="data-value flex-1">{val.value}</span>
                                    <span className="text-[var(--color-terminal-text-dim)] text-xs">
                                        {val.count?.toLocaleString()}
                                    </span>
                                    <span className="text-[var(--color-terminal-cyan)] text-xs">
                                        {val.percentage?.toFixed(1)}%
                                    </span>
                                </label>
                            ))}
                        </div>
                    )}
                </div>
            </div>

            {/* Window & Maturity */}
            <div className="terminal-panel p-4">
                <div className="grid grid-cols-2 gap-4">
                    <div>
                        <label className="data-label block mb-1">Window (months)</label>
                        <input
                            type="number"
                            min={1}
                            value={windowMonths}
                            onChange={(e) => setWindowMonths(Number(e.target.value))}
                            className="input-terminal w-full"
                        />
                    </div>
                    <div>
                        <label className="data-label block mb-1">Maturity (months)</label>
                        <input
                            type="number"
                            min={0}
                            value={maturityMonths}
                            onChange={(e) => setMaturityMonths(Number(e.target.value))}
                            className="input-terminal w-full"
                        />
                    </div>
                </div>
            </div>

            {/* Optional Settings */}
            <div className="terminal-panel p-4">
                <div className="grid grid-cols-2 gap-4">
                    <div>
                        <label className="data-label block mb-1">Target Name (optional)</label>
                        <input
                            type="text"
                            value={targetName}
                            onChange={(e) => setTargetName(e.target.value)}
                            className="input-terminal w-full"
                            placeholder="target"
                        />
                    </div>
                    <div>
                        <label className="data-label block mb-1">Extraction Date (optional)</label>
                        <input
                            type="date"
                            value={extractionDate}
                            onChange={(e) => setExtractionDate(e.target.value)}
                            className="input-terminal w-full"
                        />
                    </div>
                </div>
            </div>

            {/* Define Target */}
            <div className="terminal-panel p-4">
                <button
                    onClick={handleDefine}
                    disabled={!canDefine() || loading}
                    className={`btn-terminal-primary px-6 ${(!canDefine() || loading) ? 'opacity-50 cursor-not-allowed' : ''}`}
                >
                    {loading ? 'DEFINING...' : 'DEFINE TARGET'}
                </button>
            </div>

            {defineResult?.errors?.length > 0 && (
                <div className="terminal-panel p-4 border-[var(--color-terminal-red)] bg-[#1a0a0a] text-[var(--color-terminal-red)] text-sm">
                    {defineResult.errors.map((err, idx) => (
                        <div key={idx}>?s??,? {err}</div>
                    ))}
                </div>
            )}

            {defineResult?.warnings?.length > 0 && (
                <div className="terminal-panel p-4 border-[var(--color-terminal-yellow)] bg-[#1a1a0a] text-[var(--color-terminal-yellow)] text-sm">
                    {defineResult.warnings.map((warn, idx) => (
                        <div key={idx}>?s??,? {warn.message || warn}</div>
                    ))}
                </div>
            )}

            {/* Distribution */}
            {distribution && (
                <div className="terminal-panel">
                    <div className="terminal-header">TARGET DISTRIBUTION</div>
                    <div className="p-4 space-y-3">
                        <div className="flex items-center gap-4">
                            <span>Class 1</span>
                            <span className="data-value-highlight">
                                {distribution.class_1_count?.toLocaleString()} ({distribution.class_1_pct?.toFixed(2)}%)
                            </span>
                        </div>
                        <div className="flex items-center gap-4">
                            <span>Class 0</span>
                            <span className="data-value">
                                {distribution.class_0_count?.toLocaleString()} ({distribution.class_0_pct?.toFixed(2)}%)
                            </span>
                        </div>
                        {distribution.warnings?.length > 0 && (
                            <div className="text-[var(--color-terminal-yellow)] text-sm">
                                {distribution.warnings.map((warn, idx) => (
                                    <div key={idx}>?s??,? {warn.message || warn}</div>
                                ))}
                            </div>
                        )}
                    </div>
                </div>
            )}

            {error && (
                <div className="terminal-panel p-4 border-[var(--color-terminal-red)] bg-[#1a0a0a] text-[var(--color-terminal-red)] text-sm">
                    ?s??,? {error}
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
                    CONTINUE ?+'
                </button>
            </div>
        </div>
    );
}
