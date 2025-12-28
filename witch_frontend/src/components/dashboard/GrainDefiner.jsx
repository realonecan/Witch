import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';

/**
 * GrainDefiner
 * 
 * Defines the observation grain for ML dataset:
 * - Entity table (e.g., clients)
 * - Entity ID column (e.g., client_id)
 * - Observation date column
 * - Date range
 * - Deduplication rule
 */
export function GrainDefiner({
    dbSessionId,
    tables = [],
    onGrainDefined,
    defineGrain,
    previewGrain,
    getTableColumns,
}) {
    // Form state
    const [entityTable, setEntityTable] = useState('');
    const [entityIdColumn, setEntityIdColumn] = useState('');
    const [observationDateColumn, setObservationDateColumn] = useState('');
    const [startDate, setStartDate] = useState('');
    const [endDate, setEndDate] = useState('');
    const [deduplicationRule, setDeduplicationRule] = useState('latest');

    // UI state
    const [columns, setColumns] = useState([]);
    const [dateColumns, setDateColumns] = useState([]);
    const [isLoadingColumns, setIsLoadingColumns] = useState(false);
    const [isDefining, setIsDefining] = useState(false);
    const [isPreviewing, setIsPreviewing] = useState(false);
    const [error, setError] = useState(null);
    const [grainResult, setGrainResult] = useState(null);
    const [previewData, setPreviewData] = useState(null);

    // Deduplication options
    const dedupOptions = [
        { value: 'latest', label: 'LATEST', desc: 'Keep most recent observation per entity per date' },
        { value: 'earliest', label: 'EARLIEST', desc: 'Keep first observation per entity per date' },
        { value: 'none', label: 'NONE', desc: 'Allow multiple observations per entity per date' },
    ];

    // Fetch columns when table changes
    useEffect(() => {
        const fetchColumns = async () => {
            if (!entityTable) {
                setColumns([]);
                setDateColumns([]);
                return;
            }

            setIsLoadingColumns(true);
            try {
                const result = await getTableColumns(entityTable);
                if (result?.columns) {
                    const cols = result.columns.map(c => ({
                        name: c.name,
                        type: c.type,
                    }));
                    setColumns(cols);

                    // Filter date-like columns
                    const dateCols = cols.filter(c =>
                        c.type.toLowerCase().includes('date') ||
                        c.type.toLowerCase().includes('timestamp') ||
                        c.name.toLowerCase().includes('date') ||
                        c.name.toLowerCase().includes('time')
                    );
                    setDateColumns(dateCols);

                    // Auto-detect entity ID
                    const idPatterns = ['client_id', 'user_id', 'customer_id', 'account_id', 'entity_id', 'id'];
                    const detectedId = cols.find(c =>
                        idPatterns.some(p => c.name.toLowerCase() === p || c.name.toLowerCase().includes(p))
                    );
                    if (detectedId) setEntityIdColumn(detectedId.name);

                    // Auto-detect date column
                    if (dateCols.length > 0) {
                        setObservationDateColumn(dateCols[0].name);
                    }
                }
            } catch (err) {
                console.error('Failed to fetch columns:', err);
            } finally {
                setIsLoadingColumns(false);
            }
        };

        fetchColumns();
    }, [entityTable, getTableColumns]);

    // Handle grain definition
    const handleDefineGrain = async () => {
        if (!entityTable || !entityIdColumn || !observationDateColumn) {
            setError('Please fill in all required fields.');
            return;
        }

        setIsDefining(true);
        setError(null);

        try {
            const result = await defineGrain({
                entity_table: entityTable,
                entity_id_column: entityIdColumn,
                observation_date_column: observationDateColumn,
                start_date: startDate || null,
                end_date: endDate || null,
                deduplication_rule: deduplicationRule,
            });

            if (result.error) {
                setError(result.error);
            } else {
                setGrainResult(result);
                // Auto-preview
                await handlePreview(result);
            }
        } catch (err) {
            setError(err.message || 'Failed to define grain');
        } finally {
            setIsDefining(false);
        }
    };

    // Handle preview
    const handlePreview = async (grain) => {
        setIsPreviewing(true);
        try {
            const result = await previewGrain({
                grain_sql: grain?.grain_sql,
                limit: 10,
            });

            if (result.error) {
                setPreviewData({ error: result.error });
            } else {
                setPreviewData(result);
            }
        } catch (err) {
            setPreviewData({ error: err.message });
        } finally {
            setIsPreviewing(false);
        }
    };

    // Confirm and proceed
    const handleConfirm = () => {
        if (grainResult && onGrainDefined) {
            onGrainDefined(grainResult);
        }
    };

    return (
        <motion.div
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
            exit={{ opacity: 0, x: -20 }}
            className="space-y-4"
        >
            {/* Entity Table Selection */}
            <div>
                <label className="block text-[11px] text-[#808080] uppercase tracking-wide mb-2">
                    ENTITY TABLE <span className="text-[#ff6b00]">*</span>
                </label>
                <select
                    value={entityTable}
                    onChange={(e) => setEntityTable(e.target.value)}
                    className="input-terminal w-full"
                >
                    <option value="">-- Select entity table --</option>
                    {tables.map((table) => (
                        <option key={table} value={table}>{table}</option>
                    ))}
                </select>
                <p className="text-[10px] text-[#505050] mt-1">
                    Table containing your entities (e.g., clients, users, accounts)
                </p>
            </div>

            {/* Column selectors (show after table selected) */}
            {entityTable && (
                <>
                    {isLoadingColumns ? (
                        <div className="p-4 bg-[#121212] border border-[#2a2a2a] text-center">
                            <span className="text-[11px] text-[#ffc107] animate-pulse">
                                ⏳ LOADING COLUMNS...
                            </span>
                        </div>
                    ) : (
                        <>
                            {/* Entity ID Column */}
                            <div>
                                <label className="block text-[11px] text-[#808080] uppercase tracking-wide mb-2">
                                    ENTITY ID COLUMN <span className="text-[#ff6b00]">*</span>
                                </label>
                                <select
                                    value={entityIdColumn}
                                    onChange={(e) => setEntityIdColumn(e.target.value)}
                                    className="input-terminal w-full"
                                >
                                    <option value="">-- Select entity ID --</option>
                                    {columns.map((col) => (
                                        <option key={col.name} value={col.name}>
                                            {col.name} ({col.type})
                                        </option>
                                    ))}
                                </select>
                                <p className="text-[10px] text-[#505050] mt-1">
                                    Uniquely identifies each entity (e.g., client_id)
                                </p>
                            </div>

                            {/* Observation Date Column */}
                            <div>
                                <label className="block text-[11px] text-[#808080] uppercase tracking-wide mb-2">
                                    OBSERVATION DATE <span className="text-[#ff6b00]">*</span>
                                </label>
                                <select
                                    value={observationDateColumn}
                                    onChange={(e) => setObservationDateColumn(e.target.value)}
                                    className="input-terminal w-full"
                                >
                                    <option value="">-- Select date column --</option>
                                    {dateColumns.length > 0 ? (
                                        dateColumns.map((col) => (
                                            <option key={col.name} value={col.name}>
                                                {col.name} ({col.type})
                                            </option>
                                        ))
                                    ) : (
                                        columns.map((col) => (
                                            <option key={col.name} value={col.name}>
                                                {col.name} ({col.type})
                                            </option>
                                        ))
                                    )}
                                </select>
                                <p className="text-[10px] text-[#505050] mt-1">
                                    Date column for point-in-time observations
                                </p>
                            </div>

                            {/* Date Range */}
                            <div className="grid grid-cols-2 gap-4">
                                <div>
                                    <label className="block text-[11px] text-[#808080] uppercase tracking-wide mb-2">
                                        START DATE (optional)
                                    </label>
                                    <input
                                        type="date"
                                        value={startDate}
                                        onChange={(e) => setStartDate(e.target.value)}
                                        className="input-terminal w-full"
                                    />
                                </div>
                                <div>
                                    <label className="block text-[11px] text-[#808080] uppercase tracking-wide mb-2">
                                        END DATE (optional)
                                    </label>
                                    <input
                                        type="date"
                                        value={endDate}
                                        onChange={(e) => setEndDate(e.target.value)}
                                        className="input-terminal w-full"
                                    />
                                </div>
                            </div>

                            {/* Deduplication Rule */}
                            <div>
                                <label className="block text-[11px] text-[#808080] uppercase tracking-wide mb-2">
                                    DEDUPLICATION RULE
                                </label>
                                <div className="flex gap-2">
                                    {dedupOptions.map((opt) => (
                                        <button
                                            key={opt.value}
                                            type="button"
                                            onClick={() => setDeduplicationRule(opt.value)}
                                            className={`flex-1 px-3 py-2 text-[10px] font-mono border transition-all ${deduplicationRule === opt.value
                                                    ? 'bg-[#ff6b00] border-[#ff6b00] text-black'
                                                    : 'bg-[#0a0a0a] border-[#2a2a2a] text-[#808080] hover:border-[#3a3a3a]'
                                                }`}
                                        >
                                            {opt.label}
                                        </button>
                                    ))}
                                </div>
                                <p className="text-[10px] text-[#505050] mt-1">
                                    {dedupOptions.find(o => o.value === deduplicationRule)?.desc}
                                </p>
                            </div>
                        </>
                    )}
                </>
            )}

            {/* Error Display */}
            {error && (
                <div className="p-3 bg-[#ff174420] border border-[#ff1744] text-[#ff1744] text-[11px]">
                    ❌ {error}
                </div>
            )}

            {/* Define Button */}
            {entityTable && entityIdColumn && observationDateColumn && !grainResult && (
                <button
                    type="button"
                    onClick={handleDefineGrain}
                    disabled={isDefining}
                    className="btn-terminal w-full text-[11px] py-3 disabled:opacity-50"
                >
                    {isDefining ? '⏳ DEFINING GRAIN...' : '📐 DEFINE GRAIN'}
                </button>
            )}

            {/* Grain Result & Preview */}
            {grainResult && (
                <div className="space-y-4">
                    {/* Stats */}
                    <div className="p-4 bg-[#00c85310] border border-[#00c853]">
                        <div className="flex items-center justify-between mb-3">
                            <span className="text-[12px] text-[#00c853] font-bold">
                                ✓ GRAIN DEFINED
                            </span>
                            <button
                                type="button"
                                onClick={() => {
                                    setGrainResult(null);
                                    setPreviewData(null);
                                }}
                                className="btn-terminal text-[10px] px-3"
                            >
                                ↻ CHANGE
                            </button>
                        </div>
                        <div className="grid grid-cols-3 gap-4 text-center">
                            <div>
                                <div className="text-[18px] font-bold text-white">
                                    {grainResult.stats?.unique_entities?.toLocaleString() || '—'}
                                </div>
                                <div className="text-[10px] text-[#808080] uppercase">Entities</div>
                            </div>
                            <div>
                                <div className="text-[18px] font-bold text-white">
                                    {grainResult.stats?.total_observations?.toLocaleString() || '—'}
                                </div>
                                <div className="text-[10px] text-[#808080] uppercase">Observations</div>
                            </div>
                            <div>
                                <div className="text-[18px] font-bold text-white">
                                    {grainResult.stats?.date_range || '—'}
                                </div>
                                <div className="text-[10px] text-[#808080] uppercase">Date Range</div>
                            </div>
                        </div>
                    </div>

                    {/* Preview Table */}
                    {isPreviewing ? (
                        <div className="p-4 bg-[#121212] border border-[#2a2a2a] text-center">
                            <span className="text-[11px] text-[#ffc107] animate-pulse">
                                ⏳ LOADING PREVIEW...
                            </span>
                        </div>
                    ) : previewData?.rows?.length > 0 && (
                        <div className="p-4 bg-[#121212] border border-[#2a2a2a]">
                            <div className="text-[11px] text-[#808080] uppercase tracking-wide mb-2">
                                PREVIEW (first {previewData.rows.length} rows)
                            </div>
                            <div className="overflow-x-auto">
                                <table className="w-full text-[11px] font-mono">
                                    <thead>
                                        <tr className="border-b border-[#2a2a2a]">
                                            {previewData.columns?.map((col) => (
                                                <th key={col} className="text-left py-2 px-3 text-[#ff6b00]">
                                                    {col}
                                                </th>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {previewData.rows.map((row, i) => (
                                            <tr key={i} className="border-b border-[#1a1a1a]">
                                                {previewData.columns?.map((col) => (
                                                    <td key={col} className="py-2 px-3 text-[#e0e0e0]">
                                                        {row[col] ?? '—'}
                                                    </td>
                                                ))}
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    )}

                    {/* Confirm Button */}
                    <button
                        type="button"
                        onClick={handleConfirm}
                        className="btn-terminal w-full text-[11px] py-3 bg-[#00c853] text-black hover:bg-[#00a844]"
                    >
                        ✓ CONFIRM GRAIN & CONTINUE
                    </button>
                </div>
            )}
        </motion.div>
    );
}
