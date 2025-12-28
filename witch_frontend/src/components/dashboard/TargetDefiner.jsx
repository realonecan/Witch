import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import apiClient from '../../api/client';

/**
 * TargetDefiner
 * 
 * Data-driven target definition:
 * - Detect candidate columns (status-like)
 * - Select column and positive class values
 * - Preview distribution
 * - Generate target SQL logic
 */
export function TargetDefiner({
    dbSessionId,
    tableName,
    groupingColumn,
    defineTarget,
    getTargetDistribution,
    onTargetDefined,
}) {
    // Column detection
    const [targetColumns, setTargetColumns] = useState([]);
    const [isLoadingColumns, setIsLoadingColumns] = useState(false);

    // Selected column
    const [selectedColumn, setSelectedColumn] = useState(null);
    const [columnValues, setColumnValues] = useState([]);
    const [isLoadingValues, setIsLoadingValues] = useState(false);

    // Value selection
    const [selectedValues, setSelectedValues] = useState(new Set());

    // Generated target
    const [targetResult, setTargetResult] = useState(null);
    const [targetPreview, setTargetPreview] = useState(null);
    const [isPreviewLoading, setIsPreviewLoading] = useState(false);

    // UI state
    const [isGenerating, setIsGenerating] = useState(false);
    const [error, setError] = useState(null);

    // Detect target columns on mount
    useEffect(() => {
        const detectColumns = async () => {
            if (!dbSessionId || !tableName) return;

            setIsLoadingColumns(true);
            try {
                const response = await apiClient.post('/detect-target-columns', {
                    session_id: dbSessionId,
                    table_name: tableName,
                });

                const candidates = response.data?.candidates || [];
                setTargetColumns(candidates);

                // Auto-select first status-like column
                const statusCol = candidates.find(c => c.is_status_like);
                if (statusCol) {
                    await selectColumn(statusCol.column_name);
                }
            } catch (err) {
                console.error('Failed to detect columns:', err);
                setError(err.response?.data?.detail || 'Failed to detect target columns');
            } finally {
                setIsLoadingColumns(false);
            }
        };

        detectColumns();
    }, [dbSessionId, tableName]);

    // Select a column and load its values
    const selectColumn = async (columnName) => {
        setSelectedColumn(columnName);
        setColumnValues([]);
        setSelectedValues(new Set());
        setTargetResult(null);
        setTargetPreview(null);
        setIsLoadingValues(true);

        try {
            const response = await apiClient.post('/get-column-values', {
                session_id: dbSessionId,
                table_name: tableName,
                column_name: columnName,
                limit: 50,
            });

            setColumnValues(response.data?.values || []);
        } catch (err) {
            console.error('Failed to get values:', err);
            setError(err.response?.data?.detail || 'Failed to get column values');
        } finally {
            setIsLoadingValues(false);
        }
    };

    // Toggle value selection
    const toggleValue = (value) => {
        const newSelected = new Set(selectedValues);
        if (newSelected.has(value)) {
            newSelected.delete(value);
        } else {
            newSelected.add(value);
        }
        setSelectedValues(newSelected);
        setTargetResult(null);
        setTargetPreview(null);
    };

    // Generate target
    const handleGenerateTarget = async () => {
        if (selectedValues.size === 0 || !selectedColumn) {
            setError('Please select at least one value for the positive class.');
            return;
        }

        setIsGenerating(true);
        setError(null);

        try {
            const response = await apiClient.post('/generate-target', {
                session_id: dbSessionId,
                table_name: tableName,
                column_name: selectedColumn,
                selected_values: Array.from(selectedValues),
                grouping_column: groupingColumn,
            });

            setTargetResult(response.data);

            // Auto-preview
            await previewDistribution(response.data);
        } catch (err) {
            console.error('Generate target error:', err);
            setError(err.response?.data?.detail || 'Failed to generate target');
        } finally {
            setIsGenerating(false);
        }
    };

    // Preview distribution
    const previewDistribution = async (target) => {
        if (!target?.sql_logic) return;

        setIsPreviewLoading(true);
        try {
            const response = await apiClient.post('/preview-target', {
                session_id: dbSessionId,
                table_name: tableName,
                sql_logic: target.sql_logic,
                target_name: target.target_name,
                grouping_column: groupingColumn,
            });

            setTargetPreview(response.data);
        } catch (err) {
            console.error('Preview error:', err);
            setTargetPreview({
                status: 'error',
                warnings: [{ message: 'Could not preview distribution' }],
                is_usable: true,
            });
        } finally {
            setIsPreviewLoading(false);
        }
    };

    // Confirm and proceed
    const handleConfirm = () => {
        if (targetResult && onTargetDefined) {
            onTargetDefined(targetResult);
        }
    };

    return (
        <motion.div
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
            exit={{ opacity: 0, x: -20 }}
            className="space-y-4"
        >
            {/* Context Bar */}
            <div className="flex items-center gap-4 p-3 bg-[#121212] border border-[#2a2a2a]">
                <div className="text-[10px] text-[#808080]">
                    TABLE: <span className="text-[#ff6b00]">{tableName}</span>
                </div>
                <div className="text-[10px] text-[#808080]">
                    GROUP BY: <span className="text-[#2196f3]">{groupingColumn}</span>
                </div>
            </div>

            {/* Loading Columns */}
            {isLoadingColumns && (
                <div className="p-6 bg-[#121212] border border-[#2a2a2a] text-center">
                    <span className="text-[12px] text-[#ffc107] animate-pulse">
                        ⏳ DETECTING TARGET COLUMNS...
                    </span>
                </div>
            )}

            {/* Column Selection */}
            {!isLoadingColumns && targetColumns.length > 0 && (
                <div className="p-4 bg-[#121212] border border-[#2a2a2a]">
                    <div className="text-[11px] text-[#808080] uppercase tracking-wide mb-3">
                        📊 SELECT A STATUS/STATE COLUMN
                    </div>
                    <div className="flex flex-wrap gap-2">
                        {targetColumns.map((col) => (
                            <button
                                key={col.column_name}
                                type="button"
                                onClick={() => selectColumn(col.column_name)}
                                className={`px-3 py-2 text-[11px] font-mono border transition-all ${selectedColumn === col.column_name
                                        ? 'bg-[#ff6b00] border-[#ff6b00] text-black'
                                        : col.is_status_like
                                            ? 'bg-[#1a1a1a] border-[#ff6b00] text-[#ff6b00] hover:bg-[#ff6b0020]'
                                            : 'bg-[#0a0a0a] border-[#2a2a2a] text-[#808080] hover:border-[#3a3a3a]'
                                    }`}
                            >
                                {col.column_name}
                                <span className="ml-2 text-[9px] opacity-60">
                                    ({col.distinct_count} values)
                                </span>
                                {col.is_status_like && (
                                    <span className="ml-1 text-[8px]">⭐</span>
                                )}
                            </button>
                        ))}
                    </div>
                    <p className="text-[9px] text-[#505050] mt-2">
                        ⭐ = Detected as status/state column (recommended)
                    </p>
                </div>
            )}

            {/* No columns found */}
            {!isLoadingColumns && targetColumns.length === 0 && (
                <div className="p-4 bg-[#ff6b0020] border border-[#ff6b00] text-center">
                    <div className="text-[11px] text-[#ff6b00]">
                        ⚠️ No suitable status columns detected.
                    </div>
                </div>
            )}

            {/* Loading Values */}
            {isLoadingValues && (
                <div className="p-6 bg-[#121212] border border-[#2a2a2a] text-center">
                    <span className="text-[12px] text-[#ffc107] animate-pulse">
                        ⏳ LOADING COLUMN VALUES...
                    </span>
                </div>
            )}

            {/* Value Selection */}
            {!isLoadingValues && selectedColumn && columnValues.length > 0 && !targetResult && (
                <div className="p-4 bg-[#121212] border border-[#2a2a2a]">
                    <div className="text-[11px] text-[#808080] uppercase tracking-wide mb-3">
                        🎯 SELECT VALUES FOR POSITIVE CLASS (TARGET = 1)
                    </div>
                    <p className="text-[10px] text-[#606060] mb-4">
                        Check values that indicate the target event.
                    </p>

                    <div className="space-y-2 max-h-64 overflow-y-auto">
                        {columnValues.map((item) => {
                            const isSelected = selectedValues.has(item.value);
                            const displayValue = item.is_null ? '(NULL)' : item.value;

                            return (
                                <div
                                    key={item.value}
                                    onClick={() => toggleValue(item.value)}
                                    className={`flex items-center justify-between p-3 border cursor-pointer transition-all ${isSelected
                                            ? 'bg-[#00c85320] border-[#00c853]'
                                            : 'bg-[#0a0a0a] border-[#2a2a2a] hover:border-[#3a3a3a]'
                                        }`}
                                >
                                    <div className="flex items-center gap-3">
                                        <div className={`w-5 h-5 border flex items-center justify-center text-[10px] ${isSelected
                                                ? 'bg-[#00c853] border-[#00c853] text-black'
                                                : 'border-[#606060]'
                                            }`}>
                                            {isSelected && '✓'}
                                        </div>
                                        <span className={`text-[12px] font-mono ${item.is_null ? 'text-[#808080] italic' : 'text-[#e0e0e0]'
                                            }`}>
                                            {displayValue}
                                        </span>
                                    </div>
                                    <div className="flex items-center gap-4">
                                        <span className="text-[11px] text-[#808080]">
                                            {item.count?.toLocaleString()}
                                        </span>
                                        <div className="w-24 h-2 bg-[#0a0a0a] border border-[#2a2a2a] overflow-hidden">
                                            <div
                                                className="h-full bg-[#ff6b00]"
                                                style={{ width: `${Math.min(item.percentage || 0, 100)}%` }}
                                            />
                                        </div>
                                        <span className="text-[10px] text-[#606060] w-14 text-right">
                                            {(item.percentage || 0).toFixed(1)}%
                                        </span>
                                    </div>
                                </div>
                            );
                        })}
                    </div>

                    {/* Generate Button */}
                    <div className="mt-4 flex items-center justify-between">
                        <span className="text-[10px] text-[#808080]">
                            {selectedValues.size} value(s) selected
                        </span>
                        <button
                            type="button"
                            onClick={handleGenerateTarget}
                            disabled={selectedValues.size === 0 || isGenerating}
                            className="btn-terminal text-[11px] px-6 py-2 disabled:opacity-50"
                        >
                            {isGenerating ? '⏳ GENERATING...' : '📝 GENERATE TARGET'}
                        </button>
                    </div>
                </div>
            )}

            {/* Error */}
            {error && (
                <div className="p-3 bg-[#ff174420] border border-[#ff1744] text-[#ff1744] text-[11px]">
                    ❌ {error}
                </div>
            )}

            {/* Target Result */}
            {targetResult && (
                <div className="space-y-4">
                    <div className="p-4 bg-[#121212] border border-[#2a2a2a] space-y-3">
                        <div className="flex items-center justify-between">
                            <div>
                                <div className="text-[10px] text-[#808080] uppercase mb-1">TARGET NAME</div>
                                <div className="text-[14px] text-[#ff6b00] font-mono font-bold">
                                    {targetResult.target_name}
                                </div>
                            </div>
                            <button
                                type="button"
                                onClick={() => {
                                    setTargetResult(null);
                                    setTargetPreview(null);
                                }}
                                className="btn-terminal text-[10px] px-3"
                            >
                                ↻ CHANGE
                            </button>
                        </div>
                        <div>
                            <div className="text-[10px] text-[#808080] uppercase mb-1">DESCRIPTION</div>
                            <div className="text-[11px] text-[#e0e0e0]">{targetResult.description}</div>
                        </div>
                        <div>
                            <div className="text-[10px] text-[#808080] uppercase mb-1">SQL LOGIC</div>
                            <pre className="bg-[#0a0a0a] border border-[#2a2a2a] p-3 text-[11px] font-mono text-[#00bcd4] overflow-x-auto">
                                {targetResult.sql_logic}
                            </pre>
                        </div>
                    </div>

                    {/* Preview Loading */}
                    {isPreviewLoading && (
                        <div className="p-4 bg-[#121212] border border-[#2a2a2a] text-center">
                            <span className="text-[11px] text-[#ffc107] animate-pulse">
                                ⏳ CHECKING DISTRIBUTION...
                            </span>
                        </div>
                    )}

                    {/* Preview Result */}
                    {!isPreviewLoading && targetPreview && (
                        <div className={`p-4 border ${!targetPreview.is_usable
                                ? 'bg-[#ff174420] border-[#ff1744]'
                                : targetPreview.warnings?.length > 0
                                    ? 'bg-[#ff6b0020] border-[#ff6b00]'
                                    : 'bg-[#00c85310] border-[#00c853]'
                            }`}>
                            <div className="flex items-center gap-2 mb-3">
                                {!targetPreview.is_usable ? (
                                    <span className="text-[12px] text-[#ff1744] font-bold">🚫 UNUSABLE</span>
                                ) : targetPreview.warnings?.length > 0 ? (
                                    <span className="text-[12px] text-[#ff6b00] font-bold">⚠️ WARNING</span>
                                ) : (
                                    <span className="text-[12px] text-[#00c853] font-bold">✓ GOOD</span>
                                )}
                            </div>

                            {/* Distribution bars */}
                            {targetPreview.distribution && (
                                <div className="space-y-2">
                                    {targetPreview.distribution.map((d) => (
                                        <div key={d.value} className="flex items-center gap-3">
                                            <span className="w-16 text-[11px] font-mono text-[#808080]">
                                                {targetPreview.target_name} = {d.value}
                                            </span>
                                            <div className="flex-1 h-4 bg-[#0a0a0a] border border-[#2a2a2a] overflow-hidden">
                                                <div
                                                    className={`h-full ${d.value === 1 ? 'bg-[#00c853]' : 'bg-[#ff6b00]'}`}
                                                    style={{ width: `${Math.min(d.percentage, 100)}%` }}
                                                />
                                            </div>
                                            <span className="w-20 text-[11px] text-[#808080] text-right">
                                                {d.count?.toLocaleString()} ({d.percentage?.toFixed(1)}%)
                                            </span>
                                        </div>
                                    ))}
                                </div>
                            )}
                        </div>
                    )}

                    {/* Confirm Button */}
                    {targetPreview?.is_usable && (
                        <button
                            type="button"
                            onClick={handleConfirm}
                            className="btn-terminal w-full text-[11px] py-3 bg-[#00c853] text-black hover:bg-[#00a844]"
                        >
                            ✓ CONFIRM TARGET & CONTINUE
                        </button>
                    )}
                </div>
            )}
        </motion.div>
    );
}
