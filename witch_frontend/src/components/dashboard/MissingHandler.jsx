import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';

/**
 * MissingHandler
 * 
 * Configure missing value strategies per feature column:
 * - ZERO: Replace NULL with 0
 * - NULL: Keep as NULL
 * - SENTINEL: Replace with sentinel value (e.g., -999)
 * - MEAN: Mark for post-SQL mean imputation
 * 
 * Optionally add indicator columns (is_missing_<col>)
 */
export function MissingHandler({
    features = [],
    applyMissingStrategy,
    recommendMissingStrategy,
    onStrategiesApplied,
}) {
    // Strategy configuration per column
    const [columnStrategies, setColumnStrategies] = useState({});
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState(null);

    // Available strategies
    const strategies = [
        { value: 'zero', label: 'ZERO', desc: 'Replace NULL with 0', color: '#00c853' },
        { value: 'null', label: 'NULL', desc: 'Keep as NULL', color: '#808080' },
        { value: 'sentinel', label: 'SENTINEL', desc: 'Replace with -999', color: '#ff6b00' },
        { value: 'mean', label: 'MEAN', desc: 'Post-SQL mean imputation', color: '#2196f3' },
    ];

    // Initialize strategies based on feature types
    useEffect(() => {
        const initStrategies = {};
        features.forEach((feature) => {
            (feature.feature_columns || []).forEach((col) => {
                // Default strategy based on column name
                let defaultStrategy = 'zero';
                if (col.toLowerCase().includes('avg') || col.toLowerCase().includes('mean')) {
                    defaultStrategy = 'mean';
                } else if (col.toLowerCase().includes('recency')) {
                    defaultStrategy = 'sentinel';
                }

                initStrategies[col] = {
                    strategy: defaultStrategy,
                    add_indicator: false,
                    sentinel_value: -999,
                };
            });
        });
        setColumnStrategies(initStrategies);
    }, [features]);

    // Update strategy for a column
    const updateStrategy = (column, field, value) => {
        setColumnStrategies(prev => ({
            ...prev,
            [column]: {
                ...prev[column],
                [field]: value,
            },
        }));
    };

    // Get all columns
    const allColumns = features.flatMap(f => f.feature_columns || []);

    // Apply all strategies
    const handleApply = async () => {
        setIsLoading(true);
        setError(null);

        try {
            // Build column configs
            const columns = Object.entries(columnStrategies).map(([col, config]) => ({
                column_name: col,
                strategy: config.strategy,
                add_indicator: config.add_indicator,
                sentinel_value: config.sentinel_value,
            }));

            // Group by feature for API call
            const result = await applyMissingStrategy({
                feature_name: 'dataset_features',
                feature_key: 'dataset_features',
                columns,
                source_alias: 'features',
            });

            if (result.error) {
                setError(result.error);
            } else {
                if (onStrategiesApplied) {
                    onStrategiesApplied({
                        strategies: columnStrategies,
                        post_sql_impute: result.post_sql_impute || [],
                        wrapper_cte: result.wrapper_cte,
                    });
                }
            }
        } catch (err) {
            setError(err.message || 'Failed to apply strategies');
        } finally {
            setIsLoading(false);
        }
    };

    // Set all to same strategy
    const setAllStrategy = (strategy) => {
        setColumnStrategies(prev => {
            const updated = { ...prev };
            Object.keys(updated).forEach(col => {
                updated[col] = { ...updated[col], strategy };
            });
            return updated;
        });
    };

    // Toggle all indicators
    const toggleAllIndicators = () => {
        const allOn = Object.values(columnStrategies).every(c => c.add_indicator);
        setColumnStrategies(prev => {
            const updated = { ...prev };
            Object.keys(updated).forEach(col => {
                updated[col] = { ...updated[col], add_indicator: !allOn };
            });
            return updated;
        });
    };

    return (
        <motion.div
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
            exit={{ opacity: 0, x: -20 }}
            className="space-y-4"
        >
            {/* Header */}
            <div className="flex items-center justify-between">
                <div className="text-[11px] text-[#808080] uppercase tracking-wide">
                    🔧 MISSING VALUE STRATEGIES
                </div>
                <div className="text-[10px] text-[#606060]">
                    {allColumns.length} column(s) to configure
                </div>
            </div>

            {/* Quick Actions */}
            <div className="flex items-center gap-2 p-3 bg-[#121212] border border-[#2a2a2a]">
                <span className="text-[10px] text-[#808080]">QUICK SET:</span>
                {strategies.map((s) => (
                    <button
                        key={s.value}
                        type="button"
                        onClick={() => setAllStrategy(s.value)}
                        className="px-2 py-1 text-[9px] font-mono border border-[#2a2a2a] hover:border-[#3a3a3a] text-[#808080] hover:text-white transition-all"
                    >
                        ALL {s.label}
                    </button>
                ))}
                <button
                    type="button"
                    onClick={toggleAllIndicators}
                    className="px-2 py-1 text-[9px] font-mono border border-[#2a2a2a] hover:border-[#3a3a3a] text-[#808080] hover:text-white transition-all"
                >
                    TOGGLE INDICATORS
                </button>
            </div>

            {/* Column List */}
            <div className="space-y-2 max-h-[400px] overflow-y-auto">
                {allColumns.map((col) => {
                    const config = columnStrategies[col] || { strategy: 'zero', add_indicator: false };

                    return (
                        <div
                            key={col}
                            className="p-3 bg-[#0a0a0a] border border-[#2a2a2a] space-y-2"
                        >
                            {/* Column Name */}
                            <div className="flex items-center justify-between">
                                <span className="text-[12px] font-mono text-[#e0e0e0]">{col}</span>
                                <div className="flex items-center gap-2">
                                    <label className="flex items-center gap-1 cursor-pointer">
                                        <input
                                            type="checkbox"
                                            checked={config.add_indicator}
                                            onChange={(e) => updateStrategy(col, 'add_indicator', e.target.checked)}
                                            className="w-3 h-3"
                                        />
                                        <span className="text-[9px] text-[#606060]">+ INDICATOR</span>
                                    </label>
                                </div>
                            </div>

                            {/* Strategy Buttons */}
                            <div className="flex gap-1">
                                {strategies.map((s) => (
                                    <button
                                        key={s.value}
                                        type="button"
                                        onClick={() => updateStrategy(col, 'strategy', s.value)}
                                        className={`flex-1 px-2 py-1 text-[9px] font-mono border transition-all ${config.strategy === s.value
                                                ? `bg-[${s.color}20] border-[${s.color}] text-[${s.color}]`
                                                : 'bg-transparent border-[#2a2a2a] text-[#606060] hover:border-[#3a3a3a]'
                                            }`}
                                        style={config.strategy === s.value ? {
                                            backgroundColor: `${s.color}20`,
                                            borderColor: s.color,
                                            color: s.color,
                                        } : {}}
                                        title={s.desc}
                                    >
                                        {s.label}
                                    </button>
                                ))}
                            </div>

                            {/* Sentinel Value Input */}
                            {config.strategy === 'sentinel' && (
                                <div className="flex items-center gap-2">
                                    <span className="text-[9px] text-[#606060]">VALUE:</span>
                                    <input
                                        type="number"
                                        value={config.sentinel_value}
                                        onChange={(e) => updateStrategy(col, 'sentinel_value', parseInt(e.target.value, 10))}
                                        className="input-terminal w-24 text-[10px] py-1"
                                    />
                                </div>
                            )}
                        </div>
                    );
                })}
            </div>

            {/* Summary */}
            <div className="p-3 bg-[#121212] border border-[#2a2a2a]">
                <div className="text-[10px] text-[#808080] uppercase mb-2">SUMMARY</div>
                <div className="flex gap-4 text-[11px]">
                    {strategies.map((s) => {
                        const count = Object.values(columnStrategies).filter(c => c.strategy === s.value).length;
                        return (
                            <span key={s.value} style={{ color: s.color }}>
                                {s.label}: {count}
                            </span>
                        );
                    })}
                    <span className="text-[#ffc107]">
                        INDICATORS: {Object.values(columnStrategies).filter(c => c.add_indicator).length}
                    </span>
                </div>
            </div>

            {/* Error */}
            {error && (
                <div className="p-3 bg-[#ff174420] border border-[#ff1744] text-[#ff1744] text-[11px]">
                    ❌ {error}
                </div>
            )}

            {/* Apply Button */}
            <button
                type="button"
                onClick={handleApply}
                disabled={isLoading || allColumns.length === 0}
                className="btn-terminal w-full text-[11px] py-3 disabled:opacity-50"
            >
                {isLoading ? '⏳ APPLYING...' : '✓ APPLY STRATEGIES & ASSEMBLE DATASET'}
            </button>
        </motion.div>
    );
}
