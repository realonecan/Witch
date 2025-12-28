import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';

/**
 * FeatureBuilder
 * 
 * Template-based feature generation:
 * - rolling_count, rolling_sum, rolling_avg
 * - recency, distinct_count
 * - Configurable windows (30d, 60d, 90d)
 */
export function FeatureBuilder({
    dbSessionId,
    tables = [],
    grainConfig,
    listFeatureTemplates,
    generateFeature,
    getTableColumns,
    onFeaturesGenerated,
}) {
    // Templates
    const [templates, setTemplates] = useState([]);
    const [isLoadingTemplates, setIsLoadingTemplates] = useState(false);

    // Feature configuration
    const [features, setFeatures] = useState([]);
    const [currentFeature, setCurrentFeature] = useState({
        name: '',
        key: '',
        template: '',
        source_table: '',
        value_column: '',
        windows: [30],
    });

    // UI state
    const [columns, setColumns] = useState([]);
    const [isLoadingColumns, setIsLoadingColumns] = useState(false);
    const [isGenerating, setIsGenerating] = useState(false);
    const [error, setError] = useState(null);
    const [generatedSql, setGeneratedSql] = useState(null);

    // Window options
    const windowOptions = [7, 14, 30, 60, 90, 180, 365];

    // Load templates on mount
    useEffect(() => {
        const loadTemplates = async () => {
            setIsLoadingTemplates(true);
            try {
                const result = await listFeatureTemplates();
                if (result?.templates) {
                    setTemplates(result.templates);
                }
            } catch (err) {
                console.error('Failed to load templates:', err);
            } finally {
                setIsLoadingTemplates(false);
            }
        };
        loadTemplates();
    }, [listFeatureTemplates]);

    // Fetch columns when source table changes
    useEffect(() => {
        const fetchColumns = async () => {
            if (!currentFeature.source_table) {
                setColumns([]);
                return;
            }

            setIsLoadingColumns(true);
            try {
                const result = await getTableColumns(currentFeature.source_table);
                if (result?.columns) {
                    setColumns(result.columns.map(c => ({
                        name: c.name,
                        type: c.type,
                    })));
                }
            } catch (err) {
                console.error('Failed to fetch columns:', err);
            } finally {
                setIsLoadingColumns(false);
            }
        };
        fetchColumns();
    }, [currentFeature.source_table, getTableColumns]);

    // Auto-generate key from name
    const handleNameChange = (name) => {
        const key = name.toLowerCase().replace(/[^a-z0-9_]/g, '_').replace(/__+/g, '_');
        setCurrentFeature(prev => ({ ...prev, name, key }));
    };

    // Toggle window selection
    const toggleWindow = (window) => {
        setCurrentFeature(prev => {
            const windows = prev.windows.includes(window)
                ? prev.windows.filter(w => w !== window)
                : [...prev.windows, window].sort((a, b) => a - b);
            return { ...prev, windows };
        });
    };

    // Add feature to list
    const handleAddFeature = () => {
        if (!currentFeature.name || !currentFeature.template || !currentFeature.source_table) {
            setError('Please fill in all required fields.');
            return;
        }

        setFeatures(prev => [...prev, { ...currentFeature, id: Date.now() }]);
        setCurrentFeature({
            name: '',
            key: '',
            template: '',
            source_table: currentFeature.source_table, // Keep same table
            value_column: '',
            windows: [30],
        });
        setError(null);
    };

    // Remove feature
    const removeFeature = (id) => {
        setFeatures(prev => prev.filter(f => f.id !== id));
    };

    // Generate all features
    const handleGenerateFeatures = async () => {
        if (features.length === 0) {
            setError('Add at least one feature before generating.');
            return;
        }

        setIsGenerating(true);
        setError(null);

        try {
            const generatedFeatures = [];

            for (const feature of features) {
                const result = await generateFeature({
                    name: feature.name,
                    key: feature.key,
                    template_type: feature.template,
                    source_table: feature.source_table,
                    value_column: feature.value_column,
                    windows: feature.windows,
                    entity_id_column: grainConfig?.entity_id_column,
                    event_time_column: grainConfig?.observation_date_column,
                });

                if (result.error) {
                    setError(`Failed to generate ${feature.name}: ${result.error}`);
                    break;
                }

                generatedFeatures.push({
                    ...feature,
                    sql: result.sql,
                    feature_columns: result.feature_columns,
                    max_source_time_column: result.max_source_time_column,
                });
            }

            if (generatedFeatures.length === features.length) {
                setGeneratedSql(generatedFeatures);
                if (onFeaturesGenerated) {
                    onFeaturesGenerated(generatedFeatures);
                }
            }
        } catch (err) {
            setError(err.message || 'Failed to generate features');
        } finally {
            setIsGenerating(false);
        }
    };

    // Get template icon
    const getTemplateIcon = (type) => {
        const icons = {
            rolling_count: '#',
            rolling_sum: '∑',
            rolling_avg: 'μ',
            recency: '⏱',
            distinct_count: '◇',
        };
        return icons[type] || '•';
    };

    return (
        <motion.div
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
            exit={{ opacity: 0, x: -20 }}
            className="space-y-4"
        >
            {/* Current Feature Builder */}
            <div className="p-4 bg-[#121212] border border-[#2a2a2a] space-y-4">
                <div className="text-[11px] text-[#808080] uppercase tracking-wide">
                    ➕ ADD FEATURE
                </div>

                {/* Feature Name */}
                <div>
                    <label className="block text-[10px] text-[#606060] uppercase mb-1">
                        Feature Name
                    </label>
                    <input
                        type="text"
                        value={currentFeature.name}
                        onChange={(e) => handleNameChange(e.target.value)}
                        placeholder="e.g., Transaction Count"
                        className="input-terminal w-full text-[12px]"
                    />
                </div>

                {/* Template Selection */}
                <div>
                    <label className="block text-[10px] text-[#606060] uppercase mb-1">
                        Template Type
                    </label>
                    {isLoadingTemplates ? (
                        <div className="text-[11px] text-[#ffc107] animate-pulse">Loading...</div>
                    ) : (
                        <div className="flex flex-wrap gap-2">
                            {templates.map((t) => (
                                <button
                                    key={t.template_type}
                                    type="button"
                                    onClick={() => setCurrentFeature(prev => ({ ...prev, template: t.template_type }))}
                                    className={`px-3 py-2 text-[10px] font-mono border transition-all ${currentFeature.template === t.template_type
                                            ? 'bg-[#ff6b00] border-[#ff6b00] text-black'
                                            : 'bg-[#0a0a0a] border-[#2a2a2a] text-[#808080] hover:border-[#3a3a3a]'
                                        }`}
                                    title={t.description}
                                >
                                    <span className="mr-1">{getTemplateIcon(t.template_type)}</span>
                                    {t.template_type.toUpperCase()}
                                </button>
                            ))}
                        </div>
                    )}
                </div>

                {/* Source Table & Column */}
                <div className="grid grid-cols-2 gap-4">
                    <div>
                        <label className="block text-[10px] text-[#606060] uppercase mb-1">
                            Source Table
                        </label>
                        <select
                            value={currentFeature.source_table}
                            onChange={(e) => setCurrentFeature(prev => ({ ...prev, source_table: e.target.value, value_column: '' }))}
                            className="input-terminal w-full text-[12px]"
                        >
                            <option value="">-- Select --</option>
                            {tables.map((table) => (
                                <option key={table} value={table}>{table}</option>
                            ))}
                        </select>
                    </div>
                    <div>
                        <label className="block text-[10px] text-[#606060] uppercase mb-1">
                            Value Column {currentFeature.template !== 'rolling_count' && '*'}
                        </label>
                        <select
                            value={currentFeature.value_column}
                            onChange={(e) => setCurrentFeature(prev => ({ ...prev, value_column: e.target.value }))}
                            className="input-terminal w-full text-[12px]"
                            disabled={isLoadingColumns || !currentFeature.source_table}
                        >
                            <option value="">-- Select --</option>
                            {columns.map((col) => (
                                <option key={col.name} value={col.name}>{col.name}</option>
                            ))}
                        </select>
                    </div>
                </div>

                {/* Window Selection */}
                <div>
                    <label className="block text-[10px] text-[#606060] uppercase mb-1">
                        Time Windows (days)
                    </label>
                    <div className="flex flex-wrap gap-2">
                        {windowOptions.map((w) => (
                            <button
                                key={w}
                                type="button"
                                onClick={() => toggleWindow(w)}
                                className={`px-3 py-1 text-[10px] font-mono border transition-all ${currentFeature.windows.includes(w)
                                        ? 'bg-[#00c853] border-[#00c853] text-black'
                                        : 'bg-[#0a0a0a] border-[#2a2a2a] text-[#606060] hover:border-[#3a3a3a]'
                                    }`}
                            >
                                {w}d
                            </button>
                        ))}
                    </div>
                </div>

                {/* Add Button */}
                <button
                    type="button"
                    onClick={handleAddFeature}
                    disabled={!currentFeature.name || !currentFeature.template || !currentFeature.source_table}
                    className="btn-terminal text-[11px] px-4 py-2 disabled:opacity-50"
                >
                    ➕ ADD TO LIST
                </button>
            </div>

            {/* Error Display */}
            {error && (
                <div className="p-3 bg-[#ff174420] border border-[#ff1744] text-[#ff1744] text-[11px]">
                    ❌ {error}
                </div>
            )}

            {/* Feature List */}
            {features.length > 0 && (
                <div className="space-y-2">
                    <div className="text-[11px] text-[#808080] uppercase tracking-wide">
                        FEATURES ({features.length})
                    </div>
                    {features.map((f) => (
                        <div
                            key={f.id}
                            className="flex items-center justify-between p-3 bg-[#0a0a0a] border border-[#2a2a2a]"
                        >
                            <div className="flex items-center gap-3">
                                <span className="text-[14px]">{getTemplateIcon(f.template)}</span>
                                <div>
                                    <div className="text-[12px] text-[#e0e0e0] font-mono">{f.name}</div>
                                    <div className="text-[10px] text-[#606060]">
                                        {f.template} | {f.source_table} | {f.windows.map(w => `${w}d`).join(', ')}
                                    </div>
                                </div>
                            </div>
                            <button
                                type="button"
                                onClick={() => removeFeature(f.id)}
                                className="text-[#ff1744] hover:text-[#ff5252] text-[11px]"
                            >
                                ✕
                            </button>
                        </div>
                    ))}

                    {/* Generate Button */}
                    <button
                        type="button"
                        onClick={handleGenerateFeatures}
                        disabled={isGenerating}
                        className="btn-terminal w-full text-[11px] py-3 disabled:opacity-50"
                    >
                        {isGenerating ? '⏳ GENERATING SQL...' : `⚡ GENERATE ${features.length} FEATURE(S)`}
                    </button>
                </div>
            )}

            {/* Generated SQL Preview */}
            {generatedSql && (
                <div className="p-4 bg-[#00c85310] border border-[#00c853]">
                    <div className="text-[12px] text-[#00c853] font-bold mb-2">
                        ✓ {generatedSql.length} FEATURE(S) GENERATED
                    </div>
                    <div className="text-[10px] text-[#808080]">
                        Total columns: {generatedSql.reduce((sum, f) => sum + (f.feature_columns?.length || 0), 0)}
                    </div>
                </div>
            )}
        </motion.div>
    );
}
