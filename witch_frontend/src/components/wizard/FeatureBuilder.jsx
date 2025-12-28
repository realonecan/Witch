import { useEffect, useMemo, useState } from 'react';
import api from '../../api/client';

const AGGREGATION_OPTIONS = {
    numeric: ['SUM', 'AVG', 'MIN', 'MAX', 'STDDEV'],
    categorical: ['COUNT_DISTINCT', 'MODE'],
    datetime: ['RECENCY', 'FREQUENCY'],
    boolean: ['SUM', 'PCT_TRUE'],
};

const WINDOW_OPTIONS = [7, 14, 30, 60, 90];

const AGG_TEMPLATE_MAP = {
    SUM: 'rolling_sum',
    AVG: 'rolling_avg',
    MIN: 'rolling_min',
    MAX: 'rolling_max',
    STDDEV: 'rolling_stddev',
    COUNT_DISTINCT: 'distinct_count',
    MODE: 'mode',
    RECENCY: 'recency',
    FREQUENCY: 'rolling_count',
    PCT_TRUE: 'pct_true',
};

const TEMPLATE_REQUIRES_WINDOW = new Set([
    'rolling_count',
    'rolling_sum',
    'rolling_avg',
    'rolling_min',
    'rolling_max',
    'rolling_stddev',
    'distinct_count',
    'mode',
    'pct_true',
]);

const TEMPLATE_REQUIRES_VALUE = new Set([
    'rolling_sum',
    'rolling_avg',
    'rolling_min',
    'rolling_max',
    'rolling_stddev',
    'distinct_count',
    'mode',
    'pct_true',
]);

const isDateLike = (type = '') => {
    const lower = type.toLowerCase();
    return lower.includes('date') || lower.includes('time');
};

const toSafeKey = (value) => {
    let safe = value.toLowerCase().replace(/[^a-z0-9_]/g, '_');
    safe = safe.replace(/_+/g, '_').replace(/^_+|_+$/g, '');
    if (!safe) {
        safe = 'feature';
    }
    if (/^[0-9]/.test(safe)) {
        safe = `f_${safe}`;
    }
    return safe;
};

export default function FeatureBuilder({
    sessionId,
    selectedTables,
    selectedEntity,
    grainDefinition,
    onFeaturesDefine,
    onBack
}) {
    const [sourceTable, setSourceTable] = useState('');
    const [joinColumn, setJoinColumn] = useState('');
    const [timeColumn, setTimeColumn] = useState('');
    const [columns, setColumns] = useState([]);
    const [selectedFeatures, setSelectedFeatures] = useState({});
    const [selectedWindows, setSelectedWindows] = useState({ 30: true });
    const [templates, setTemplates] = useState([]);
    const [generatedFeatures, setGeneratedFeatures] = useState([]);
    const [loading, setLoading] = useState(false);
    const [generationError, setGenerationError] = useState(null);

    const tableOptions = useMemo(() => (selectedTables || []).map((t) => t.name), [selectedTables]);

    useEffect(() => {
        loadTemplates();
    }, []);

    useEffect(() => {
        if (!sourceTable && tableOptions.length) {
            setSourceTable(tableOptions[0]);
        }
    }, [sourceTable, tableOptions]);

    useEffect(() => {
        if (sourceTable) {
            loadColumns();
        }
    }, [sourceTable]);

    const loadTemplates = async () => {
        try {
            const res = await api.get('/feature/templates');
            setTemplates(res.data.templates || []);
        } catch (err) {
            setTemplates([]);
        }
    };

    const loadColumns = async () => {
        setLoading(true);
        setGenerationError(null);
        try {
            const res = await api.post('/table-columns', {
                session_id: sessionId,
                table_name: sourceTable,
            });
            const cols = res.data.columns || [];
            setColumns(cols);

            const initial = {};
            cols.forEach(col => {
                initial[col.name] = { aggs: [], type: col.type };
            });
            setSelectedFeatures(initial);

            const entityId = grainDefinition?.entity_id_column || selectedEntity?.column_name;
            if (entityId && cols.some((col) => col.name === entityId)) {
                setJoinColumn(entityId);
            } else if (cols.length) {
                setJoinColumn(cols[0].name);
            }

            const dateCol = cols.find((col) => isDateLike(col.type));
            if (dateCol) {
                setTimeColumn(dateCol.name);
            } else if (cols.length) {
                setTimeColumn(cols[0].name);
            }
        } catch (err) {
            setGenerationError(err.response?.data?.detail || 'Failed to load columns');
        } finally {
            setLoading(false);
        }
    };

    const toggleAgg = (colName, agg) => {
        setSelectedFeatures(prev => {
            const current = prev[colName]?.aggs || [];
            const newAggs = current.includes(agg)
                ? current.filter(a => a !== agg)
                : [...current, agg];
            return {
                ...prev,
                [colName]: { ...prev[colName], aggs: newAggs }
            };
        });
    };

    const toggleWindow = (days) => {
        setSelectedWindows(prev => ({
            ...prev,
            [days]: !prev[days]
        }));
    };

    const getAggOptions = (colType) => {
        const lower = colType?.toLowerCase() || '';
        if (lower.includes('int') || lower.includes('float') || lower.includes('numeric') || lower.includes('decimal')) {
            return AGGREGATION_OPTIONS.numeric;
        }
        if (lower.includes('bool')) {
            return AGGREGATION_OPTIONS.boolean;
        }
        if (lower.includes('date') || lower.includes('time')) {
            return AGGREGATION_OPTIONS.datetime;
        }
        return AGGREGATION_OPTIONS.categorical;
    };

    const selectedWindowDays = Object.entries(selectedWindows)
        .filter(([_, v]) => v)
        .map(([k]) => Number(k));

    const buildSelections = () => {
        const selections = [];
        Object.entries(selectedFeatures).forEach(([column, meta]) => {
            meta.aggs.forEach((agg) => {
                const templateType = AGG_TEMPLATE_MAP[agg];
                if (!templateType) {
                    return;
                }
                const windows = TEMPLATE_REQUIRES_WINDOW.has(templateType) ? selectedWindowDays : [null];
                windows.forEach((windowDays) => {
                    selections.push({
                        column,
                        agg,
                        templateType,
                        windowDays,
                        valueColumn: TEMPLATE_REQUIRES_VALUE.has(templateType) ? column : null,
                    });
                });
            });
        });
        return selections;
    };

    const countFeatures = () => buildSelections().length;

    const handleContinue = async () => {
        setGenerationError(null);
        if (!grainDefinition) {
            setGenerationError('Define grain before generating features.');
            return;
        }
        if (!sourceTable || !joinColumn || !timeColumn) {
            setGenerationError('Select source table, join column, and time column.');
            return;
        }

        const selections = buildSelections();
        if (!selections.length) {
            setGenerationError('Select at least one feature to generate.');
            return;
        }

        setLoading(true);
        const generated = [];
        const usedKeys = new Set();

        try {
            for (const selection of selections) {
                const windowSuffix = selection.windowDays ? `${selection.windowDays}d` : 'recency';
                const baseKey = toSafeKey(`${selection.column}_${selection.agg.toLowerCase()}_${windowSuffix}`);
                let featureKey = baseKey;
                let counter = 1;
                while (usedKeys.has(featureKey)) {
                    counter += 1;
                    featureKey = `${baseKey}_${counter}`;
                }
                usedKeys.add(featureKey);

                const featureName = selection.windowDays
                    ? `${selection.agg} ${selection.column} (${selection.windowDays}d)`
                    : `${selection.agg} ${selection.column}`;

                const res = await api.post('/feature/generate', {
                    session_id: sessionId,
                    feature_name: featureName,
                    feature_key: featureKey,
                    template_type: selection.templateType,
                    source_table: sourceTable,
                    join_column: joinColumn,
                    time_column: timeColumn,
                    value_column: selection.valueColumn,
                    window_days: selection.windowDays || 30,
                    source_schema: 'public',
                    entity_table: grainDefinition.entity_table,
                    entity_id_column: grainDefinition.entity_id_column,
                    observation_date_column: grainDefinition.observation_date_column,
                    grain_schema: grainDefinition.schema || 'public',
                });

                generated.push({
                    name: featureName,
                    sql: res.data.sql,
                    feature_columns: res.data.feature_columns,
                    source_table: sourceTable,
                    max_source_time_column: res.data.max_source_time_column,
                    window_description: res.data.window_description,
                });
            }

            setGeneratedFeatures(generated);
            onFeaturesDefine({
                feature_sqls: generated,
                feature_count: generated.length,
                source_table: sourceTable,
                join_column: joinColumn,
                time_column: timeColumn,
            });
        } catch (err) {
            setGenerationError(err.response?.data?.detail || 'Failed to generate features');
        } finally {
            setLoading(false);
        }
    };

    const featureCount = countFeatures();

    return (
        <div className="p-6 space-y-6">
            {/* Header */}
            <div className="terminal-header">
                STEP 5 OF 7: BUILD FEATURES
            </div>

            {/* Source Table */}
            <div className="terminal-panel p-4 space-y-4">
                <div className="flex items-center gap-4">
                    <span className="data-label">SOURCE TABLE:</span>
                    <select
                        value={sourceTable}
                        onChange={(e) => setSourceTable(e.target.value)}
                        className="input-terminal flex-1"
                    >
                        <option value="">Select table...</option>
                        {tableOptions.map((table) => (
                            <option key={table} value={table}>{table}</option>
                        ))}
                    </select>
                </div>

                <div className="grid grid-cols-2 gap-4">
                    <div>
                        <label className="data-label block mb-1">Join Column</label>
                        <select
                            value={joinColumn}
                            onChange={(e) => setJoinColumn(e.target.value)}
                            className="input-terminal w-full"
                        >
                            <option value="">Select column...</option>
                            {columns.map((col) => (
                                <option key={col.name} value={col.name}>{col.name}</option>
                            ))}
                        </select>
                    </div>
                    <div>
                        <label className="data-label block mb-1">Time Column</label>
                        <select
                            value={timeColumn}
                            onChange={(e) => setTimeColumn(e.target.value)}
                            className="input-terminal w-full"
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

            {/* Window Selection */}
            <div className="terminal-panel p-4">
                <div className="data-label mb-3">TIME WINDOWS</div>
                <div className="flex gap-4">
                    {WINDOW_OPTIONS.map((days) => (
                        <label key={days} className="flex items-center gap-2 cursor-pointer">
                            <input
                                type="checkbox"
                                checked={selectedWindows[days] || false}
                                onChange={() => toggleWindow(days)}
                                className="accent-[var(--color-terminal-orange)]"
                            />
                            <span className={selectedWindows[days] ? 'data-value-highlight' : 'data-value'}>
                                {days}d
                            </span>
                        </label>
                    ))}
                </div>
            </div>

            {/* Aggregation Matrix */}
            {columns.length > 0 && (
                <div className="terminal-panel">
                    <div className="terminal-header">CONFIGURE AGGREGATIONS</div>
                    <div className="overflow-x-auto">
                        <table className="terminal-table">
                            <thead>
                                <tr>
                                    <th>Column</th>
                                    <th>Type</th>
                                    <th>Aggregations</th>
                                </tr>
                            </thead>
                            <tbody>
                                {columns.filter(c => c.name !== selectedEntity?.column_name).map((col) => {
                                    const aggOptions = getAggOptions(col.type);
                                    const selected = selectedFeatures[col.name]?.aggs || [];

                                    return (
                                        <tr key={col.name}>
                                            <td className="data-value">{col.name}</td>
                                            <td className="text-[var(--color-terminal-text-dim)] text-xs">{col.type}</td>
                                            <td>
                                                <div className="flex flex-wrap gap-2">
                                                    {aggOptions.map((agg) => (
                                                        <label
                                                            key={agg}
                                                            className={`
                                px-2 py-1 text-xs cursor-pointer border
                                ${selected.includes(agg)
                                                                    ? 'border-[var(--color-terminal-orange)] bg-[var(--color-terminal-orange)] text-black'
                                                                    : 'border-[var(--color-terminal-border)] hover:border-[var(--color-terminal-orange)]'
                                                                }
                              `}
                                                            onClick={() => toggleAgg(col.name, agg)}
                                                        >
                                                            {agg}
                                                        </label>
                                                    ))}
                                                </div>
                                            </td>
                                        </tr>
                                    );
                                })}
                            </tbody>
                        </table>
                    </div>
                </div>
            )}

            {/* Feature Count */}
            <div className="terminal-panel p-4">
                <span className="data-label">FEATURES:</span>
                <span className="data-value-highlight ml-2">{featureCount}</span>
                <span className="text-[var(--color-terminal-text-dim)] ml-4 text-xs">
                    ({Object.values(selectedFeatures).filter(v => v.aggs.length > 0).length} columns x {selectedWindowDays.length} windows)
                </span>
            </div>

            {templates.length > 0 && (
                <div className="terminal-panel p-4">
                    <div className="data-label mb-2">TEMPLATES LOADED</div>
                    <div className="text-xs text-[var(--color-terminal-text-dim)]">
                        {templates.length} templates available from backend.
                    </div>
                </div>
            )}

            {generatedFeatures.length > 0 && (
                <div className="terminal-panel">
                    <div className="terminal-header">GENERATED FEATURES</div>
                    <div className="p-4 space-y-2">
                        {generatedFeatures.map((feat, idx) => (
                            <div key={idx} className="text-sm">
                                <span className="data-value">{feat.name}</span>
                                <span className="text-[var(--color-terminal-text-dim)] ml-2">
                                    ({feat.feature_columns.join(', ')})
                                </span>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {generationError && (
                <div className="terminal-panel p-4 border-[var(--color-terminal-red)] bg-[#1a0a0a] text-[var(--color-terminal-red)] text-sm">
                    ?s??,? {generationError}
                </div>
            )}

            {/* Navigation */}
            <div className="flex justify-between">
                <button onClick={onBack} className="btn-terminal">
                    ?+? BACK
                </button>
                <button
                    onClick={handleContinue}
                    disabled={loading || featureCount === 0}
                    className={`btn-terminal-primary px-6 ${loading || featureCount === 0 ? 'opacity-50 cursor-not-allowed' : ''}`}
                >
                    {loading ? 'GENERATING...' : 'CONTINUE'} ?+'
                </button>
            </div>
        </div>
    );
}
