import { useState, useEffect } from 'react';
import api from '../../api/client';

export default function TableSelector({
    sessionId,
    selectedEntity,
    onTablesSelect,
    onBack
}) {
    const [tables, setTables] = useState([]);
    const [selectedTables, setSelectedTables] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        loadTables();
    }, [sessionId]);

    const normalizeTable = (table) => ({
        ...table,
        name: table.table_name ?? table.name,
        has_entity: table.has_entity_column ?? table.has_entity,
        date_range: table.min_date && table.max_date
            ? `${table.min_date} - ${table.max_date}`
            : table.date_range || null,
    });

    const loadTables = async () => {
        setLoading(true);
        setError(null);
        try {
            const res = await api.post('/schema/tables', { session_id: sessionId });
            const tablesData = (res.data.tables || []).map(normalizeTable);
            setTables(tablesData);
            // Auto-select tables with entity column
            const withEntity = tablesData.filter(t => t.has_entity);
            setSelectedTables(withEntity.map(t => t.name));
        } catch (err) {
            setError(err.response?.data?.detail || 'Failed to load tables');
        } finally {
            setLoading(false);
        }
    };

    const toggleTable = (tableName) => {
        setSelectedTables(prev =>
            prev.includes(tableName)
                ? prev.filter(t => t !== tableName)
                : [...prev, tableName]
        );
    };

    const selectAll = () => setSelectedTables(tables.map(t => t.name));
    const deselectAll = () => setSelectedTables([]);
    const selectWithEntity = () => setSelectedTables(
        tables.filter(t => t.has_entity).map(t => t.name)
    );

    const handleContinue = () => {
        const selected = tables.filter(t => selectedTables.includes(t.name));
        onTablesSelect(selected);
    };

    if (loading) {
        return (
            <div className="flex items-center justify-center h-64">
                <span className="text-[var(--color-terminal-text-dim)]">LOADING TABLES...</span>
            </div>
        );
    }

    return (
        <div className="p-6 space-y-6">
            {/* Header */}
            <div className="terminal-header">
                STEP 2 OF 7: SELECT TABLES
            </div>

            {/* Entity Info */}
            <div className="terminal-panel p-4 flex items-center justify-between">
                <div>
                    <span className="data-label mr-2">ENTITY:</span>
                    <span className="data-value-highlight">{selectedEntity?.column_name}</span>
                </div>
                <div className="text-[var(--color-terminal-text-dim)] text-xs">
                    {selectedEntity?.unique_count?.toLocaleString()} unique values
                </div>
            </div>

            {/* Error */}
            {error && (
                <div className="terminal-panel p-4 border-[var(--color-terminal-red)] bg-[#1a0a0a]">
                    <div className="flex items-center gap-2 text-[var(--color-terminal-red)]">
                        <span>?s??,?</span>
                        <span>{error}</span>
                    </div>
                    <button onClick={loadTables} className="btn-terminal mt-3 text-xs">
                        REFRESH
                    </button>
                </div>
            )}

            {/* Quick Actions */}
            <div className="flex gap-2">
                <button onClick={selectAll} className="btn-terminal text-xs">SELECT ALL</button>
                <button onClick={deselectAll} className="btn-terminal text-xs">DESELECT ALL</button>
                <button onClick={selectWithEntity} className="btn-terminal text-xs">
                    SELECT ONLY WITH {selectedEntity?.column_name?.toUpperCase()}
                </button>
            </div>

            {/* Tables List */}
            <div className="terminal-panel">
                <div className="max-h-96 overflow-y-auto">
                    {tables.map((table) => (
                        <label
                            key={table.name}
                            className={`
                flex items-start gap-4 p-4 cursor-pointer border-b border-[var(--color-terminal-border)]
                hover:bg-[var(--color-terminal-bg-light)] transition-colors
                ${selectedTables.includes(table.name) ? 'bg-[var(--color-terminal-bg-light)]' : ''}
              `}
                        >
                            <input
                                type="checkbox"
                                checked={selectedTables.includes(table.name)}
                                onChange={() => toggleTable(table.name)}
                                className="mt-1 accent-[var(--color-terminal-orange)]"
                            />
                            <div className="flex-1">
                                <div className="flex items-center gap-3">
                                    <span className="data-value">{table.name}</span>
                                    {table.has_entity && (
                                        <span className="text-[var(--color-terminal-green)] text-xs">
                                            ?o" {selectedEntity?.column_name}
                                        </span>
                                    )}
                                </div>
                                <div className="flex gap-6 mt-1 text-xs text-[var(--color-terminal-text-dim)]">
                                    <span>{table.row_count?.toLocaleString() || '??"'} rows</span>
                                    <span>{table.column_count?.toLocaleString() || '??"'} cols</span>
                                    {table.date_range && <span>{table.date_range}</span>}
                                </div>
                            </div>
                        </label>
                    ))}
                </div>
            </div>

            {/* Summary */}
            <div className="terminal-panel p-4 flex items-center justify-between">
                <div>
                    <span className="data-label mr-2">SELECTED:</span>
                    <span className="data-value">{selectedTables.length} tables</span>
                </div>
            </div>

            {/* Navigation */}
            <div className="flex justify-between">
                <button onClick={onBack} className="btn-terminal">
                    ?+? BACK
                </button>
                <button
                    onClick={handleContinue}
                    disabled={selectedTables.length === 0}
                    className={`btn-terminal-primary px-6 ${selectedTables.length === 0 ? 'opacity-50 cursor-not-allowed' : ''}`}
                >
                    CONTINUE ?+'
                </button>
            </div>
        </div>
    );
}
