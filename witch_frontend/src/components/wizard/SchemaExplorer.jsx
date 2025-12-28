import { useState, useEffect } from 'react';
import api from '../../api/client';

export default function SchemaExplorer({
    sessionId,
    onEntitySelect,
    connectionStatus
}) {
    const [entities, setEntities] = useState([]);
    const [tables, setTables] = useState([]);
    const [selectedEntity, setSelectedEntity] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        if (sessionId) {
            loadSchemaData();
        }
    }, [sessionId]);

    const formatDateRange = (table) => {
        if (table.min_date && table.max_date) {
            return `${table.min_date} - ${table.max_date}`;
        }
        return table.date_range || null;
    };

    const normalizeTable = (table) => ({
        ...table,
        name: table.table_name ?? table.name,
        has_entity: table.has_entity_column ?? table.has_entity,
        date_range: formatDateRange(table),
    });

    const normalizeEntity = (entity) => ({
        ...entity,
        unique_count: entity.total_unique ?? entity.unique_count,
        table_count: Array.isArray(entity.tables) ? entity.tables.length : entity.table_count,
    });

    const loadSchemaData = async () => {
        setLoading(true);
        setError(null);
        try {
            // Fetch tables
            const tablesRes = await api.post('/schema/tables', { session_id: sessionId });
            const tablesData = (tablesRes.data.tables || []).map(normalizeTable);
            setTables(tablesData);

            // Fetch entity columns
            const entitiesRes = await api.post('/schema/entities', { session_id: sessionId });
            const entitiesData = (entitiesRes.data.entities || []).map(normalizeEntity);
            setEntities(entitiesData);
        } catch (err) {
            setError(err.response?.data?.detail || 'Failed to load schema');
        } finally {
            setLoading(false);
        }
    };


    const handleEntitySelect = (entity) => {
        setSelectedEntity(entity);
    };

    const handleContinue = () => {
        if (selectedEntity) {
            onEntitySelect(selectedEntity);
        }
    };

    if (loading) {
        return (
            <div className="flex items-center justify-center h-64">
                <div className="text-[var(--color-terminal-text-dim)] flex items-center gap-2">
                    <span className="animate-pulse">●</span>
                    <span>LOADING SCHEMA...</span>
                </div>
            </div>
        );
    }

    return (
        <div className="p-6 space-y-6">
            {/* Step Header */}
            <div className="terminal-header">
                STEP 1 OF 7: SELECT ENTITY
            </div>

            {/* Connection Status */}
            <div className="terminal-panel p-4">
                <div className="flex items-center justify-between">
                    <div className="flex items-center gap-4">
                        <span className="data-label">DATABASE:</span>
                        <span className="data-value">Connected Database (Postgres)</span>
                    </div>
                    <div className={`flex items-center gap-2 ${connectionStatus === 'connected' ? 'status-online' : 'status-offline'}`}>
                        <span>{connectionStatus === 'connected' ? '🟢' : '🔴'}</span>
                        <span className="text-xs uppercase tracking-wide">
                            {connectionStatus === 'connected' ? 'Connected' : 'Disconnected'}
                        </span>
                    </div>
                </div>
                <div className="mt-2 text-[var(--color-terminal-text-dim)]">
                    TABLES: {tables.length}
                </div>
            </div>

            {/* Error Display */}
            {error && (
                <div className="terminal-panel p-4 border-[var(--color-terminal-red)] bg-[#1a0a0a]">
                    <div className="flex items-center gap-2 text-[var(--color-terminal-red)]">
                        <span>⚠️</span>
                        <span>{error}</span>
                    </div>
                    <button onClick={loadSchemaData} className="btn-terminal mt-3 text-xs">
                        REFRESH
                    </button>
                </div>
            )}

            {/* Entity Selection */}
            <div className="terminal-panel">
                <div className="terminal-header">DETECTED ENTITY COLUMNS</div>
                <div className="p-4">
                    {entities.length === 0 ? (
                        <div className="text-[var(--color-terminal-text-dim)] py-4">
                            No entity columns detected. Expected columns ending in "_id" with high cardinality.
                        </div>
                    ) : (
                        <div className="space-y-2">
                            {entities.map((entity) => (
                                <label
                                    key={entity.column_name}
                                    className={`
                    flex items-center gap-4 p-3 cursor-pointer
                    border transition-colors
                    ${selectedEntity?.column_name === entity.column_name
                                            ? 'border-[var(--color-terminal-orange)] bg-[var(--color-terminal-bg-light)]'
                                            : 'border-transparent hover:bg-[var(--color-terminal-bg-light)]'
                                        }
                  `}
                                    onClick={() => handleEntitySelect(entity)}
                                >
                                    <input
                                        type="radio"
                                        name="entity"
                                        checked={selectedEntity?.column_name === entity.column_name}
                                        onChange={() => handleEntitySelect(entity)}
                                        className="accent-[var(--color-terminal-orange)]"
                                    />
                                    <span className="data-value flex-1">{entity.column_name}</span>
                                    <span className="text-[var(--color-terminal-text-dim)] text-xs">
                                        Found in {entity.table_count || 1} tables
                                    </span>
                                    <span className="text-[var(--color-terminal-cyan)] text-xs">
                                        {entity.unique_count?.toLocaleString() || '—'} unique
                                    </span>
                                </label>
                            ))}
                        </div>
                    )}
                </div>
            </div>

            {/* Tables Overview */}
            <div className="terminal-panel">
                <div className="terminal-header">TABLES OVERVIEW</div>
                <div className="overflow-x-auto">
                    <table className="terminal-table">
                        <thead>
                            <tr>
                                <th>Table</th>
                                <th>Rows</th>
                                <th>Date Range</th>
                                <th>Has Entity</th>
                            </tr>
                        </thead>
                        <tbody>
                            {tables.slice(0, 10).map((table) => (
                                <tr key={table.name}>
                                    <td className="data-value">{table.name}</td>
                                    <td>{table.row_count?.toLocaleString() || '—'}</td>
                                    <td className="text-[var(--color-terminal-text-dim)]">
                                        {table.date_range || '—'}
                                    </td>
                                    <td>
                                        {table.has_entity ? (
                                            <span className="status-online">✓</span>
                                        ) : (
                                            <span className="text-[var(--color-terminal-text-dim)]">—</span>
                                        )}
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>

            {/* Action Button */}
            <div className="flex justify-end">
                <button
                    onClick={handleContinue}
                    disabled={!selectedEntity}
                    className={`btn-terminal-primary px-6 py-2 ${!selectedEntity ? 'opacity-50 cursor-not-allowed' : ''}`}
                >
                    SELECT {selectedEntity?.column_name?.toUpperCase() || 'ENTITY'} →
                </button>
            </div>
        </div>
    );
}
