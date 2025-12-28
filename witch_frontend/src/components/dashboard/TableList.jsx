import { useState, useEffect } from 'react';

export function TableList({ tables, onAuditTable, isAuditing, auditHistory = [] }) {
  const [searchTerm, setSearchTerm] = useState('');
  const [activeTab, setActiveTab] = useState('tables'); // 'tables' or 'history'

  const filteredTables = tables?.filter(table => 
    table.toLowerCase().includes(searchTerm.toLowerCase())
  ) || [];

  // Get health score color
  const getHealthColor = (score) => {
    if (score >= 80) return '#00c853';
    if (score >= 60) return '#ffc107';
    if (score >= 40) return '#ff9800';
    return '#ff1744';
  };

  // Check if a table has been audited
  const getAuditForTable = (tableName) => {
    return auditHistory.find(a => a.table_name === tableName);
  };

  return (
    <div className="bg-[#121212] border border-[#2a2a2a] h-full flex flex-col">
      {/* Header with tabs */}
      <div className="bg-[#1a1a1a] border-b border-[#2a2a2a]">
        <div className="flex">
          <button
            onClick={() => setActiveTab('tables')}
            className={`flex-1 px-3 py-2 text-[10px] font-bold uppercase tracking-wide transition-colors ${
              activeTab === 'tables' 
                ? 'text-[#ff6b00] border-b-2 border-[#ff6b00] bg-[#0a0a0a]' 
                : 'text-[#606060] hover:text-[#909090]'
            }`}
          >
            ◆ TABLES ({tables?.length || 0})
          </button>
          <button
            onClick={() => setActiveTab('history')}
            className={`flex-1 px-3 py-2 text-[10px] font-bold uppercase tracking-wide transition-colors ${
              activeTab === 'history' 
                ? 'text-[#ff6b00] border-b-2 border-[#ff6b00] bg-[#0a0a0a]' 
                : 'text-[#606060] hover:text-[#909090]'
            }`}
          >
            ♥ AUDITS ({auditHistory.length})
          </button>
        </div>
      </div>

      {/* Search (tables tab only) */}
      {activeTab === 'tables' && (
        <div className="p-2 border-b border-[#2a2a2a]">
          <input
            type="text"
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            placeholder="SEARCH TABLES..."
            className="input-terminal w-full text-[11px] py-1.5"
          />
        </div>
      )}

      {/* Content */}
      <div className="flex-1 overflow-y-auto">
        {activeTab === 'tables' ? (
          // Tables List
          filteredTables.length === 0 ? (
            <div className="px-3 py-4 text-center text-[11px] text-[#606060]">
              {searchTerm ? 'NO TABLES MATCH SEARCH' : 'NO TABLES FOUND'}
            </div>
          ) : (
            filteredTables.map((tableName, idx) => {
              const audit = getAuditForTable(tableName);
              
              return (
                <div
                  key={tableName}
                  className="flex items-center justify-between px-3 py-2 border-b border-[#2a2a2a] hover:bg-[#1a1a1a] group"
                >
                  <div className="flex items-center gap-2 flex-1 min-w-0">
                    <span className="text-[10px] text-[#606060] font-mono w-6 flex-shrink-0">
                      {String(idx + 1).padStart(2, '0')}
                    </span>
                    <span className="text-[11px] text-[#e0e0e0] font-mono truncate">
                      {tableName}
                    </span>
                    {/* Show health badge if audited */}
                    {audit && (
                      <span 
                        className="text-[9px] font-bold px-1.5 py-0.5 rounded flex-shrink-0"
                        style={{ 
                          backgroundColor: `${getHealthColor(audit.health_score)}20`,
                          color: getHealthColor(audit.health_score),
                        }}
                      >
                        {audit.health_score}
                      </span>
                    )}
                  </div>
                  
                  <button
                    type="button"
                    onClick={() => onAuditTable(tableName)}
                    disabled={isAuditing}
                    className="opacity-0 group-hover:opacity-100 btn-terminal text-[9px] py-0.5 px-2 flex items-center gap-1 disabled:opacity-50 flex-shrink-0"
                    title="Audit table for ML suitability"
                  >
                    <span>♥</span>
                    <span>{audit ? 'RE-AUDIT' : 'AUDIT'}</span>
                  </button>
                </div>
              );
            })
          )
        ) : (
          // Audit History List
          auditHistory.length === 0 ? (
            <div className="px-3 py-8 text-center">
              <div className="text-[#606060] text-[11px] mb-2">NO AUDITS YET</div>
              <div className="text-[#404040] text-[10px]">
                Switch to TABLES tab and click AUDIT
              </div>
            </div>
          ) : (
            auditHistory.map((audit, idx) => (
              <div
                key={audit.table_name}
                className="px-3 py-3 border-b border-[#2a2a2a] hover:bg-[#1a1a1a]"
              >
                <div className="flex items-center justify-between mb-2">
                  <span className="text-[11px] text-[#e0e0e0] font-mono font-bold">
                    {audit.table_name}
                  </span>
                  <span 
                    className="text-[11px] font-bold font-mono"
                    style={{ color: getHealthColor(audit.health_score) }}
                  >
                    {audit.health_score}/100
                  </span>
                </div>
                
                <div className="flex items-center gap-3 text-[9px]">
                  <span className="text-[#808080]">
                    {audit.row_count?.toLocaleString()} rows
                  </span>
                  <span className="text-[#808080]">
                    {audit.total_columns} cols
                  </span>
                  {audit.critical_count > 0 && (
                    <span className="text-[#ff1744]">
                      {audit.critical_count} CRIT
                    </span>
                  )}
                  {audit.warning_count > 0 && (
                    <span className="text-[#ffc107]">
                      {audit.warning_count} WARN
                    </span>
                  )}
                </div>

                {audit.audited_at && (
                  <div className="text-[8px] text-[#404040] mt-1">
                    {new Date(audit.audited_at).toLocaleString()}
                  </div>
                )}
              </div>
            ))
          )
        )}
      </div>

      {/* Footer hint */}
      <div className="px-3 py-2 bg-[#0a0a0a] border-t border-[#2a2a2a] text-[9px] text-[#606060]">
        {activeTab === 'tables' 
          ? 'HOVER ROW → CLICK [♥ AUDIT] FOR ML HEALTH CHECK'
          : `${auditHistory.length} AUDITS SAVED THIS SESSION`
        }
      </div>
    </div>
  );
}
