import { useState } from 'react';

export function DataHealthDashboard({ auditData, onClose }) {
  const [expandedColumn, setExpandedColumn] = useState(null);

  if (!auditData) return null;

  const { table_name, row_count, columns, alerts, summary } = auditData;

  // Filter alerts by level
  const criticalAlerts = alerts?.filter(a => a.level === 'critical') || [];
  const warningAlerts = alerts?.filter(a => a.level === 'warning') || [];
  const errorAlerts = alerts?.filter(a => a.level === 'error') || [];

  // Health score color
  const getHealthColor = (score) => {
    if (score >= 80) return '#00c853'; // Green
    if (score >= 60) return '#ffc107'; // Yellow
    if (score >= 40) return '#ff9800'; // Orange
    return '#ff1744'; // Red
  };

  const healthScore = summary?.health_score || 0;
  const healthColor = getHealthColor(healthScore);

  // Format percentage for display
  const formatPercent = (val) => {
    if (val === null || val === undefined) return '—';
    return `${(val * 100).toFixed(1)}%`;
  };

  // Format number
  const formatNumber = (val) => {
    if (val === null || val === undefined) return '—';
    if (typeof val === 'number') {
      return val.toLocaleString();
    }
    return val;
  };

  return (
    <div className="fixed inset-0 bg-black/80 flex items-center justify-center z-[100] p-4 overflow-auto">
      <div className="w-full max-w-5xl max-h-[90vh] bg-[#0a0a0a] border border-[#2a2a2a] flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between terminal-header">
          <div className="flex items-center gap-4">
            <span>◆ DATA QUALITY AUDIT</span>
            <span className="text-black/70">|</span>
            <span className="font-mono">{table_name?.toUpperCase()}</span>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="text-black hover:bg-[#cc5500] px-2 py-0.5 text-[10px] font-bold"
          >
            [X] CLOSE
          </button>
        </div>

        {/* Content - Scrollable */}
        <div className="flex-1 overflow-auto p-4">
          {/* Top Stats Row */}
          <div className="grid grid-cols-4 gap-4 mb-6">
            {/* Health Score */}
            <div className="bg-[#121212] border border-[#2a2a2a] p-4">
              <div className="text-[10px] text-[#606060] uppercase tracking-wide mb-2">
                HEALTH SCORE
              </div>
              <div className="flex items-center gap-3">
                <div 
                  className="text-4xl font-bold font-mono"
                  style={{ color: healthColor }}
                >
                  {healthScore}
                </div>
                <div className="flex-1">
                  <div className="h-2 bg-[#2a2a2a] rounded-full overflow-hidden">
                    <div 
                      className="h-full transition-all duration-500"
                      style={{ 
                        width: `${healthScore}%`,
                        backgroundColor: healthColor,
                      }}
                    />
                  </div>
                  <div className="text-[9px] text-[#606060] mt-1">
                    {healthScore >= 80 ? 'EXCELLENT' : healthScore >= 60 ? 'GOOD' : healthScore >= 40 ? 'FAIR' : 'POOR'}
                  </div>
                </div>
              </div>
            </div>

            {/* Row Count */}
            <div className="bg-[#121212] border border-[#2a2a2a] p-4">
              <div className="text-[10px] text-[#606060] uppercase tracking-wide mb-2">
                TOTAL ROWS
              </div>
              <div className="text-2xl font-bold font-mono text-[#00c853]">
                {formatNumber(row_count)}
              </div>
            </div>

            {/* Column Count */}
            <div className="bg-[#121212] border border-[#2a2a2a] p-4">
              <div className="text-[10px] text-[#606060] uppercase tracking-wide mb-2">
                COLUMNS
              </div>
              <div className="text-2xl font-bold font-mono text-[#2196f3]">
                {summary?.total_columns || 0}
              </div>
              <div className="text-[9px] text-[#606060] mt-1">
                {summary?.numeric_columns || 0} NUM · {summary?.text_columns || 0} TXT · {summary?.date_columns || 0} DATE
              </div>
            </div>

            {/* Alerts Count */}
            <div className="bg-[#121212] border border-[#2a2a2a] p-4">
              <div className="text-[10px] text-[#606060] uppercase tracking-wide mb-2">
                ISSUES FOUND
              </div>
              <div className="flex items-center gap-4">
                {criticalAlerts.length > 0 && (
                  <div className="flex items-center gap-1">
                    <span className="text-xl font-bold font-mono text-[#ff1744]">
                      {criticalAlerts.length}
                    </span>
                    <span className="text-[9px] text-[#ff1744]">CRIT</span>
                  </div>
                )}
                {warningAlerts.length > 0 && (
                  <div className="flex items-center gap-1">
                    <span className="text-xl font-bold font-mono text-[#ffc107]">
                      {warningAlerts.length}
                    </span>
                    <span className="text-[9px] text-[#ffc107]">WARN</span>
                  </div>
                )}
                {criticalAlerts.length === 0 && warningAlerts.length === 0 && (
                  <div className="text-xl font-bold font-mono text-[#00c853]">
                    0
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* Critical Alerts */}
          {criticalAlerts.length > 0 && (
            <div className="mb-4">
              <div className="bg-[#ff1744]/10 border border-[#ff1744]/30 p-3">
                <div className="flex items-center gap-2 mb-2">
                  <span className="text-[#ff1744] text-lg">⚠</span>
                  <span className="text-[11px] font-bold text-[#ff1744] uppercase tracking-wide">
                    CRITICAL ISSUES ({criticalAlerts.length})
                  </span>
                </div>
                <div className="space-y-1">
                  {criticalAlerts.map((alert, idx) => (
                    <div key={idx} className="flex items-center gap-3 text-[12px]">
                      <span className="text-[#ff1744] font-mono text-[10px] bg-[#ff1744]/20 px-2 py-0.5">
                        {alert.column || 'TABLE'}
                      </span>
                      <span className="text-[#e0e0e0]">{alert.message}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* Warning Alerts */}
          {warningAlerts.length > 0 && (
            <div className="mb-4">
              <div className="bg-[#ffc107]/10 border border-[#ffc107]/30 p-3">
                <div className="flex items-center gap-2 mb-2">
                  <span className="text-[#ffc107] text-lg">⚡</span>
                  <span className="text-[11px] font-bold text-[#ffc107] uppercase tracking-wide">
                    WARNINGS ({warningAlerts.length})
                  </span>
                </div>
                <div className="space-y-1">
                  {warningAlerts.map((alert, idx) => (
                    <div key={idx} className="flex items-center gap-3 text-[12px]">
                      <span className="text-[#ffc107] font-mono text-[10px] bg-[#ffc107]/20 px-2 py-0.5">
                        {alert.column || 'TABLE'}
                      </span>
                      <span className="text-[#e0e0e0]">{alert.message}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* Error Alerts */}
          {errorAlerts.length > 0 && (
            <div className="mb-4">
              <div className="bg-[#9c27b0]/10 border border-[#9c27b0]/30 p-3">
                <div className="flex items-center gap-2 mb-2">
                  <span className="text-[#9c27b0] text-lg">✕</span>
                  <span className="text-[11px] font-bold text-[#9c27b0] uppercase tracking-wide">
                    ERRORS ({errorAlerts.length})
                  </span>
                </div>
                <div className="space-y-1">
                  {errorAlerts.map((alert, idx) => (
                    <div key={idx} className="text-[12px] text-[#e0e0e0]">
                      {alert.message}
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* Column X-Ray Table */}
          <div className="bg-[#121212] border border-[#2a2a2a]">
            <div className="bg-[#1a1a1a] px-3 py-2 border-b border-[#2a2a2a]">
              <span className="text-[11px] font-bold text-[#ff6b00] uppercase tracking-wide">
                ◆ COLUMN X-RAY
              </span>
            </div>
            
            <div className="overflow-x-auto">
              <table className="w-full text-[11px]">
                <thead>
                  <tr className="bg-[#0a0a0a]">
                    <th className="text-left px-3 py-2 text-[#ff6b00] font-bold uppercase tracking-wide border-b border-[#2a2a2a]">
                      COLUMN
                    </th>
                    <th className="text-left px-3 py-2 text-[#ff6b00] font-bold uppercase tracking-wide border-b border-[#2a2a2a]">
                      TYPE
                    </th>
                    <th className="text-left px-3 py-2 text-[#ff6b00] font-bold uppercase tracking-wide border-b border-[#2a2a2a] w-32">
                      NULL %
                    </th>
                    <th className="text-right px-3 py-2 text-[#ff6b00] font-bold uppercase tracking-wide border-b border-[#2a2a2a]">
                      DISTINCT
                    </th>
                    <th className="text-right px-3 py-2 text-[#ff6b00] font-bold uppercase tracking-wide border-b border-[#2a2a2a]">
                      MIN
                    </th>
                    <th className="text-right px-3 py-2 text-[#ff6b00] font-bold uppercase tracking-wide border-b border-[#2a2a2a]">
                      MAX
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(columns || {}).map(([colName, stats], idx) => {
                    const nullPct = stats.null_percentage || 0;
                    const nullBarColor = nullPct > 0.4 ? '#ff1744' : nullPct > 0.2 ? '#ffc107' : '#00c853';
                    
                    return (
                      <tr 
                        key={colName}
                        className="hover:bg-[#1a1a1a] cursor-pointer"
                        onClick={() => setExpandedColumn(expandedColumn === colName ? null : colName)}
                      >
                        <td className="px-3 py-2 border-b border-[#2a2a2a]">
                          <span className="font-mono text-[#e0e0e0]">{colName}</span>
                          {stats.distinct_count <= 1 && (
                            <span className="ml-2 text-[8px] bg-[#ff1744] text-white px-1 py-0.5">
                              ZERO VAR
                            </span>
                          )}
                        </td>
                        <td className="px-3 py-2 border-b border-[#2a2a2a]">
                          <span className={`text-[10px] px-1.5 py-0.5 ${
                            stats.type === 'numeric' ? 'bg-[#2196f3]/20 text-[#2196f3]' :
                            stats.type === 'date' ? 'bg-[#9c27b0]/20 text-[#9c27b0]' :
                            stats.type === 'text' ? 'bg-[#00bcd4]/20 text-[#00bcd4]' :
                            'bg-[#606060]/20 text-[#909090]'
                          }`}>
                            {stats.type?.toUpperCase() || 'OTHER'}
                          </span>
                        </td>
                        <td className="px-3 py-2 border-b border-[#2a2a2a]">
                          <div className="flex items-center gap-2">
                            <div className="flex-1 h-1.5 bg-[#2a2a2a] rounded-full overflow-hidden">
                              <div 
                                className="h-full"
                                style={{ 
                                  width: `${Math.min(nullPct * 100, 100)}%`,
                                  backgroundColor: nullBarColor,
                                }}
                              />
                            </div>
                            <span className="font-mono text-[10px] w-12 text-right" style={{ color: nullBarColor }}>
                              {formatPercent(nullPct)}
                            </span>
                          </div>
                        </td>
                        <td className="px-3 py-2 border-b border-[#2a2a2a] text-right font-mono text-[#e0e0e0]">
                          {formatNumber(stats.distinct_count)}
                        </td>
                        <td className="px-3 py-2 border-b border-[#2a2a2a] text-right font-mono text-[#e0e0e0]">
                          {stats.type === 'date' 
                            ? (stats.min_date?.split(' ')[0] || '—')
                            : formatNumber(stats.min)}
                        </td>
                        <td className="px-3 py-2 border-b border-[#2a2a2a] text-right font-mono text-[#e0e0e0]">
                          {stats.type === 'date' 
                            ? (stats.max_date?.split(' ')[0] || '—')
                            : formatNumber(stats.max)}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>

          {/* Expanded Column Details */}
          {expandedColumn && columns[expandedColumn] && (
            <div className="mt-4 bg-[#121212] border border-[#2a2a2a] p-4">
              <div className="text-[11px] font-bold text-[#ff6b00] uppercase tracking-wide mb-3">
                ◆ DETAILS: {expandedColumn}
              </div>
              <div className="grid grid-cols-4 gap-4 text-[11px]">
                {Object.entries(columns[expandedColumn]).map(([key, value]) => (
                  <div key={key}>
                    <span className="text-[#606060] uppercase">{key.replace(/_/g, ' ')}: </span>
                    <span className="font-mono text-[#e0e0e0]">
                      {typeof value === 'number' ? formatNumber(value) : String(value ?? '—')}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-4 py-2 bg-[#121212] border-t border-[#2a2a2a] text-[10px] text-[#606060]">
          <span>AUDIT COMPLETE</span>
          <span className="mx-2">|</span>
          <span>CLICK ROW FOR DETAILS</span>
          <span className="mx-2">|</span>
          <span>HEALTH SCORE: <span style={{ color: healthColor }}>{healthScore}/100</span></span>
        </div>
      </div>
    </div>
  );
}

