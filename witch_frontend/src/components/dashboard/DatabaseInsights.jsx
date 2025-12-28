import { useEffect, useMemo, useRef, useState } from 'react';
import api from '../../api/client';

const formatNumber = (value) => {
  if (value === null || value === undefined) return '-';
  if (typeof value === 'number') return value.toLocaleString();
  return value;
};

const formatPercent = (value) => {
  if (value === null || value === undefined) return '-';
  return `${(value * 100).toFixed(1)}%`;
};

const formatDate = (value) => {
  if (!value) return '-';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toISOString().split('T')[0];
};

const formatCompactNumber = (value) => {
  if (value === null || value === undefined || Number.isNaN(value)) return '-';
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)}B`;
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return Number.isInteger(value) ? String(value) : value.toFixed(2);
};

const truncateLabel = (value, max = 16) => {
  if (value === null || value === undefined) return '-';
  const text = String(value);
  if (text.length <= max) return text;
  return `${text.slice(0, Math.max(0, max - 3))}...`;
};

const formatPercentValue = (value) => {
  if (value === null || value === undefined || Number.isNaN(value)) return '-';
  return `${value.toFixed(1)}%`;
};

const getHealthColor = (score) => {
  if (score >= 80) return '#00c853';
  if (score >= 60) return '#ffc107';
  if (score >= 40) return '#ff9800';
  return '#ff1744';
};

const getNullColor = (pct) => {
  if (pct >= 0.5) return '#ff1744';
  if (pct >= 0.25) return '#ff9800';
  if (pct >= 0.1) return '#ffc107';
  return '#00c853';
};

const getStatusColor = (status) => {
  if (status === 'RUNNING') return '#ffc107';
  if (status === 'DONE' || status === 'READY') return '#00c853';
  if (status === 'ERROR') return '#ff1744';
  if (status === 'QUEUED') return '#8a8a8a';
  return '#606060';
};

const buildHistogramSeries = (data) => {
  const bins = data?.bins || 12;
  const histogram = data?.histogram || [];
  const bucketMap = histogram.reduce((acc, item) => {
    acc[item.bucket] = item.count;
    return acc;
  }, {});
  const counts = Array.from({ length: bins }, (_, idx) => bucketMap[idx + 1] || 0);
  const maxCount = Math.max(1, ...counts);
  const nonZero = counts.filter((count) => count > 0);
  const minNonZero = nonZero.length ? Math.min(...nonZero) : 0;
  const useLog = maxCount >= 1000 && minNonZero > 0 && maxCount / minNonZero > 200;
  const scaled = counts.map((count) => (useLog ? Math.log10(count + 1) : count));
  const scaleMax = Math.max(1, ...scaled);
  return {
    bins,
    counts,
    maxCount,
    scaleMax,
    useLog,
  };
};

export function DatabaseInsights({ dbSessionId, dbName, auditTable, onShowAudit }) {
  const [schemaTables, setSchemaTables] = useState([]);
  const [entities, setEntities] = useState([]);
  const [auditResults, setAuditResults] = useState({});
  const [auditErrors, setAuditErrors] = useState({});
  const [valueDistributions, setValueDistributions] = useState({});
  const [histograms, setHistograms] = useState({});
  const [distributionErrors, setDistributionErrors] = useState({});
  const [histogramErrors, setHistogramErrors] = useState({});
  const [isLoadingDistributions, setIsLoadingDistributions] = useState(false);
  const [isLoadingHistograms, setIsLoadingHistograms] = useState(false);
  const [auditProgress, setAuditProgress] = useState({ total: 0, completed: 0, current: '' });
  const [isAuditingAll, setIsAuditingAll] = useState(false);
  const [isLoadingTables, setIsLoadingTables] = useState(false);
  const [isLoadingEntities, setIsLoadingEntities] = useState(false);
  const [schemaError, setSchemaError] = useState(null);
  const [entitiesError, setEntitiesError] = useState(null);
  const [lastAuditAt, setLastAuditAt] = useState(null);
  const [scanLog, setScanLog] = useState([]);

  const cancelRef = useRef(false);
  const runRef = useRef(0);
  const loadedDistributionKeys = useRef(new Set());
  const loadedHistogramKeys = useRef(new Set());

  const pushLog = (message) => {
    const stamp = new Date().toLocaleTimeString('en-US', { hour12: false });
    setScanLog((prev) => {
      const next = [{ time: stamp, message }, ...prev];
      return next.slice(0, 8);
    });
  };

  useEffect(() => {
    if (!dbSessionId) return;
    runRef.current += 1;
    const runId = runRef.current;
    cancelRef.current = false;
    setSchemaError(null);
    setSchemaTables([]);
    setEntities([]);
    setAuditResults({});
    setAuditErrors({});
    setValueDistributions({});
    setHistograms({});
    setDistributionErrors({});
    setHistogramErrors({});
    setIsLoadingDistributions(false);
    setIsLoadingHistograms(false);
    setIsLoadingTables(false);
    setIsLoadingEntities(false);
    setAuditProgress({ total: 0, completed: 0, current: '' });
    setLastAuditAt(null);
    setEntitiesError(null);
    setScanLog([]);
    loadedDistributionKeys.current = new Set();
    loadedHistogramKeys.current = new Set();
    loadSchemaAndAudit(runId);
  }, [dbSessionId]);

  const loadSchemaAndAudit = async (runId) => {
    setIsLoadingTables(true);
    pushLog('Schema tables: loading');
    try {
      const tablesRes = await api.post('/schema/tables', {
        session_id: dbSessionId,
        schema: 'public',
      });
      if (runId !== runRef.current) return;
      const tables = tablesRes.data?.tables || [];
      setSchemaTables(tables);
      setIsLoadingTables(false);

      pushLog(`Schema tables: loaded ${tables.length}`);
      pushLog('Audit scan: started');
      runAuditAll(tables, runId);

      setIsLoadingEntities(true);
      setEntitiesError(null);
      pushLog('Entity scan: started (pg_stats)');
      try {
        const entitiesRes = await api.post('/schema/entities', {
          session_id: dbSessionId,
          schema: 'public',
        });
        if (runId !== runRef.current) return;
        setEntities(entitiesRes.data?.entities || []);
        pushLog(`Entity scan: loaded ${entitiesRes.data?.entities?.length || 0}`);
      } catch (err) {
        if (runId !== runRef.current) return;
        setEntitiesError(err.response?.data?.detail || 'Entity scan failed');
        pushLog('Entity scan: error');
      } finally {
        if (runId === runRef.current) {
          setIsLoadingEntities(false);
        }
      }
    } catch (err) {
      if (runId !== runRef.current) return;
      setSchemaError(err.response?.data?.detail || 'Failed to load schema data');
      setIsLoadingTables(false);
      setIsLoadingEntities(false);
      pushLog('Schema tables: error');
    }
  };

  const runAuditAll = async (tables, runId) => {
    if (!auditTable || tables.length === 0) return;
    setIsAuditingAll(true);
    setAuditProgress({ total: tables.length, completed: 0, current: '' });

    try {
      const sortedTables = [...tables].sort((a, b) => (a.row_count || 0) - (b.row_count || 0));
      for (const table of sortedTables) {
        if (cancelRef.current || runId !== runRef.current) break;
        const tableName = table.table_name;
        pushLog(`Audit table: ${tableName}`);
        setAuditProgress((prev) => ({ ...prev, current: tableName }));
        const result = await auditTable(tableName);
        if (runId !== runRef.current) break;
        if (result?.error) {
          setAuditErrors((prev) => ({ ...prev, [tableName]: result.error }));
        } else if (result) {
          setAuditResults((prev) => ({ ...prev, [tableName]: result }));
        }
        setAuditProgress((prev) => ({ ...prev, completed: prev.completed + 1 }));
      }
    } catch (err) {
      if (runId !== runRef.current) return;
      setAuditErrors((prev) => ({
        ...prev,
        _global: err?.message || 'Audit scan failed',
      }));
    } finally {
      if (runId === runRef.current) {
        setIsAuditingAll(false);
        setAuditProgress((prev) => ({ ...prev, current: '' }));
        setLastAuditAt(new Date().toISOString());
        if (!cancelRef.current) {
          pushLog('Audit scan: complete');
        }
      }
    }
  };

  const stopAudit = () => {
    cancelRef.current = true;
    setIsAuditingAll(false);
    setAuditProgress((prev) => ({ ...prev, current: '' }));
    pushLog('Audit scan: stopped');
  };

  const rerunAudit = () => {
    if (!schemaTables.length) return;
    cancelRef.current = false;
    runRef.current += 1;
    const runId = runRef.current;
    setAuditResults({});
    setAuditErrors({});
    setAuditProgress({ total: 0, completed: 0, current: '' });
    pushLog('Audit scan: restart');
    runAuditAll(schemaTables, runId);
  };

  const handleOpenAudit = async (tableName) => {
    const existing = auditResults[tableName];
    if (existing) {
      onShowAudit?.(existing);
      return;
    }
    if (!auditTable) return;
    const result = await auditTable(tableName);
    if (result?.error) {
      setAuditErrors((prev) => ({ ...prev, [tableName]: result.error }));
      return;
    }
    if (result) {
      setAuditResults((prev) => ({ ...prev, [tableName]: result }));
      onShowAudit?.(result);
    }
  };

  const auditList = useMemo(() => Object.values(auditResults), [auditResults]);
  const auditedCount = auditList.length;
  const totalTables = schemaTables.length;
  const completedCount = isAuditingAll ? auditProgress.completed : auditedCount;
  const auditPercent = totalTables ? Math.round((completedCount / totalTables) * 100) : 0;

  const totalRows = useMemo(() => (
    schemaTables.reduce((sum, table) => sum + (table.row_count || 0), 0)
  ), [schemaTables]);

  const tablesWithDates = useMemo(() => (
    schemaTables.filter((table) => table.min_date && table.max_date).length
  ), [schemaTables]);

  const tablesWithEntities = useMemo(() => (
    schemaTables.filter((table) => table.has_entity_column).length
  ), [schemaTables]);

  const averageHealth = useMemo(() => {
    if (!auditList.length) return null;
    const total = auditList.reduce((sum, audit) => sum + (audit.summary?.health_score || 0), 0);
    return Math.round(total / auditList.length);
  }, [auditList]);

  const alertTotals = useMemo(() => {
    return auditList.reduce((totals, audit) => {
      (audit.alerts || []).forEach((alert) => {
        if (alert.level === 'critical') totals.critical += 1;
        if (alert.level === 'warning') totals.warning += 1;
        if (alert.level === 'error') totals.error += 1;
      });
      return totals;
    }, { critical: 0, warning: 0, error: 0 });
  }, [auditList]);

  const auditSampleInfo = useMemo(() => {
    const sampleAudit = auditList.find((audit) => audit.summary?.sampled);
    if (!sampleAudit) return null;
    return {
      size: sampleAudit.summary?.sample_size,
      percent: sampleAudit.summary?.sample_percent,
    };
  }, [auditList]);

  const riskTables = useMemo(() => {
    return auditList
      .map((audit) => {
        const alerts = audit.alerts || [];
        return {
          table: audit.table_name,
          health: audit.summary?.health_score || 0,
          rows: audit.row_count || 0,
          critical: alerts.filter((a) => a.level === 'critical').length,
          warning: alerts.filter((a) => a.level === 'warning').length,
          audited_at: audit.audited_at,
        };
      })
      .sort((a, b) => a.health - b.health);
  }, [auditList]);

  const missingHotspots = useMemo(() => {
    const items = [];
    auditList.forEach((audit) => {
      Object.entries(audit.columns || {}).forEach(([colName, stats]) => {
        const pct = stats.null_percentage;
        if (pct === null || pct === undefined) return;
        items.push({
          table: audit.table_name,
          column: colName,
          nullPct: pct,
          type: stats.type,
          distinct: stats.distinct_count,
        });
      });
    });
    return items.sort((a, b) => b.nullPct - a.nullPct);
  }, [auditList]);

  const targetCandidates = useMemo(() => {
    const items = [];
    auditList.forEach((audit) => {
      Object.entries(audit.columns || {}).forEach(([colName, stats]) => {
        if (!stats) return;
        const distinct = stats.distinct_count;
        const nullPct = stats.null_percentage || 0;
        if (stats.type !== 'text') return;
        if (distinct && distinct >= 2 && distinct <= 10 && nullPct < 0.3) {
          items.push({
            table: audit.table_name,
            column: colName,
            distinct,
            nullPct,
          });
        }
      });
    });
    return items.sort((a, b) => a.distinct - b.distinct);
  }, [auditList]);

  const dateCoverage = useMemo(() => {
    const items = [];
    schemaTables.forEach((table) => {
      if (!table.min_date || !table.max_date) return;
      const minTs = Date.parse(table.min_date);
      const maxTs = Date.parse(table.max_date);
      if (Number.isNaN(minTs) || Number.isNaN(maxTs)) return;
      items.push({
        table: table.table_name,
        min: table.min_date,
        max: table.max_date,
        minTs,
        maxTs,
      });
    });
    if (!items.length) return { bars: [], globalMin: null, globalMax: null };
    const globalMin = Math.min(...items.map((item) => item.minTs));
    const globalMax = Math.max(...items.map((item) => item.maxTs));
    const range = globalMax - globalMin || 1;
    const bars = items.map((item) => ({
      table: item.table,
      min: item.min,
      max: item.max,
      left: Math.max(0, ((item.minTs - globalMin) / range) * 100),
      width: Math.max(2, ((item.maxTs - item.minTs) / range) * 100),
    }));
    return {
      bars,
      globalMin: new Date(globalMin),
      globalMax: new Date(globalMax),
    };
  }, [schemaTables]);

  const currentTableInfo = useMemo(() => {
    if (!auditProgress.current) return null;
    return schemaTables.find((table) => table.table_name === auditProgress.current) || null;
  }, [schemaTables, auditProgress.current]);

  const tablesStatus = isLoadingTables
    ? 'RUNNING'
    : schemaTables.length
      ? 'DONE'
      : 'IDLE';

  const entitiesStatus = isLoadingEntities
    ? 'RUNNING'
    : entitiesError
      ? 'ERROR'
      : entities.length
        ? 'DONE'
        : schemaTables.length
          ? 'QUEUED'
          : 'IDLE';

  const auditStatus = isAuditingAll
    ? 'RUNNING'
    : auditedCount > 0
      ? 'DONE'
      : schemaTables.length
        ? 'QUEUED'
        : 'IDLE';

  const visualsStatus = auditedCount > 0 ? 'READY' : auditStatus === 'RUNNING' ? 'QUEUED' : 'IDLE';
  const autoScanStatus = isAuditingAll
    ? 'running'
    : isLoadingTables || isLoadingEntities
      ? 'starting'
      : 'idle';

  const topEntities = useMemo(() => {
    return (entities || [])
      .slice()
      .sort((a, b) => (b.confidence || 0) - (a.confidence || 0));
  }, [entities]);

  const categoricalCandidates = useMemo(() => {
    const items = [];
    auditList.forEach((audit) => {
      Object.entries(audit.columns || {}).forEach(([colName, stats]) => {
        if (!stats || stats.type !== 'text') return;
        const distinct = stats.distinct_count || 0;
        const nullPct = stats.null_percentage || 0;
        if (distinct >= 2 && distinct <= 12 && nullPct <= 0.4) {
          items.push({
            table: audit.table_name,
            column: colName,
            distinct,
            nullPct,
            rowCount: audit.row_count || 0,
          });
        }
      });
    });
    return items.sort((a, b) => a.distinct - b.distinct || b.rowCount - a.rowCount);
  }, [auditList]);

  const numericCandidates = useMemo(() => {
    const items = [];
    auditList.forEach((audit) => {
      Object.entries(audit.columns || {}).forEach(([colName, stats]) => {
        if (!stats || stats.type !== 'numeric') return;
        const distinct = stats.distinct_count || 0;
        if (distinct <= 10) return;
        if (stats.min === null || stats.max === null) return;
        items.push({
          table: audit.table_name,
          column: colName,
          rowCount: audit.row_count || 0,
        });
      });
    });
    return items.sort((a, b) => b.rowCount - a.rowCount);
  }, [auditList]);

  const duplicateRisks = useMemo(() => {
    const items = [];
    auditList.forEach((audit) => {
      const rowCount = audit.row_count || 0;
      if (!rowCount) return;
      let bestColumn = null;
      let bestDistinct = 0;
      Object.entries(audit.columns || {}).forEach(([colName, stats]) => {
        const distinct = stats?.distinct_count || 0;
        if (distinct > bestDistinct) {
          bestDistinct = distinct;
          bestColumn = colName;
        }
      });
      if (!bestColumn || bestDistinct === 0) return;
      const uniqueRate = Math.min(1, bestDistinct / rowCount);
      items.push({
        table: audit.table_name,
        bestColumn,
        bestDistinct,
        rowCount,
        duplicateRate: 1 - uniqueRate,
      });
    });
    return items.sort((a, b) => b.duplicateRate - a.duplicateRate);
  }, [auditList]);

  const nullHeatmapTables = useMemo(() => {
    return auditList
      .filter((audit) => audit.columns && Object.keys(audit.columns).length > 0)
      .sort((a, b) => (b.row_count || 0) - (a.row_count || 0))
      .map((audit) => {
        const columns = Object.entries(audit.columns || {})
          .map(([name, stats]) => ({
            name,
            nullPct: stats?.null_percentage || 0,
          }))
          .sort((a, b) => b.nullPct - a.nullPct);
        return {
          table: audit.table_name,
          columns,
        };
      });
  }, [auditList]);

  const categoricalKey = useMemo(
    () => categoricalCandidates.map((c) => `${c.table}.${c.column}`).join('|'),
    [categoricalCandidates],
  );

  const numericKey = useMemo(
    () => numericCandidates.map((c) => `${c.table}.${c.column}`).join('|'),
    [numericCandidates],
  );

  useEffect(() => {
    if (!dbSessionId || !categoricalCandidates.length) return;
    let isActive = true;
    setIsLoadingDistributions(true);

    const loadDistributions = async () => {
      for (const candidate of categoricalCandidates) {
        if (!isActive) return;
        const key = `${candidate.table}.${candidate.column}`;
        if (loadedDistributionKeys.current.has(key)) continue;
        loadedDistributionKeys.current.add(key);
        try {
          const response = await api.post('/get-column-values', {
            session_id: dbSessionId,
            table_name: candidate.table,
            column_name: candidate.column,
            limit: 8,
          });
          if (!isActive) return;
          setValueDistributions((prev) => ({ ...prev, [key]: response.data }));
        } catch (err) {
          if (!isActive) return;
          setDistributionErrors((prev) => ({
            ...prev,
            [key]: err.response?.data?.detail || 'Failed to load values',
          }));
        }
      }
    };

    loadDistributions().finally(() => {
      if (isActive) setIsLoadingDistributions(false);
    });

    return () => {
      isActive = false;
    };
  }, [dbSessionId, categoricalKey]);

  useEffect(() => {
    if (!dbSessionId || !numericCandidates.length) return;
    let isActive = true;
    setIsLoadingHistograms(true);

    const loadHistograms = async () => {
      for (const candidate of numericCandidates) {
        if (!isActive) return;
        const key = `${candidate.table}.${candidate.column}`;
        if (loadedHistogramKeys.current.has(key)) continue;
        loadedHistogramKeys.current.add(key);
        try {
          const response = await api.post('/schema/histogram', {
            session_id: dbSessionId,
            table_name: candidate.table,
            column_name: candidate.column,
            bins: 12,
            sample_size: 100000,
          });
          if (!isActive) return;
          setHistograms((prev) => ({ ...prev, [key]: response.data }));
        } catch (err) {
          if (!isActive) return;
          setHistogramErrors((prev) => ({
            ...prev,
            [key]: err.response?.data?.detail || 'Failed to load histogram',
          }));
        }
      }
    };

    loadHistograms().finally(() => {
      if (isActive) setIsLoadingHistograms(false);
    });

    return () => {
      isActive = false;
    };
  }, [dbSessionId, numericKey]);

  return (
    <div className="flex-1 overflow-y-auto">
      <div className="terminal-header">DATABASE INSIGHTS</div>

      <div className="p-4 space-y-4">
        {schemaError && (
          <div className="terminal-panel p-4 border-[var(--color-terminal-red)] bg-[#1a0a0a] text-[var(--color-terminal-red)] text-sm">
            {schemaError}
          </div>
        )}

        <div className="terminal-panel">
          <div className="terminal-header">SCAN CONSOLE</div>
          <div className="p-4 space-y-2 text-xs font-mono">
            <div className="grid grid-cols-[140px_80px_1fr] text-[var(--color-terminal-text-dim)]">
              <span>TASK</span>
              <span>STATE</span>
              <span>DETAIL</span>
            </div>
            <div className="grid grid-cols-[140px_80px_1fr]">
              <span>Schema Tables</span>
              <span style={{ color: getStatusColor(tablesStatus) }}>{tablesStatus}</span>
              <span className="text-[var(--color-terminal-text-dim)]">
                {isLoadingTables ? 'Loading table list...' : `${totalTables} tables ready`}
              </span>
            </div>
            <div className="grid grid-cols-[140px_80px_1fr]">
              <span>Entity Scan</span>
              <span style={{ color: getStatusColor(entitiesStatus) }}>{entitiesStatus}</span>
              <span className="text-[var(--color-terminal-text-dim)]">
                {entitiesError
                  ? entitiesError
                  : isLoadingEntities
                    ? 'Scanning IDs via pg_stats...'
                    : entities.length
                      ? `${entities.length} entity candidates`
                      : 'Waiting for schema'}
              </span>
            </div>
            <div className="grid grid-cols-[140px_80px_1fr]">
              <span>Quality Audit</span>
              <span style={{ color: getStatusColor(auditStatus) }}>{auditStatus}</span>
              <span className="text-[var(--color-terminal-text-dim)]">
                {isAuditingAll
                  ? `Auditing: ${auditProgress.current || '-'}`
                  : auditedCount > 0
                    ? `${auditedCount}/${totalTables} tables`
                    : 'Queued'}
              </span>
            </div>
            <div className="grid grid-cols-[140px_80px_1fr]">
              <span>Visuals</span>
              <span style={{ color: getStatusColor(visualsStatus) }}>{visualsStatus}</span>
              <span className="text-[var(--color-terminal-text-dim)]">
                {auditedCount > 0 ? 'Charts and health panels online' : 'Waiting for audits'}
              </span>
            </div>
            <div className="border-t border-[#1f1f1f] pt-2 space-y-1">
              <div className="text-[10px] text-[var(--color-terminal-text-dim)] uppercase">Activity Log</div>
              {scanLog.length === 0 && (
                <div className="text-[var(--color-terminal-text-dim)]">No events yet.</div>
              )}
              {scanLog.map((entry, idx) => (
                <div key={`${entry.time}-${idx}`} className="grid grid-cols-[60px_1fr] gap-2">
                  <span className="text-[var(--color-terminal-text-dim)]">{entry.time}</span>
                  <span>{entry.message}</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="terminal-panel p-4">
            <div className="text-[10px] text-[var(--color-terminal-text-dim)] uppercase tracking-wide">Database</div>
            <div className="text-lg font-mono text-[var(--color-terminal-orange)] mt-1">
              {dbName || 'unknown'}
            </div>
            <div className="text-xs text-[var(--color-terminal-text-dim)] mt-2">
              {totalTables} tables indexed
            </div>
          </div>
          <div className="terminal-panel p-4">
            <div className="text-[10px] text-[var(--color-terminal-text-dim)] uppercase tracking-wide">Total Rows</div>
            <div className="text-lg font-mono text-[var(--color-terminal-green)] mt-1">
              {formatNumber(totalRows)}
            </div>
            <div className="text-xs text-[var(--color-terminal-text-dim)] mt-2">
              {tablesWithDates} tables with time coverage
            </div>
          </div>
          <div className="terminal-panel p-4">
            <div className="text-[10px] text-[var(--color-terminal-text-dim)] uppercase tracking-wide">Audit Coverage</div>
            <div className="text-lg font-mono text-[var(--color-terminal-cyan)] mt-1">
              {auditedCount}/{totalTables}
            </div>
            <div className="text-xs text-[var(--color-terminal-text-dim)] mt-2">
              {auditPercent}% of tables audited
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="terminal-panel p-4">
            <div className="text-[10px] text-[var(--color-terminal-text-dim)] uppercase tracking-wide">Average Health</div>
            <div
              className="text-lg font-mono mt-1"
              style={{ color: averageHealth !== null ? getHealthColor(averageHealth) : '#606060' }}
            >
              {averageHealth !== null ? `${averageHealth}/100` : '-'}
            </div>
            <div className="text-xs text-[var(--color-terminal-text-dim)] mt-2">
              Critical: {alertTotals.critical} | Warnings: {alertTotals.warning}
            </div>
          </div>
          <div className="terminal-panel p-4">
            <div className="text-[10px] text-[var(--color-terminal-text-dim)] uppercase tracking-wide">Entity Coverage</div>
            <div className="text-lg font-mono text-[var(--color-terminal-yellow)] mt-1">
              {tablesWithEntities}/{totalTables}
            </div>
            <div className="text-xs text-[var(--color-terminal-text-dim)] mt-2">
              Tables with entity columns
            </div>
          </div>
          <div className="terminal-panel p-4">
            <div className="text-[10px] text-[var(--color-terminal-text-dim)] uppercase tracking-wide">Latest Audit</div>
            <div className="text-lg font-mono text-[var(--color-terminal-text)] mt-1">
              {lastAuditAt ? formatDate(lastAuditAt) : '-'}
            </div>
            <div className="text-xs text-[var(--color-terminal-text-dim)] mt-2">
              Auto scan status: {autoScanStatus}
            </div>
          </div>
        </div>

        <div className="terminal-panel">
          <div className="terminal-header">AUDIT PROGRESS</div>
          <div className="p-4 space-y-3 text-sm">
            <div className="flex items-center justify-between text-[var(--color-terminal-text-dim)]">
              <span>Progress: {completedCount}/{totalTables}</span>
              <span>{isAuditingAll ? 'RUNNING' : 'IDLE'}</span>
            </div>
            <div className="h-2 bg-[#2a2a2a] rounded-full overflow-hidden">
              <div
                className="h-full bg-[var(--color-terminal-orange)] transition-all"
                style={{ width: `${auditPercent}%` }}
              />
            </div>
            <div className="text-[10px] text-[var(--color-terminal-text-dim)]">
              Current table: {auditProgress.current || '-'}
              {currentTableInfo ? ` (${formatNumber(currentTableInfo.row_count)} rows)` : ''}
            </div>
            <div className="text-[10px] text-[var(--color-terminal-text-dim)]">
              Scan mode: {auditSampleInfo
                ? `Sampled ${formatNumber(auditSampleInfo.size)} rows${auditSampleInfo.percent ? ` (~${auditSampleInfo.percent}%)` : ''}`
                : 'Full scan for small tables'}
            </div>
            <div className="flex gap-2">
              {isAuditingAll ? (
                <button onClick={stopAudit} className="btn-terminal text-xs">
                  STOP SCAN
                </button>
              ) : (
                <button onClick={rerunAudit} className="btn-terminal text-xs">
                  RE-RUN SCAN
                </button>
              )}
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div className="terminal-panel">
            <div className="terminal-header">RISKY TABLES</div>
            <div className="p-4 space-y-2 text-sm max-h-[420px] overflow-y-auto">
              {riskTables.length === 0 && (
                <div className="text-[var(--color-terminal-text-dim)]">Waiting for audit results...</div>
              )}
              {riskTables.map((table) => (
                <div key={table.table} className="flex items-center justify-between gap-3">
                  <div className="flex-1">
                    <div className="text-[var(--color-terminal-text)] font-mono">{table.table}</div>
                    <div className="text-[10px] text-[var(--color-terminal-text-dim)]">
                      {formatNumber(table.rows)} rows | {table.critical} critical | {table.warning} warnings
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-mono" style={{ color: getHealthColor(table.health) }}>
                      {table.health}
                    </span>
                    <button
                      type="button"
                      onClick={() => handleOpenAudit(table.table)}
                      className="btn-terminal text-[10px]"
                    >
                      DETAILS
                    </button>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="terminal-panel">
            <div className="terminal-header">DATE COVERAGE</div>
            <div className="p-4 space-y-3 text-sm">
              {dateCoverage.bars.length === 0 && (
                <div className="text-[var(--color-terminal-text-dim)]">No date ranges detected.</div>
              )}
              {dateCoverage.bars.map((bar) => (
                <div key={bar.table} className="space-y-1">
                  <div className="flex items-center justify-between text-[11px]">
                    <span className="font-mono text-[var(--color-terminal-text)]">{bar.table}</span>
                    <span className="text-[var(--color-terminal-text-dim)]">
                      {formatDate(bar.min)} - {formatDate(bar.max)}
                    </span>
                  </div>
                  <div className="relative h-2 bg-[#1a1a1a] rounded-full overflow-hidden">
                    <div
                      className="absolute top-0 h-full bg-[var(--color-terminal-cyan)]"
                      style={{ left: `${bar.left}%`, width: `${bar.width}%` }}
                    />
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div className="terminal-panel">
            <div className="terminal-header">ENTITY CANDIDATES</div>
            <div className="p-4 space-y-2 text-sm max-h-[420px] overflow-y-auto">
              {isLoadingEntities && (
                <div className="text-[var(--color-terminal-text-dim)]">Scanning entity candidates...</div>
              )}
              {!isLoadingEntities && entitiesError && (
                <div className="text-[var(--color-terminal-red)]">{entitiesError}</div>
              )}
              {!isLoadingEntities && !entitiesError && topEntities.length === 0 && (
                <div className="text-[var(--color-terminal-text-dim)]">No entity candidates detected yet.</div>
              )}
              {topEntities.map((entity) => (
                <div key={entity.column_name} className="flex items-center justify-between">
                  <div>
                    <div className="font-mono text-[var(--color-terminal-text)]">{entity.column_name}</div>
                    <div className="text-[10px] text-[var(--color-terminal-text-dim)]">
                      {entity.tables?.length || 0} tables | {formatNumber(entity.total_unique)} unique
                    </div>
                  </div>
                  <div className="text-xs text-[var(--color-terminal-cyan)]">
                    {Math.round((entity.confidence || 0) * 100)}%
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="terminal-panel">
            <div className="terminal-header">TARGET CANDIDATES</div>
            <div className="p-4 space-y-2 text-sm max-h-[420px] overflow-y-auto">
              {auditedCount === 0 && (
                <div className="text-[var(--color-terminal-text-dim)]">
                  {isAuditingAll ? 'Auditing tables for target signals...' : 'Waiting for audit results...'}
                </div>
              )}
              {auditedCount > 0 && targetCandidates.length === 0 && (
                <div className="text-[var(--color-terminal-text-dim)]">No low-cardinality labels found yet.</div>
              )}
              {targetCandidates.map((candidate) => (
                <div key={`${candidate.table}-${candidate.column}`} className="flex items-center justify-between">
                  <div>
                    <div className="font-mono text-[var(--color-terminal-text)]">
                      {candidate.table}.{candidate.column}
                    </div>
                    <div className="text-[10px] text-[var(--color-terminal-text-dim)]">
                      {candidate.distinct} values | {formatPercent(candidate.nullPct)} null
                    </div>
                  </div>
                  <span className="text-xs text-[var(--color-terminal-yellow)]">LABEL</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className="terminal-panel">
          <div className="terminal-header">MISSINGNESS HOTSPOTS</div>
          <div className="p-4 space-y-2 text-sm max-h-[520px] overflow-y-auto">
            {auditedCount === 0 && (
              <div className="text-[var(--color-terminal-text-dim)]">
                {isAuditingAll ? 'Scanning for missingness hotspots...' : 'Waiting for audit results...'}
              </div>
            )}
            {auditedCount > 0 && missingHotspots.length === 0 && (
              <div className="text-[var(--color-terminal-text-dim)]">No missingness data yet.</div>
            )}
            {missingHotspots.length > 0 && (
              <div className="grid grid-cols-[200px_1fr_60px] gap-2 text-[9px] text-[var(--color-terminal-text-dim)]">
                <span>Column</span>
                <div className="flex items-center justify-between">
                  <span>0%</span>
                  <span>100%</span>
                </div>
                <span className="text-right">%</span>
              </div>
            )}
            {missingHotspots.map((item) => {
              const color = item.nullPct > 0.4 ? '#ff1744' : item.nullPct > 0.2 ? '#ffc107' : '#00c853';
              return (
                <div key={`${item.table}-${item.column}`} className="grid grid-cols-[200px_1fr_60px] gap-2 items-center">
                  <div className="font-mono text-[11px] text-[var(--color-terminal-text)] truncate" title={`${item.table}.${item.column}`}>
                    {truncateLabel(`${item.table}.${item.column}`, 18)}
                  </div>
                  <svg viewBox="0 0 100 12" className="w-full h-3">
                    <rect x="0" y="1" width="100" height="10" fill="#0f0f0f" stroke="#1f1f1f" />
                    <line x1="20" y1="1" x2="20" y2="11" stroke="#1f1f1f" strokeWidth="1" />
                    <line x1="40" y1="1" x2="40" y2="11" stroke="#1f1f1f" strokeWidth="1" />
                    <rect
                      x="0"
                      y="2"
                      width={Math.max(1, Math.min(100, item.nullPct * 100))}
                      height="8"
                      fill={color}
                    />
                  </svg>
                  <div className="text-[10px] text-right" style={{ color }}>
                    {formatPercent(item.nullPct)}
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div className="terminal-panel">
            <div className="terminal-header">CATEGORICAL DISTRIBUTIONS</div>
            <div className="p-4 space-y-3 text-sm max-h-[520px] overflow-y-auto">
              {auditedCount === 0 && (
                <div className="text-[var(--color-terminal-text-dim)]">
                  {isAuditingAll ? 'Waiting for audit results...' : 'Run a scan to surface categories.'}
                </div>
              )}
              {auditedCount > 0 && categoricalCandidates.length === 0 && (
                <div className="text-[var(--color-terminal-text-dim)]">
                  No low-cardinality text columns found yet.
                </div>
              )}
              {isLoadingDistributions && (
                <div className="text-[var(--color-terminal-text-dim)]">
                  Loading top value distributions...
                </div>
              )}
              {categoricalCandidates.map((candidate) => {
                const key = `${candidate.table}.${candidate.column}`;
                const data = valueDistributions[key];
                const error = distributionErrors[key];
                return (
                  <div key={key} className="border-b border-[#1f1f1f] pb-3 last:border-b-0">
                    <div className="flex items-center justify-between">
                      <div className="font-mono text-[var(--color-terminal-text)]">
                        {candidate.table}.{candidate.column}
                      </div>
                      <div className="text-[10px] text-[var(--color-terminal-text-dim)]">
                        {candidate.distinct} values
                      </div>
                    </div>
                    {error && (
                      <div className="text-[10px] text-[var(--color-terminal-red)] mt-2">{error}</div>
                    )}
                    {!error && !data && (
                      <div className="text-[10px] text-[var(--color-terminal-text-dim)] mt-2">
                        Loading values...
                      </div>
                    )}
                    {data && (
                      <div className="space-y-2 mt-2">
                        {data.sampled && (
                          <div className="text-[10px] text-[var(--color-terminal-text-dim)]">
                            Sampled {formatNumber(data.sample_size)} rows{data.sample_percent ? ` (~${data.sample_percent}%)` : ''}
                          </div>
                        )}
                        {(() => {
                          const values = data.values || [];
                          const maxCount = Math.max(1, ...values.map((item) => item.count || 0));
                          return (
                            <div className="space-y-2">
                              <div className="grid grid-cols-[140px_1fr_60px] gap-2 text-[9px] text-[var(--color-terminal-text-dim)]">
                                <span>Value</span>
                                <div className="flex items-center justify-between">
                                  <span>0</span>
                                  <span>{formatCompactNumber(maxCount)}</span>
                                </div>
                                <span className="text-right">%</span>
                              </div>
                              {values.map((value) => {
                                const label = value.is_null ? 'NULL' : String(value.value);
                                const pct = value.percentage || 0;
                                const ratio = maxCount ? (value.count || 0) / maxCount : 0;
                                return (
                                  <div key={`${key}-${label}`} className="grid grid-cols-[140px_1fr_60px] gap-2 items-center">
                                    <div className="text-[11px] text-[var(--color-terminal-text)] font-mono truncate" title={label}>
                                      {truncateLabel(label, 14)}
                                    </div>
                                    <svg viewBox="0 0 100 12" className="w-full h-3">
                                      <rect x="0" y="1" width="100" height="10" fill="#0f0f0f" stroke="#1f1f1f" />
                                      <line x1="50" y1="1" x2="50" y2="11" stroke="#1f1f1f" strokeWidth="1" />
                                      <rect
                                        x="0"
                                        y="2"
                                        width={Math.max(1, Math.min(100, ratio * 100))}
                                        height="8"
                                        fill="#14b8a6"
                                      />
                                    </svg>
                                    <div className="text-[10px] text-right text-[var(--color-terminal-text-dim)]" title={`Count: ${formatNumber(value.count)}`}>
                                      {formatPercentValue(pct)}
                                    </div>
                                  </div>
                                );
                              })}
                            </div>
                          );
                        })()}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>

          <div className="terminal-panel">
            <div className="terminal-header">NUMERIC HISTOGRAMS</div>
            <div className="p-4 space-y-4 text-sm max-h-[520px] overflow-y-auto">
              {auditedCount === 0 && (
                <div className="text-[var(--color-terminal-text-dim)]">
                  {isAuditingAll ? 'Waiting for audit results...' : 'Run a scan to profile numeric columns.'}
                </div>
              )}
              {auditedCount > 0 && numericCandidates.length === 0 && (
                <div className="text-[var(--color-terminal-text-dim)]">
                  No numeric columns available for histograms.
                </div>
              )}
              {isLoadingHistograms && (
                <div className="text-[var(--color-terminal-text-dim)]">
                  Loading numeric histograms...
                </div>
              )}
              {numericCandidates.map((candidate) => {
                const key = `${candidate.table}.${candidate.column}`;
                const data = histograms[key];
                const error = histogramErrors[key];
                if (error) {
                  return (
                    <div key={key} className="text-[10px] text-[var(--color-terminal-red)]">
                      {key}: {error}
                    </div>
                  );
                }
                if (!data) {
                  return (
                    <div key={key} className="text-[10px] text-[var(--color-terminal-text-dim)]">
                      {key}: Loading histogram...
                    </div>
                  );
                }

                const min = data.min;
                const max = data.max;
                const mid = min !== null && max !== null ? (min + max) / 2 : null;
                const { bins, counts, maxCount, scaleMax, useLog } = buildHistogramSeries(data);
                const binSize = min !== null && max !== null && bins > 0 ? (max - min) / bins : null;
                const svgWidth = 520;
                const svgHeight = 120;
                const plotLeft = 36;
                const plotRight = 8;
                const plotTop = 10;
                const plotBottom = 22;
                const plotWidth = svgWidth - plotLeft - plotRight;
                const plotHeight = svgHeight - plotTop - plotBottom;
                const barWidth = plotWidth / Math.max(1, bins);
                const gridLines = [0.25, 0.5, 0.75];

                return (
                  <div key={key} className="border-b border-[#1f1f1f] pb-4 last:border-b-0">
                    <div className="flex items-center justify-between">
                      <div className="font-mono text-[var(--color-terminal-text)]">{key}</div>
                      <div className="text-[10px] text-[var(--color-terminal-text-dim)]">
                        {data.sampled ? `Sampled ${formatNumber(data.total_count)} rows` : 'Full scan'}
                      </div>
                    </div>
                    <div className="mt-3">
                      <svg viewBox={`0 0 ${svgWidth} ${svgHeight}`} className="w-full h-28">
                        <rect
                          x={plotLeft}
                          y={plotTop}
                          width={plotWidth}
                          height={plotHeight}
                          fill="#0f0f0f"
                          stroke="#1f1f1f"
                        />
                        {gridLines.map((line) => {
                          const y = plotTop + plotHeight - plotHeight * line;
                          return (
                            <line
                              key={`${key}-grid-${line}`}
                              x1={plotLeft}
                              x2={plotLeft + plotWidth}
                              y1={y}
                              y2={y}
                              stroke="#1f1f1f"
                              strokeWidth="1"
                            />
                          );
                        })}
                        {counts.map((count, idx) => {
                          const scaled = useLog ? Math.log10(count + 1) : count;
                          const height = Math.max(1, (scaled / scaleMax) * plotHeight);
                          const x = plotLeft + idx * barWidth;
                          const y = plotTop + (plotHeight - height);
                          const width = Math.max(1, barWidth - 1);
                          return (
                            <rect
                              key={`${key}-bar-${idx}`}
                              x={x}
                              y={y}
                              width={width}
                              height={height}
                              fill="#ff6b00"
                            />
                          );
                        })}
                        <line
                          x1={plotLeft}
                          x2={plotLeft + plotWidth}
                          y1={plotTop + plotHeight}
                          y2={plotTop + plotHeight}
                          stroke="#2a2a2a"
                          strokeWidth="1"
                        />
                        <text
                          x={plotLeft - 6}
                          y={plotTop + 8}
                          fill="#9a9a9a"
                          fontSize="9"
                          textAnchor="end"
                        >
                          {formatCompactNumber(maxCount)}
                        </text>
                        <text
                          x={plotLeft - 6}
                          y={plotTop + plotHeight}
                          fill="#9a9a9a"
                          fontSize="9"
                          textAnchor="end"
                        >
                          0
                        </text>
                        <text
                          x={plotLeft}
                          y={svgHeight - 6}
                          fill="#8a8a8a"
                          fontSize="9"
                          textAnchor="start"
                        >
                          {min !== null ? formatCompactNumber(min) : '-'}
                        </text>
                        <text
                          x={plotLeft + plotWidth / 2}
                          y={svgHeight - 6}
                          fill="#8a8a8a"
                          fontSize="9"
                          textAnchor="middle"
                        >
                          {mid !== null ? formatCompactNumber(mid) : '-'}
                        </text>
                        <text
                          x={plotLeft + plotWidth}
                          y={svgHeight - 6}
                          fill="#8a8a8a"
                          fontSize="9"
                          textAnchor="end"
                        >
                          {max !== null ? formatCompactNumber(max) : '-'}
                        </text>
                        {useLog && (
                          <text
                            x={plotLeft + plotWidth}
                            y={plotTop + 10}
                            fill="#ffc107"
                            fontSize="9"
                            textAnchor="end"
                          >
                            log scale
                          </text>
                        )}
                      </svg>
                    </div>
                    <div className="flex items-center justify-between text-[10px] text-[var(--color-terminal-text-dim)] mt-2">
                      <span>Range: {min !== null ? formatCompactNumber(min) : '-'} to {max !== null ? formatCompactNumber(max) : '-'}</span>
                      <span>Max count: {formatCompactNumber(maxCount)}</span>
                    </div>
                    {binSize !== null && binSize > 0 && (
                      <div className="text-[10px] text-[var(--color-terminal-text-dim)] mt-1">
                        Bin size: {formatCompactNumber(binSize)}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div className="terminal-panel">
            <div className="terminal-header">DUPLICATE RISK (ESTIMATED)</div>
            <div className="p-4 space-y-3 text-sm max-h-[420px] overflow-y-auto">
              {auditedCount === 0 && (
                <div className="text-[var(--color-terminal-text-dim)]">
                  {isAuditingAll ? 'Waiting for audit results...' : 'Run a scan to estimate duplicates.'}
                </div>
              )}
              {auditedCount > 0 && duplicateRisks.length === 0 && (
                <div className="text-[var(--color-terminal-text-dim)]">
                  No duplicate signals available yet.
                </div>
              )}
              {duplicateRisks.length > 0 && (
                <div className="grid grid-cols-[140px_1fr_60px] gap-2 text-[9px] text-[var(--color-terminal-text-dim)]">
                  <span>Table</span>
                  <div className="flex items-center justify-between">
                    <span>0%</span>
                    <span>100%</span>
                  </div>
                  <span className="text-right">dup%</span>
                </div>
              )}
              {duplicateRisks.map((item) => {
                const pct = item.duplicateRate * 100;
                return (
                  <div key={item.table} className="space-y-1">
                    <div className="flex items-center justify-between">
                      <div className="font-mono text-[var(--color-terminal-text)]">{item.table}</div>
                      <div className="text-[10px] text-[var(--color-terminal-text-dim)]">
                        Best key: {item.bestColumn}
                      </div>
                    </div>
                    <div className="grid grid-cols-[1fr_60px] gap-2 items-center">
                      <svg viewBox="0 0 100 12" className="w-full h-3">
                        <rect x="0" y="1" width="100" height="10" fill="#0f0f0f" stroke="#1f1f1f" />
                        <line x1="25" y1="1" x2="25" y2="11" stroke="#1f1f1f" strokeWidth="1" />
                        <line x1="50" y1="1" x2="50" y2="11" stroke="#1f1f1f" strokeWidth="1" />
                        <line x1="75" y1="1" x2="75" y2="11" stroke="#1f1f1f" strokeWidth="1" />
                        <rect
                          x="0"
                          y="2"
                          width={Math.max(1, Math.min(100, pct))}
                          height="8"
                          fill="#f4b400"
                        />
                      </svg>
                      <div className="text-right text-[10px] text-[var(--color-terminal-text-dim)]">
                        {formatPercentValue(pct)}
                      </div>
                    </div>
                    <div className="text-[10px] text-[var(--color-terminal-text-dim)]">
                      {formatNumber(item.bestDistinct)} distinct out of {formatNumber(item.rowCount)} rows
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          <div className="terminal-panel">
            <div className="terminal-header">NULL HEATMAP</div>
            <div className="p-4 space-y-4 text-sm max-h-[420px] overflow-y-auto">
              {auditedCount === 0 && (
                <div className="text-[var(--color-terminal-text-dim)]">
                  {isAuditingAll ? 'Waiting for audit results...' : 'Run a scan to build a heatmap.'}
                </div>
              )}
              {auditedCount > 0 && nullHeatmapTables.length === 0 && (
                <div className="text-[var(--color-terminal-text-dim)]">
                  No column missingness data yet.
                </div>
              )}
              {nullHeatmapTables.map((table) => (
                <div key={table.table} className="space-y-2">
                  <div className="font-mono text-[var(--color-terminal-text)]">{table.table}</div>
                  <div className="flex flex-wrap gap-1">
                    {table.columns.map((col) => (
                      <div
                        key={`${table.table}-${col.name}`}
                        className="w-3 h-3 rounded-sm"
                        style={{ backgroundColor: getNullColor(col.nullPct) }}
                        title={`${table.table}.${col.name} - ${formatPercent(col.nullPct)} null`}
                      />
                    ))}
                  </div>
                </div>
              ))}
              <div className="flex items-center gap-3 text-[10px] text-[var(--color-terminal-text-dim)]">
                <div className="flex items-center gap-1">
                  <span className="w-3 h-3 rounded-sm inline-block" style={{ backgroundColor: '#00c853' }} />
                  <span>&lt; 10%</span>
                </div>
                <div className="flex items-center gap-1">
                  <span className="w-3 h-3 rounded-sm inline-block" style={{ backgroundColor: '#ffc107' }} />
                  <span>10-25%</span>
                </div>
                <div className="flex items-center gap-1">
                  <span className="w-3 h-3 rounded-sm inline-block" style={{ backgroundColor: '#ff9800' }} />
                  <span>25-50%</span>
                </div>
                <div className="flex items-center gap-1">
                  <span className="w-3 h-3 rounded-sm inline-block" style={{ backgroundColor: '#ff1744' }} />
                  <span>&gt; 50%</span>
                </div>
              </div>
            </div>
          </div>
        </div>

        {Object.keys(auditErrors).length > 0 && (
          <div className="terminal-panel p-4 border-[var(--color-terminal-red)] bg-[#1a0a0a] text-[var(--color-terminal-red)] text-sm space-y-1">
            <div className="font-semibold">AUDIT ERRORS</div>
            {Object.entries(auditErrors).map(([table, error]) => (
              <div key={table}>{table}: {error}</div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
