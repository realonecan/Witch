import { useEffect, useMemo, useState } from 'react';
import api from '../../api/client';

const formatNumber = (value) => {
  if (value === null || value === undefined || Number.isNaN(value)) return '-';
  return Number(value).toLocaleString();
};

const formatCompactNumber = (value) => {
  if (value === null || value === undefined || Number.isNaN(value)) return '-';
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)}B`;
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return Number.isInteger(value) ? String(value) : value.toFixed(2);
};

const formatPercent = (value) => {
  if (value === null || value === undefined || Number.isNaN(value)) return '-';
  return `${(Math.max(0, Math.min(1, value)) * 100).toFixed(1)}%`;
};

const clampRate = (value) => Math.max(0, Math.min(1, value || 0));

const getEdgeColor = (edge) => {
  if (edge.is_primary || edge.is_unique) return '#00c853';
  return '#ffc107';
};

const getRiskColor = (value) => {
  if (value >= 0.3) return '#ff1744';
  if (value >= 0.15) return '#ff9800';
  return '#00c853';
};

const getMatchColor = (value) => {
  if (value >= 0.85) return '#00c853';
  if (value >= 0.6) return '#ffc107';
  return '#ff1744';
};

const buildJoinSql = (baseTable, steps, joinType, schema) => {
  if (!baseTable) return '-- Select a base table to start.';
  if (!steps.length) return '-- Add a join edge from the graph.';
  const joinKeyword = (joinType || 'left').toUpperCase();
  let sql = `SELECT *\nFROM "${schema}"."${baseTable}" t0\n`;
  steps.forEach((step, index) => {
    const leftAlias = `t${index}`;
    const rightAlias = `t${index + 1}`;
    const leftKeys = step.leftKeys?.length ? step.leftKeys : [step.leftKey];
    const rightKeys = step.rightKeys?.length ? step.rightKeys : [step.rightKey];
    const pairs = leftKeys.map((leftKey, idx) => ({
      left: leftKey,
      right: rightKeys[idx] || rightKeys[0],
    }));
    const onClause = pairs
      .filter((pair) => pair.left && pair.right)
      .map((pair) => `${leftAlias}."${pair.left}" = ${rightAlias}."${pair.right}"`)
      .join(' AND ');
    sql += `${joinKeyword} JOIN "${schema}"."${step.rightTable}" ${rightAlias} ON ${onClause}\n`;
  });
  return sql.trim();
};

const MetricBar = ({ label, value, color }) => (
  <div className="space-y-1">
    <div className="flex items-center justify-between text-[10px] text-[#808080]">
      <span>{label}</span>
      <span className="text-[#cfcfcf]">{formatPercent(value)}</span>
    </div>
    <div className="h-1 bg-[#1f1f1f]">
      <div
        className="h-full transition-all duration-300"
        style={{ width: `${clampRate(value) * 100}%`, backgroundColor: color }}
      />
    </div>
  </div>
);

export function JoinExplorer({ dbSessionId, dbTables }) {
  const [graphNodes, setGraphNodes] = useState([]);
  const [graphEdges, setGraphEdges] = useState([]);
  const [graphLoading, setGraphLoading] = useState(false);
  const [graphError, setGraphError] = useState(null);
  const [selectedEdgeId, setSelectedEdgeId] = useState(null);
  const [selectedNodeId, setSelectedNodeId] = useState(null);
  const [hoveredEdgeId, setHoveredEdgeId] = useState(null);
  const [analysisByEdge, setAnalysisByEdge] = useState({});
  const [statusMessage, setStatusMessage] = useState('');
  const [showRiskyEdges, setShowRiskyEdges] = useState(true);
  const [showIsolatedNodes, setShowIsolatedNodes] = useState(false);
  const [showInferredEdges, setShowInferredEdges] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [sampleSize, setSampleSize] = useState(100000);
  const [joinType, setJoinType] = useState('left');
  const [baseTable, setBaseTable] = useState('');
  const [chainSteps, setChainSteps] = useState([]);
  const [flipEdgeDirection, setFlipEdgeDirection] = useState(false);
  const schema = 'public';

  useEffect(() => {
    if (dbSessionId) {
      loadGraph();
    }
  }, [dbSessionId]);

  const loadGraph = async (includeInferredOverride = showInferredEdges) => {
    setGraphLoading(true);
    setGraphError(null);
    setStatusMessage('Scanning foreign keys...');
    try {
      const response = await api.post('/join/graph', {
        session_id: dbSessionId,
        schema,
        include_inferred: includeInferredOverride,
      });
      const nodes = response.data.nodes || [];
      const edges = response.data.edges || [];
      const hasFk = edges.some((edge) => edge.source === 'fk');
      const hasInferred = edges.some((edge) => edge.source === 'inferred');
      setGraphNodes(nodes);
      setGraphEdges(edges);
      if (!hasFk && hasInferred) {
        setShowInferredEdges(true);
        setStatusMessage('No FK constraints found. Showing inferred joins.');
      } else {
        setStatusMessage(hasFk ? 'FK scan complete.' : 'No FK constraints detected.');
      }
    } catch (error) {
      setGraphError('Failed to load join graph.');
    } finally {
      setGraphLoading(false);
    }
  };

  const filteredEdges = useMemo(() => {
    const withInferred = showInferredEdges
      ? graphEdges
      : graphEdges.filter((edge) => edge.source !== 'inferred');
    const base = showRiskyEdges
      ? withInferred
      : withInferred.filter((edge) => edge.is_primary || edge.is_unique);
    if (!searchTerm) return base;
    const term = searchTerm.toLowerCase();
    return base.filter((edge) =>
      edge.left_table.toLowerCase().includes(term) ||
      edge.right_table.toLowerCase().includes(term)
    );
  }, [graphEdges, showRiskyEdges, searchTerm]);

  const filteredNodes = useMemo(() => {
    const term = searchTerm.toLowerCase();
    const nodes = graphNodes.filter((node) =>
      !searchTerm || node.table_name.toLowerCase().includes(term)
    );
    if (showIsolatedNodes) return nodes;
    const connected = new Set();
    filteredEdges.forEach((edge) => {
      connected.add(edge.left_table);
      connected.add(edge.right_table);
    });
    return nodes.filter((node) => connected.has(node.table_name));
  }, [graphNodes, filteredEdges, showIsolatedNodes, searchTerm]);

  const nodePositions = useMemo(() => {
    const width = 720;
    const height = 420;
    const centerX = width / 2;
    const centerY = height / 2;
    const radius = Math.max(140, Math.min(width, height) / 2 - 50);
    const sortedNodes = [...filteredNodes].sort((a, b) =>
      a.table_name.localeCompare(b.table_name)
    );
    const positions = {};
    sortedNodes.forEach((node, index) => {
      const angle = (2 * Math.PI * index) / Math.max(1, sortedNodes.length);
      positions[node.table_name] = {
        x: centerX + radius * Math.cos(angle),
        y: centerY + radius * Math.sin(angle),
      };
    });
    return { width, height, positions };
  }, [filteredNodes]);

  const selectedEdge = graphEdges.find((edge) => edge.id === selectedEdgeId) || null;
  const fkEdgeCount = graphEdges.filter((edge) => edge.source === 'fk').length;
  const inferredEdgeCount = graphEdges.filter((edge) => edge.source === 'inferred').length;
  const graphTitle = showInferredEdges ? 'JOIN GRAPH (FK + INFERRED)' : 'JOIN GRAPH (FK ONLY)';
  const graphSubtitle = showInferredEdges ? 'EVIDENCE + INFERRED LINKS' : 'STRICT PK/FK EVIDENCE';
  const analysisKey = (edge, flipped) => `${edge.id}${flipped ? ':flip' : ''}`;
  const selectedAnalysis = selectedEdge
    ? analysisByEdge[analysisKey(selectedEdge, flipEdgeDirection)]
    : null;

  const resolveEdge = (edge) => {
    if (!edge) return null;
    if (!flipEdgeDirection) return edge;
    return {
      ...edge,
      left_table: edge.right_table,
      right_table: edge.left_table,
      left_columns: edge.right_columns,
      right_columns: edge.left_columns,
      left_schema: edge.right_schema,
      right_schema: edge.left_schema,
      left_estimate: edge.right_estimate,
      right_estimate: edge.left_estimate,
    };
  };

  const handleAnalyzeEdge = async (edge) => {
    if (!edge || !dbSessionId) return;
    const effectiveEdge = resolveEdge(edge);
    if (!effectiveEdge.left_columns.length || !effectiveEdge.right_columns.length) {
      setStatusMessage('Edge has no columns to analyze.');
      return;
    }
    const leftKey = effectiveEdge.left_columns[0];
    const rightKey = effectiveEdge.right_columns[0];
    setStatusMessage(`Analyzing ${effectiveEdge.left_table} -> ${effectiveEdge.right_table}...`);
    try {
      const response = await api.post('/join/analyze', {
        session_id: dbSessionId,
        left_table: effectiveEdge.left_table,
        right_table: effectiveEdge.right_table,
        left_key: leftKey,
        right_key: rightKey,
        schema,
        sample_size: sampleSize,
      });
      setAnalysisByEdge((prev) => ({
        ...prev,
        [analysisKey(edge, flipEdgeDirection)]: response.data,
      }));
      setStatusMessage('Join analysis ready.');
    } catch (error) {
      setStatusMessage('Join analysis failed.');
    }
  };

  const startChainFromEdge = (edge) => {
    if (!edge) return;
    const effectiveEdge = resolveEdge(edge);
    setBaseTable(effectiveEdge.left_table);
    setChainSteps([
      {
        id: edge.id,
        rightTable: effectiveEdge.right_table,
        leftKey: effectiveEdge.left_columns[0],
        rightKey: effectiveEdge.right_columns[0],
        leftKeys: effectiveEdge.left_columns,
        rightKeys: effectiveEdge.right_columns,
      },
    ]);
    setStatusMessage('Chain started from selected edge.');
  };

  const appendEdgeToChain = (edge) => {
    if (!edge) return;
    const tail = chainSteps.length ? chainSteps[chainSteps.length - 1].rightTable : baseTable;
    const effectiveEdge = resolveEdge(edge);
    if (!tail) {
      startChainFromEdge(edge);
      return;
    }
    if (tail !== effectiveEdge.left_table) {
      setStatusMessage('Edge does not connect to chain tail.');
      return;
    }
    setChainSteps((prev) => [
      ...prev,
      {
        id: `${edge.id}-${prev.length}`,
        rightTable: effectiveEdge.right_table,
        leftKey: effectiveEdge.left_columns[0],
        rightKey: effectiveEdge.right_columns[0],
        leftKeys: effectiveEdge.left_columns,
        rightKeys: effectiveEdge.right_columns,
      },
    ]);
    setStatusMessage('Edge appended to chain.');
  };

  const clearChain = () => {
    setBaseTable('');
    setChainSteps([]);
    setStatusMessage('Chain cleared.');
  };

  const sqlPreview = useMemo(
    () => buildJoinSql(baseTable, chainSteps, joinType, schema),
    [baseTable, chainSteps, joinType, schema]
  );

  const maxEstimate = Math.max(1, ...filteredNodes.map((node) => node.row_estimate || 0));
  const nodeSize = (estimate) => {
    const scaled = Math.log10((estimate || 0) + 1) / Math.log10(maxEstimate + 1);
    return 6 + scaled * 10;
  };

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <div className="terminal-header flex items-center justify-between">
        <span>JOIN EXPLORER</span>
        <span className="text-[11px] text-[#707070]">
          FK edges: {fkEdgeCount} | Inferred: {inferredEdgeCount} | Tables: {filteredNodes.length}
        </span>
      </div>

      <div className="flex-1 overflow-auto p-4">
        <div className="grid grid-cols-12 gap-3">
          <div className="col-span-12 xl:col-span-8 bg-[#121212] border border-[#2a2a2a]">
            <div className="terminal-header flex items-center justify-between">
              <span>{graphTitle}</span>
              <span className="text-[10px] text-[#707070]">{graphSubtitle}</span>
            </div>
            <div className="p-3 space-y-3">
              <div className="grid grid-cols-4 gap-3 text-[10px] text-[#808080]">
                <div className="col-span-2">
                  <div className="mb-1">FILTER TABLES</div>
                  <input
                    className="input-terminal w-full text-[11px]"
                    value={searchTerm}
                    onChange={(event) => setSearchTerm(event.target.value)}
                    placeholder="Search table..."
                  />
                </div>
                <div className="flex items-end gap-2 col-span-2">
                  <label className="flex items-center gap-2 text-[10px] text-[#808080]">
                    <input
                      type="checkbox"
                      checked={showRiskyEdges}
                      onChange={(event) => setShowRiskyEdges(event.target.checked)}
                    />
                    Show risky edges
                  </label>
                  <label className="flex items-center gap-2 text-[10px] text-[#808080]">
                    <input
                      type="checkbox"
                      checked={showInferredEdges}
                      onChange={(event) => {
                        const next = event.target.checked;
                        setShowInferredEdges(next);
                        loadGraph(next);
                      }}
                    />
                    Show inferred edges
                  </label>
                  <label className="flex items-center gap-2 text-[10px] text-[#808080]">
                    <input
                      type="checkbox"
                      checked={showIsolatedNodes}
                      onChange={(event) => setShowIsolatedNodes(event.target.checked)}
                    />
                    Show isolated tables
                  </label>
                </div>
              </div>
              <div className="grid grid-cols-3 gap-3 text-[10px] text-[#808080]">
                <div>
                  <div className="mb-1">SAMPLE SIZE</div>
                  <input
                    className="input-terminal w-full text-[11px]"
                    type="number"
                    min="1000"
                    step="1000"
                    value={sampleSize}
                    onChange={(event) => setSampleSize(Number(event.target.value))}
                  />
                </div>
                <div className="flex items-end">
                  <button
                    type="button"
                    onClick={loadGraph}
                    className="btn-terminal text-[10px] py-1 w-full"
                  >
                    REFRESH GRAPH
                  </button>
                </div>
                <div className="flex items-end text-[10px] text-[#707070]">
                  {graphLoading ? 'Scanning FK metadata...' : 'Metadata cached for session.'}
                </div>
              </div>

              {statusMessage && (
                <div className="text-[10px] text-[#707070] bg-[#0f0f0f] border border-[#1f1f1f] px-2 py-1">
                  {statusMessage}
                </div>
              )}

              {graphError && <div className="text-[10px] text-[#ff1744]">{graphError}</div>}
              {!graphLoading && graphEdges.length === 0 && (
                <div className="text-[11px] text-[#606060] border border-dashed border-[#2a2a2a] p-4">
                  No FK constraints detected and no inferred joins found.
                </div>
              )}
              {!graphLoading && graphEdges.length > 0 && filteredEdges.length === 0 && (
                <div className="text-[11px] text-[#606060] border border-dashed border-[#2a2a2a] p-4">
                  No edges match the current filters.
                </div>
              )}

              <div className="bg-[#0f0f0f] border border-[#1f1f1f] p-2">
                <svg
                  viewBox={`0 0 ${nodePositions.width} ${nodePositions.height}`}
                  width="100%"
                  height="420"
                >
                  {filteredEdges.map((edge) => {
                    const leftPos = nodePositions.positions[edge.left_table];
                    const rightPos = nodePositions.positions[edge.right_table];
                    if (!leftPos || !rightPos) return null;
                    const isSelected = selectedEdgeId === edge.id;
                    const isHovered = hoveredEdgeId === edge.id;
                    const confidence = edge.source === 'inferred' ? (edge.confidence ?? 0.4) : null;
                    const inferredColor =
                      confidence >= 0.8 ? '#00c853' : confidence >= 0.6 ? '#ffc107' : '#ff1744';
                    const stroke = isSelected
                      ? '#ff6b00'
                      : edge.source === 'inferred'
                        ? inferredColor
                        : getEdgeColor(edge);
                    return (
                      <line
                        key={edge.id}
                        x1={leftPos.x}
                        y1={leftPos.y}
                        x2={rightPos.x}
                        y2={rightPos.y}
                        stroke={stroke}
                        strokeWidth={isSelected ? 2.4 : 1.3}
                        strokeDasharray={edge.source === 'inferred' ? '4 3' : '0'}
                        opacity={isHovered || isSelected ? 1 : 0.6}
                        onMouseEnter={() => setHoveredEdgeId(edge.id)}
                        onMouseLeave={() => setHoveredEdgeId(null)}
                        onClick={() => {
                          setSelectedEdgeId(edge.id);
                          setSelectedNodeId(null);
                        }}
                        style={{ cursor: 'pointer' }}
                      />
                    );
                  })}

                  {filteredNodes.map((node) => {
                    const pos = nodePositions.positions[node.table_name];
                    if (!pos) return null;
                    const isSelected = selectedNodeId === node.table_name;
                    const radius = nodeSize(node.row_estimate);
                    return (
                      <g
                        key={node.table_name}
                        onClick={() => {
                          setSelectedNodeId(node.table_name);
                          setSelectedEdgeId(null);
                        }}
                        style={{ cursor: 'pointer' }}
                      >
                        <circle
                          cx={pos.x}
                          cy={pos.y}
                          r={radius + (isSelected ? 2 : 0)}
                          fill={isSelected ? '#ff6b00' : '#1f1f1f'}
                          stroke="#3a3a3a"
                          strokeWidth="1"
                        />
                        <text
                          x={pos.x}
                          y={pos.y - radius - 6}
                          textAnchor="middle"
                          fontSize="9"
                          fill="#cfcfcf"
                        >
                          {node.table_name}
                        </text>
                      </g>
                    );
                  })}
                </svg>
              </div>
            </div>
          </div>

          <div className="col-span-12 xl:col-span-4 bg-[#121212] border border-[#2a2a2a]">
            <div className="terminal-header">EDGE EVIDENCE</div>
            <div className="p-3 space-y-3">
              {!selectedEdge && (
                <div className="text-[11px] text-[#606060]">
                  Click a graph edge to inspect FK evidence and build a chain.
                </div>
              )}

              {selectedEdge && (
                <>
                  <div className="flex items-center justify-between text-[10px] text-[#808080]">
                    <span>DIRECTION</span>
                    <label className="flex items-center gap-2">
                      <input
                        type="checkbox"
                        checked={flipEdgeDirection}
                        onChange={(event) => setFlipEdgeDirection(event.target.checked)}
                      />
                      Flip edge
                    </label>
                  </div>
                  <div className="border border-[#1f1f1f] bg-[#0f0f0f] p-3 space-y-2">
                    <div className="text-[10px] text-[#808080]">CONSTRAINT</div>
                    <div className="text-[12px] text-[#cfcfcf]">
                      {selectedEdge.constraint_name}
                      {selectedEdge.source === 'inferred' ? ' (INFERRED)' : ''}
                    </div>
                    <div className="text-[10px] text-[#707070]">
                      {resolveEdge(selectedEdge).left_table} -> {resolveEdge(selectedEdge).right_table}
                    </div>
                    <div className="text-[10px] text-[#808080]">
                      Keys:
                      {resolveEdge(selectedEdge).left_columns.map((col, idx) => (
                        <div key={`${col}-${resolveEdge(selectedEdge).right_columns[idx]}`}>
                          {col} = {resolveEdge(selectedEdge).right_columns[idx]}
                        </div>
                      ))}
                    </div>
                    <div className="text-[10px] text-[#808080]">
                      Unique reference: {selectedEdge.is_primary || selectedEdge.is_unique ? 'YES' : 'NO'}
                    </div>
                    <div className="text-[10px] text-[#808080]">
                      Estimates: {formatCompactNumber(resolveEdge(selectedEdge).left_estimate)} rows ->{' '}
                      {formatCompactNumber(resolveEdge(selectedEdge).right_estimate)} rows
                    </div>
                    {selectedEdge.source === 'inferred' && (
                      <div className="text-[10px] text-[#808080]">
                        Confidence: {formatPercent(selectedEdge.confidence ?? 0)}
                      </div>
                    )}
                    {selectedEdge.reason && (
                      <div className="text-[10px] text-[#707070]">Reason: {selectedEdge.reason}</div>
                    )}
                  </div>

                  {resolveEdge(selectedEdge).left_columns.length > 1 && (
                    <div className="text-[10px] text-[#ffc107]">
                      Composite FK detected. Sample analysis uses first key only.
                    </div>
                  )}

                  <div className="grid grid-cols-2 gap-2">
                    <button
                      type="button"
                      onClick={() => handleAnalyzeEdge(selectedEdge)}
                      className="btn-terminal text-[10px] py-1"
                    >
                      ANALYZE SAMPLE
                    </button>
                    <button
                      type="button"
                      onClick={() => startChainFromEdge(selectedEdge)}
                      className="btn-terminal text-[10px] py-1"
                    >
                      START CHAIN
                    </button>
                    <button
                      type="button"
                      onClick={() => appendEdgeToChain(selectedEdge)}
                      className="btn-terminal text-[10px] py-1 col-span-2"
                    >
                      APPEND TO CHAIN
                    </button>
                  </div>

                  {selectedAnalysis && (
                    <div className="border border-[#1f1f1f] bg-[#0f0f0f] p-3 space-y-3">
                      <div className="text-[10px] text-[#808080]">SAMPLE METRICS</div>
                      <MetricBar
                        label="Left match rate"
                        value={selectedAnalysis.match.left_match_rate}
                        color={getMatchColor(selectedAnalysis.match.left_match_rate)}
                      />
                      <MetricBar
                        label="Right match rate"
                        value={selectedAnalysis.match.right_match_rate}
                        color={getMatchColor(selectedAnalysis.match.right_match_rate)}
                      />
                      <MetricBar
                        label="Left null rate"
                        value={selectedAnalysis.left.null_pct}
                        color={getRiskColor(selectedAnalysis.left.null_pct)}
                      />
                      <MetricBar
                        label="Right null rate"
                        value={selectedAnalysis.right.null_pct}
                        color={getRiskColor(selectedAnalysis.right.null_pct)}
                      />
                      <div className="text-[10px] text-[#707070]">
                        Cardinality: {selectedAnalysis.cardinality}
                      </div>
                    </div>
                  )}
                </>
              )}
            </div>
          </div>

          <div className="col-span-12 bg-[#121212] border border-[#2a2a2a]">
            <div className="terminal-header flex items-center justify-between">
              <span>CHAIN BUILDER</span>
              <div className="flex items-center gap-2 text-[10px] text-[#808080]">
                <span>JOIN TYPE</span>
                <select
                  className="input-terminal text-[10px]"
                  value={joinType}
                  onChange={(event) => setJoinType(event.target.value)}
                >
                  <option value="left">LEFT</option>
                  <option value="inner">INNER</option>
                  <option value="right">RIGHT</option>
                  <option value="full">FULL</option>
                </select>
                <button
                  type="button"
                  onClick={clearChain}
                  className="btn-terminal text-[10px] py-1"
                >
                  CLEAR
                </button>
              </div>
            </div>
            <div className="p-3 space-y-2 text-[11px] text-[#cfcfcf]">
              {baseTable ? (
                <div className="text-[10px] text-[#808080]">
                  BASE: <span className="text-[#cfcfcf]">{baseTable}</span>
                </div>
              ) : (
                <div className="text-[10px] text-[#606060]">No base table selected.</div>
              )}
              {chainSteps.length === 0 && (
                <div className="text-[10px] text-[#606060]">
                  Click an edge in the graph and start a chain.
                </div>
              )}
              {chainSteps.map((step, index) => (
                <div key={step.id} className="border border-[#1f1f1f] bg-[#0f0f0f] p-2">
                  <div className="text-[10px] text-[#808080]">STEP {index + 1}</div>
                  <div>{step.rightTable}</div>
                  <div className="text-[10px] text-[#707070]">
                    {step.leftKeys?.length
                      ? step.leftKeys.map((key, idx) => `${key}=${step.rightKeys[idx]}`).join(', ')
                      : `${step.leftKey}=${step.rightKey}`}
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="col-span-12 bg-[#121212] border border-[#2a2a2a]">
            <div className="terminal-header">SQL PREVIEW</div>
            <div className="p-3">
              <pre className="text-[11px] text-[#cfcfcf] bg-[#0f0f0f] border border-[#1f1f1f] p-3 overflow-auto">
                {sqlPreview}
              </pre>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
