import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import apiClient from '../../api/client';

export function FeatureLab({ 
  isOpen, 
  onClose, 
  dbSessionId, 
  tables = [],
  onSqlGenerated 
}) {
  // Step tracking: 1=Define Goal, 2=Define Target, 3=Select Features, 4=Get SQL
  const [step, setStep] = useState(1);

  
  const [selectedTable, setSelectedTable] = useState('');
  const [targetGoal, setTargetGoal] = useState('');
  const [useSmartMode, setUseSmartMode] = useState(true);
  const [manualGroupingColumn, setManualGroupingColumn] = useState('');
  const [availableColumns, setAvailableColumns] = useState([]);

  
  const [targetColumns, setTargetColumns] = useState([]); // Candidate columns for target
  const [selectedTargetColumn, setSelectedTargetColumn] = useState(null);
  const [columnValues, setColumnValues] = useState([]); // Values in selected column
  const [selectedValues, setSelectedValues] = useState(new Set()); // Values user picks as positive class
  const [targetSql, setTargetSql] = useState(null);
  const [targetPreview, setTargetPreview] = useState(null);
  const [isPreviewLoading, setIsPreviewLoading] = useState(false);
  const [isLoadingColumns, setIsLoadingColumns] = useState(false);
  const [isLoadingValues, setIsLoadingValues] = useState(false);

  
  const [suggestions, setSuggestions] = useState([]);
  const [selectedFeatures, setSelectedFeatures] = useState(new Set());
  const [groupingColumn, setGroupingColumn] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);

  
  const [generatedSql, setGeneratedSql] = useState('');
  const [isCopied, setIsCopied] = useState(false);
  const [sqlValidation, setSqlValidation] = useState(null);

  // Step labels for the indicator
  const stepLabels = ['DEFINE GOAL', 'DEFINE TARGET', 'SELECT FEATURES', 'GET SQL'];

  // Reset when modal opens
  useEffect(() => {
    if (isOpen) {
      setStep(1);
      setSelectedTable('');
      setTargetGoal('');
      setTargetColumns([]);
      setSelectedTargetColumn(null);
      setColumnValues([]);
      setSelectedValues(new Set());
      setTargetSql(null);
      setTargetPreview(null);
      setSuggestions([]);
      setSelectedFeatures(new Set());
      setGeneratedSql('');
      setError(null);
      setManualGroupingColumn('');
      setAvailableColumns([]);
      setSqlValidation(null);
    }
  }, [isOpen]);

  // Fetch columns when table is selected
  useEffect(() => {
    const fetchColumns = async () => {
      if (!selectedTable || !dbSessionId) {
        setAvailableColumns([]);
        setManualGroupingColumn('');
        return;
      }

      try {
        const response = await apiClient.post('/table-columns', {
          session_id: dbSessionId,
          table_name: selectedTable,
        });

        console.log('Table columns response:', response.data);

        if (response.data?.columns && Array.isArray(response.data.columns)) {
          const cols = response.data.columns.map(col => col.name).filter(Boolean);
          setAvailableColumns(cols);
          
          const idPatterns = [
            'client_id', 'user_id', 'customer_id', 'account_id', 
            'clientid', 'userid', 'customerid', 'accountid',
            'client', 'user', 'customer', 'account', 'id'
          ];
          
          let detectedCol = cols.find(col => 
            idPatterns.includes(col.toLowerCase())
          );
          
          if (!detectedCol) {
            detectedCol = cols.find(col => 
              idPatterns.some(pattern => col.toLowerCase().includes(pattern))
            );
          }
          
          if (detectedCol) {
            setManualGroupingColumn(detectedCol);
          }
        }
      } catch (err) {
        console.error('Failed to fetch columns:', err);
        setAvailableColumns([]);
      }
    };

    fetchColumns();
  }, [selectedTable, dbSessionId]);

  
  const handleProceedToTarget = async () => {
    if (!selectedTable || !targetGoal.trim() || !manualGroupingColumn) {
      setError('Please fill in all required fields.');
      return;
    }
    setError(null);
    setGroupingColumn(manualGroupingColumn);
    setStep(2);

    // Detect target column candidates
    await detectTargetColumns();
  };

  // Detect columns that could be used for target definition
  const detectTargetColumns = async () => {
    if (!selectedTable || !dbSessionId) return;

    setIsLoadingColumns(true);
    try {
      const response = await apiClient.post('/detect-target-columns', {
        session_id: dbSessionId,
        table_name: selectedTable,
      });

      console.log('Target columns response:', response.data);
      setTargetColumns(response.data.candidates || []);
      
      // Auto-select first status-like column if available
      const statusColumn = response.data.candidates?.find(c => c.is_status_like);
      if (statusColumn) {
        await selectTargetColumn(statusColumn.column_name);
      }
    } catch (err) {
      console.error('Failed to detect target columns:', err);
      setError(err.response?.data?.detail || 'Failed to detect target columns');
    } finally {
      setIsLoadingColumns(false);
    }
  };

  // Select a column and load its values
  const selectTargetColumn = async (columnName) => {
    setSelectedTargetColumn(columnName);
    setColumnValues([]);
    setSelectedValues(new Set());
    setTargetSql(null);
    setTargetPreview(null);
    setIsLoadingValues(true);

    try {
      const response = await apiClient.post('/get-column-values', {
        session_id: dbSessionId,
        table_name: selectedTable,
        column_name: columnName,
        limit: 50,
      });

      console.log('Column values response:', response.data);
      setColumnValues(response.data.values || []);
    } catch (err) {
      console.error('Failed to get column values:', err);
      setError(err.response?.data?.detail || 'Failed to get column values');
    } finally {
      setIsLoadingValues(false);
    }
  };

  // Toggle a value selection for positive class
  const toggleValueSelection = (value) => {
    const newSelected = new Set(selectedValues);
    if (newSelected.has(value)) {
      newSelected.delete(value);
    } else {
      newSelected.add(value);
    }
    setSelectedValues(newSelected);
    // Clear previous target SQL when selection changes
    setTargetSql(null);
    setTargetPreview(null);
  };

  
  const handleGenerateTarget = async () => {
    if (selectedValues.size === 0 || !selectedTargetColumn) {
      setError('Please select at least one value for the positive class.');
      return;
    }

    setIsLoading(true);
    setError(null);
    setTargetPreview(null);

    try {
      // Generate target SQL from selected values
      const response = await apiClient.post('/generate-target', {
        session_id: dbSessionId,
        table_name: selectedTable,
        column_name: selectedTargetColumn,
        selected_values: Array.from(selectedValues),
        grouping_column: manualGroupingColumn,
      });

      console.log('Generate target response:', response.data);
      setTargetSql(response.data);

      // Automatically preview distribution
      await previewTargetDistribution(response.data);
    } catch (err) {
      console.error('Generate target error:', err);
      setError(err.response?.data?.detail || 'Failed to generate target');
    } finally {
      setIsLoading(false);
    }
  };

  // Preview target distribution
  const previewTargetDistribution = async (targetData) => {
    if (!targetData?.sql_logic) return;

    setIsPreviewLoading(true);
    try {
      const response = await apiClient.post('/preview-target', {
        session_id: dbSessionId,
        table_name: selectedTable,
        sql_logic: targetData.sql_logic,
        target_name: targetData.target_name,
        grouping_column: manualGroupingColumn,
      });

      console.log('Target preview response:', response.data);
      setTargetPreview(response.data);
    } catch (err) {
      console.error('Preview target error:', err);
      setTargetPreview({
        status: 'error',
        warnings: [{ 
          severity: 'medium', 
          code: 'PREVIEW_FAILED',
          message: 'Could not preview target distribution',
          detail: err.response?.data?.detail || 'Failed to query the database'
        }],
        is_usable: true
      });
    } finally {
      setIsPreviewLoading(false);
    }
  };

  
  const handleConfirmTarget = async () => {
    if (!targetSql || !targetPreview?.is_usable) {
      setError('Please generate a valid target definition first.');
      return;
    }

    setIsLoading(true);
    setError(null);

    try {
      const endpoint = useSmartMode ? '/suggest-features-smart' : '/suggest-features';
      const response = await apiClient.post(endpoint, {
        session_id: dbSessionId,
        table_name: selectedTable,
        target_goal: targetGoal.trim(),
      });

      console.log('Feature suggestions:', response.data);

      const { suggestions: featureSuggestions } = response.data;
      setSuggestions(featureSuggestions || []);
      
      // Auto-select high relevance features
      const autoSelected = new Set();
      featureSuggestions?.forEach((f, idx) => {
        if (f.relevance === 'critical' || f.relevance === 'high') {
          autoSelected.add(idx);
        }
      });
      setSelectedFeatures(autoSelected);
      
      setStep(3);
    } catch (err) {
      console.error('Feature suggestion error:', err);
      setError(err.response?.data?.detail || 'Failed to generate suggestions');
    } finally {
      setIsLoading(false);
    }
  };

  
  const handleGenerateSql = async () => {
    if (selectedFeatures.size === 0) return;

    setIsLoading(true);
    setError(null);

    try {
      const selected = Array.from(selectedFeatures).map(idx => suggestions[idx]);

      const response = await apiClient.post('/generate-dataset', {
        session_id: dbSessionId,
        table_name: selectedTable,
        selected_features: selected,
        grouping_column: groupingColumn,
      });

      console.log('Generated SQL:', response.data);

      // Combine features SQL with target SQL
      let finalSql = response.data.sql_query;
      
      // If we have a target, inject it into the SQL
      if (targetSql?.sql_logic) {
        // Insert the target column after the grouping column
        const targetLine = `    ${targetSql.sql_logic} AS "${targetSql.target_name}",`;
        
        // Find the first line after SELECT and inject target
        const lines = finalSql.split('\n');
        const selectIdx = lines.findIndex(line => line.trim().startsWith('SELECT'));
        if (selectIdx !== -1 && lines.length > selectIdx + 1) {
          // Insert after the first column (grouping column)
          lines.splice(selectIdx + 2, 0, targetLine);
          finalSql = lines.join('\n');
        }
      }

      setGeneratedSql(finalSql);
      setSqlValidation(response.data.validation);
      setStep(4);
    } catch (err) {
      console.error('Generate SQL error:', err);
      setError(err.response?.data?.detail || 'Failed to generate SQL');
    } finally {
      setIsLoading(false);
    }
  };

  // Copy SQL to clipboard
  const handleCopySql = async () => {
    try {
      await navigator.clipboard.writeText(generatedSql);
      setIsCopied(true);
      setTimeout(() => setIsCopied(false), 2000);
    } catch (err) {
      console.error('Copy failed:', err);
    }
  };

  // Toggle feature selection
  const toggleFeature = (idx) => {
    const newSelected = new Set(selectedFeatures);
    if (newSelected.has(idx)) {
      newSelected.delete(idx);
    } else {
      newSelected.add(idx);
    }
    setSelectedFeatures(newSelected);
  };

  // Select all / none
  const toggleAll = () => {
    if (selectedFeatures.size === suggestions.length) {
      setSelectedFeatures(new Set());
    } else {
      setSelectedFeatures(new Set(suggestions.map((_, idx) => idx)));
    }
  };

  // Get relevance badge color
  const getRelevanceColor = (relevance) => {
    switch (relevance) {
      case 'critical': return { bg: '#ff174420', text: '#ff1744', label: 'CRITICAL' };
      case 'high': return { bg: '#ff6b0020', text: '#ff6b00', label: 'HIGH' };
      case 'medium': return { bg: '#ffc10720', text: '#ffc107', label: 'MED' };
      default: return { bg: '#60606020', text: '#606060', label: 'LOW' };
    }
  };

  // Get feature type icon
  const getTypeIcon = (type) => {
    switch (type) {
      case 'aggregation': return '∑';
      case 'recency': return '⏱';
      case 'frequency': return '#';
      case 'categorical': return '◆';
      case 'ratio': return '%';
      case 'duration': return '↔';
      case 'cardinality': return '◇';
      default: return '•';
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 backdrop-blur-sm">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        exit={{ opacity: 0, scale: 0.95 }}
        className="w-full max-w-4xl max-h-[90vh] bg-[#0a0a0a] border border-[#2a2a2a] shadow-2xl flex flex-col"
      >
        {/* Header */}
        <div className="terminal-header flex items-center justify-between">
          <div className="flex items-center gap-3">
            <span className="text-lg">⚗️</span>
            <span>FEATURE ENGINEERING LAB</span>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="text-black hover:bg-[#cc5500] px-2 py-0.5 text-[10px] font-bold"
          >
            [X] CLOSE
          </button>
        </div>

        {/* Step Indicator */}
        <div className="flex items-center px-4 py-2 bg-[#121212] border-b border-[#2a2a2a] overflow-x-auto">
          {[1, 2, 3, 4].map((s) => (
            <div key={s} className="flex items-center flex-shrink-0">
              <div className={`flex items-center justify-center w-6 h-6 rounded-full text-[10px] font-bold ${
                step === s 
                  ? 'bg-[#ff6b00] text-black' 
                  : step > s 
                    ? 'bg-[#00c853] text-black' 
                    : 'bg-[#2a2a2a] text-[#606060]'
              }`}>
                {step > s ? '✓' : s}
              </div>
              <span className={`ml-2 text-[10px] font-mono whitespace-nowrap ${
                step === s ? 'text-[#ff6b00]' : 'text-[#606060]'
              }`}>
                {stepLabels[s - 1]}
              </span>
              {s < 4 && <div className="w-6 h-px bg-[#2a2a2a] mx-2" />}
            </div>
          ))}
        </div>

        {/* Content */}
        <div className="flex-1 overflow-auto p-4">
          <AnimatePresence mode="wait">
            
            {step === 1 && (
              <motion.div
                key="step1"
                initial={{ opacity: 0, x: 20 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: -20 }}
                className="space-y-6"
              >
                {/* Table Selection */}
                <div>
                  <label className="block text-[11px] text-[#808080] uppercase tracking-wide mb-2">
                    SOURCE TABLE
                  </label>
                  <select
                    value={selectedTable}
                    onChange={(e) => setSelectedTable(e.target.value)}
                    className="input-terminal w-full"
                  >
                    <option value="">-- Select a table --</option>
                    {tables.map((table) => (
                      <option key={table} value={table}>{table}</option>
                    ))}
                  </select>
                </div>

                {/* Target Goal */}
                <div>
                  <label className="block text-[11px] text-[#808080] uppercase tracking-wide mb-2">
                    PREDICTION TARGET
                  </label>
                  <input
                    type="text"
                    value={targetGoal}
                    onChange={(e) => setTargetGoal(e.target.value)}
                    placeholder="e.g., Customer Churn, Credit Default, Fraud Detection..."
                    className="input-terminal w-full"
                  />
                  <p className="text-[10px] text-[#505050] mt-1">
                    Describe what you want to predict. This helps prioritize relevant features.
                  </p>
                </div>

                {/* Grouping Column Selector */}
                <div>
                  <label className="block text-[11px] text-[#808080] uppercase tracking-wide mb-2">
                    GROUP BY COLUMN <span className="text-[#ff6b00]">*</span>
                  </label>
                  <select
                    value={manualGroupingColumn}
                    onChange={(e) => setManualGroupingColumn(e.target.value)}
                    className="input-terminal w-full"
                    disabled={!selectedTable}
                  >
                    <option value="">-- Select grouping column --</option>
                    {availableColumns.map((col) => (
                      <option key={col} value={col}>{col}</option>
                    ))}
                  </select>
                  <p className="text-[10px] text-[#505050] mt-1">
                    Features will be aggregated per unique value of this column (e.g., client_id, user_id).
                    {!manualGroupingColumn && selectedTable && (
                      <span className="text-[#ff6b00]"> ⚠️ Required for valid SQL!</span>
                    )}
                  </p>
                </div>

                {/* Smart Mode Toggle */}
                <div className="flex items-center justify-between p-3 bg-[#121212] border border-[#2a2a2a] rounded">
                  <div>
                    <div className="text-[11px] text-[#e0e0e0] font-bold">
                      🧠 AI-POWERED SUGGESTIONS
                    </div>
                    <div className="text-[10px] text-[#606060]">
                      Use GPT-4 for smarter, context-aware feature recommendations
                    </div>
                  </div>
                  <button
                    type="button"
                    onClick={() => setUseSmartMode(!useSmartMode)}
                    className={`w-12 h-6 rounded-full transition-colors relative ${
                      useSmartMode ? 'bg-[#ff6b00]' : 'bg-[#2a2a2a]'
                    }`}
                  >
                    <div className={`absolute top-1 w-4 h-4 bg-white rounded-full transition-all ${
                      useSmartMode ? 'left-7' : 'left-1'
                    }`} />
                  </button>
                </div>

                {error && (
                  <div className="p-3 bg-[#ff174420] border border-[#ff1744] text-[#ff1744] text-[11px]">
                    ❌ {error}
                  </div>
                )}
              </motion.div>
            )}

            
            {step === 2 && (
              <motion.div
                key="step2"
                initial={{ opacity: 0, x: 20 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: -20 }}
                className="space-y-4"
              >
                {/* Context Bar */}
                <div className="flex items-center gap-4 p-3 bg-[#121212] border border-[#2a2a2a]">
                  <div className="text-[10px] text-[#808080]">
                    TABLE: <span className="text-[#ff6b00]">{selectedTable}</span>
                  </div>
                  <div className="text-[10px] text-[#808080]">
                    GOAL: <span className="text-[#00c853]">{targetGoal}</span>
                  </div>
                  <div className="text-[10px] text-[#808080]">
                    GROUP BY: <span className="text-[#2196f3]">{groupingColumn}</span>
                  </div>
                </div>

                {/* Loading State */}
                {isLoadingColumns && (
                  <div className="p-6 bg-[#121212] border border-[#2a2a2a] text-center">
                    <div className="text-[12px] text-[#ffc107] animate-pulse">
                      ⏳ DETECTING TARGET COLUMNS...
                    </div>
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
                          onClick={() => selectTargetColumn(col.column_name)}
                          className={`px-3 py-2 text-[11px] font-mono border transition-all ${
                            selectedTargetColumn === col.column_name
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
                      ⚠️ No suitable status columns detected in this table.
                    </div>
                    <p className="text-[10px] text-[#808080] mt-1">
                      Try selecting a different table or check the column types.
                    </p>
                  </div>
                )}

                {/* Value Selection */}
                {isLoadingValues && (
                  <div className="p-6 bg-[#121212] border border-[#2a2a2a] text-center">
                    <div className="text-[12px] text-[#ffc107] animate-pulse">
                      ⏳ LOADING COLUMN VALUES...
                    </div>
                  </div>
                )}

                {!isLoadingValues && selectedTargetColumn && columnValues.length > 0 && !targetSql && (
                  <div className="p-4 bg-[#121212] border border-[#2a2a2a]">
                    <div className="text-[11px] text-[#808080] uppercase tracking-wide mb-3">
                      🎯 SELECT VALUES FOR POSITIVE CLASS (TARGET = 1)
                    </div>
                    <p className="text-[10px] text-[#606060] mb-4">
                      Check the values that indicate "{targetGoal}". These will become <span className="text-[#00c853]">1</span> in your target.
                    </p>
                    
                    <div className="space-y-2 max-h-64 overflow-y-auto">
                      {columnValues.map((item) => {
                        const isSelected = selectedValues.has(item.value);
                        const displayValue = item.is_null ? '(NULL)' : item.value;
                        
                        return (
                          <div
                            key={item.value}
                            onClick={() => toggleValueSelection(item.value)}
                            className={`flex items-center justify-between p-3 border cursor-pointer transition-all ${
                              isSelected
                                ? 'bg-[#00c85320] border-[#00c853]'
                                : 'bg-[#0a0a0a] border-[#2a2a2a] hover:border-[#3a3a3a]'
                            }`}
                          >
                            <div className="flex items-center gap-3">
                              <div className={`w-5 h-5 border flex items-center justify-center text-[10px] ${
                                isSelected
                                  ? 'bg-[#00c853] border-[#00c853] text-black'
                                  : 'border-[#606060]'
                              }`}>
                                {isSelected && '✓'}
                              </div>
                              <span className={`text-[12px] font-mono ${
                                item.is_null ? 'text-[#808080] italic' : 'text-[#e0e0e0]'
                              }`}>
                                {displayValue}
                              </span>
                            </div>
                            <div className="flex items-center gap-4">
                              <span className="text-[11px] text-[#808080]">
                                {item.count.toLocaleString()}
                              </span>
                              <div className="w-24 h-2 bg-[#0a0a0a] border border-[#2a2a2a] overflow-hidden">
                                <div
                                  className="h-full bg-[#ff6b00]"
                                  style={{ width: `${Math.min(item.percentage, 100)}%` }}
                                />
                              </div>
                              <span className="text-[10px] text-[#606060] w-14 text-right">
                                {item.percentage.toFixed(1)}%
                              </span>
                            </div>
                          </div>
                        );
                      })}
                    </div>

                    {/* Generate Button */}
                    <div className="mt-4 flex items-center justify-between">
                      <span className="text-[10px] text-[#808080]">
                        {selectedValues.size} value(s) selected as positive class
                      </span>
                      <button
                        type="button"
                        onClick={handleGenerateTarget}
                        disabled={selectedValues.size === 0 || isLoading}
                        className="btn-terminal text-[11px] px-6 py-2 disabled:opacity-50"
                      >
                        {isLoading ? '⏳ GENERATING...' : '📝 GENERATE TARGET'}
                      </button>
                    </div>
                  </div>
                )}

                {/* Target SQL Result & Preview */}
                {targetSql && (
                  <div className="space-y-4">
                    {/* Target Details */}
                    <div className="p-4 bg-[#121212] border border-[#2a2a2a] space-y-3">
                      <div className="flex items-center justify-between">
                        <div>
                          <div className="text-[10px] text-[#808080] uppercase mb-1">TARGET NAME</div>
                          <div className="text-[14px] text-[#ff6b00] font-mono font-bold">{targetSql.target_name}</div>
                        </div>
                        <button
                          type="button"
                          onClick={() => {
                            setTargetSql(null);
                            setTargetPreview(null);
                          }}
                          className="btn-terminal text-[10px] px-3"
                        >
                          ↻ CHANGE
                        </button>
                      </div>
                      <div>
                        <div className="text-[10px] text-[#808080] uppercase mb-1">DESCRIPTION</div>
                        <div className="text-[11px] text-[#e0e0e0]">{targetSql.description}</div>
                      </div>
                      <div>
                        <div className="text-[10px] text-[#808080] uppercase mb-1">SQL LOGIC</div>
                        <pre className="bg-[#0a0a0a] border border-[#2a2a2a] p-3 text-[11px] font-mono text-[#00bcd4] overflow-x-auto">
                          {targetSql.sql_logic}
                        </pre>
                      </div>
                    </div>

                    {/* Target Distribution Preview */}
                    {isPreviewLoading ? (
                      <div className="p-4 bg-[#121212] border border-[#2a2a2a] text-center">
                        <div className="text-[11px] text-[#ffc107] animate-pulse">
                          ⏳ CHECKING TARGET DISTRIBUTION...
                        </div>
                      </div>
                    ) : targetPreview && (
                      <div className={`p-4 border ${
                        !targetPreview.is_usable 
                          ? 'bg-[#ff174420] border-[#ff1744]' 
                          : targetPreview.warnings?.length > 0 
                            ? 'bg-[#ff6b0020] border-[#ff6b00]' 
                            : 'bg-[#00c85310] border-[#00c853]'
                      }`}>
                        {/* Header */}
                        <div className="flex items-center justify-between mb-4">
                          <div className="text-[12px] font-bold flex items-center gap-2">
                            {!targetPreview.is_usable ? (
                              <span className="text-[#ff1744]">🚫 CRITICAL: TARGET UNUSABLE</span>
                            ) : targetPreview.warnings?.length > 0 ? (
                              <span className="text-[#ff6b00]">⚠️ TARGET DISTRIBUTION WARNING</span>
                            ) : (
                              <span className="text-[#00c853]">✓ TARGET DISTRIBUTION LOOKS GOOD</span>
                            )}
                          </div>
                          <div className="text-[10px] text-[#808080]">
                            TOTAL: {targetPreview.total_records?.toLocaleString() || 'N/A'} records
                          </div>
                        </div>

                        {/* Distribution Bars */}
                        {targetPreview.distribution && (
                          <div className="space-y-2 mb-4">
                            {targetPreview.distribution.map((d) => {
                              const barColor = d.value === 1 ? '#00c853' : '#ff6b00';
                              const barWidth = Math.max(d.percentage, 2);
                              return (
                                <div key={d.value} className="flex items-center gap-3">
                                  <div className="w-24 text-[11px] font-mono">
                                    <span className="text-[#808080]">{targetPreview.target_name} = </span>
                                    <span className={d.value === 1 ? 'text-[#00c853]' : 'text-[#ff6b00]'}>
                                      {d.value}
                                    </span>
                                  </div>
                                  <div className="flex-1 h-6 bg-[#0a0a0a] border border-[#2a2a2a] relative overflow-hidden">
                                    <div 
                                      className="h-full transition-all duration-500"
                                      style={{ 
                                        width: `${barWidth}%`, 
                                        backgroundColor: barColor,
                                        opacity: 0.8
                                      }}
                                    />
                                    <div className="absolute inset-0 flex items-center justify-end pr-2">
                                      <span className="text-[10px] font-mono text-white font-bold drop-shadow-lg">
                                        {d.count.toLocaleString()} ({d.percentage.toFixed(1)}%)
                                      </span>
                                    </div>
                                  </div>
                                </div>
                              );
                            })}
                          </div>
                        )}

                        {/* Warnings */}
                        {targetPreview.warnings?.length > 0 && (
                          <div className="space-y-2 mb-4">
                            {targetPreview.warnings.map((warning, idx) => (
                              <div 
                                key={idx}
                                className={`p-3 text-[11px] border ${
                                  warning.severity === 'critical' 
                                    ? 'bg-[#ff174410] border-[#ff1744] text-[#ff1744]' 
                                    : warning.severity === 'high'
                                      ? 'bg-[#ff6b0010] border-[#ff6b00] text-[#ff6b00]'
                                      : 'bg-[#ffc10710] border-[#ffc107] text-[#ffc107]'
                                }`}
                              >
                                <div className="font-bold mb-1">
                                  {warning.severity === 'critical' && '🚫 '}
                                  {warning.severity === 'high' && '⚠️ '}
                                  {warning.severity === 'medium' && '💡 '}
                                  {warning.message}
                                </div>
                                <div className="text-[10px] opacity-80">
                                  {warning.detail}
                                </div>
                              </div>
                            ))}
                          </div>
                        )}

                        {/* Recommendation */}
                        {targetPreview.recommendation && (
                          <div className={`p-3 text-[11px] border ${
                            !targetPreview.is_usable 
                              ? 'bg-[#ff174410] border-[#ff1744]' 
                              : 'bg-[#2a2a2a] border-[#3a3a3a]'
                          }`}>
                            <div className="text-[10px] text-[#808080] uppercase mb-1">RECOMMENDATION</div>
                            <div className={!targetPreview.is_usable ? 'text-[#ff1744]' : 'text-[#e0e0e0]'}>
                              {targetPreview.recommendation}
                            </div>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                )}

                {error && (
                  <div className="p-3 bg-[#ff174420] border border-[#ff1744] text-[#ff1744] text-[11px]">
                    ❌ {error}
                  </div>
                )}
              </motion.div>
            )}

            
            {step === 3 && (
              <motion.div
                key="step3"
                initial={{ opacity: 0, x: 20 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: -20 }}
                className="space-y-4"
              >
                {/* Info Bar */}
                <div className="flex items-center justify-between p-3 bg-[#121212] border border-[#2a2a2a]">
                  <div className="flex items-center gap-4 flex-wrap">
                    <div className="text-[10px] text-[#808080]">
                      TABLE: <span className="text-[#ff6b00]">{selectedTable}</span>
                    </div>
                    <div className="text-[10px] text-[#808080]">
                      TARGET: <span className="text-[#00c853]">{targetSql?.target_name}</span>
                    </div>
                    <div className="text-[10px] text-[#808080]">
                      GROUP BY: <span className="text-[#2196f3]">{groupingColumn}</span>
                    </div>
                  </div>
                  <div className="text-[10px] text-[#606060]">
                    {suggestions.length} FEATURES
                  </div>
                </div>

                {/* Select All */}
                <div className="flex items-center justify-between">
                  <button
                    type="button"
                    onClick={toggleAll}
                    className="btn-terminal text-[10px]"
                  >
                    {selectedFeatures.size === suggestions.length ? '☐ DESELECT ALL' : '☑ SELECT ALL'}
                  </button>
                  <div className="text-[11px] text-[#808080]">
                    <span className="text-[#ff6b00] font-bold">{selectedFeatures.size}</span> / {suggestions.length} selected
                  </div>
                </div>

                {/* Feature List */}
                <div className="space-y-2 max-h-[350px] overflow-y-auto pr-2">
                  {suggestions.map((feature, idx) => {
                    const isSelected = selectedFeatures.has(idx);
                    const relevance = getRelevanceColor(feature.relevance);

                    return (
                      <div
                        key={idx}
                        onClick={() => toggleFeature(idx)}
                        className={`p-3 border cursor-pointer transition-all ${
                          isSelected 
                            ? 'bg-[#1a1a1a] border-[#ff6b00]' 
                            : 'bg-[#0a0a0a] border-[#2a2a2a] hover:border-[#3a3a3a]'
                        }`}
                      >
                        <div className="flex items-start gap-3">
                          <div className={`w-4 h-4 border flex items-center justify-center text-[10px] mt-0.5 flex-shrink-0 ${
                            isSelected 
                              ? 'bg-[#ff6b00] border-[#ff6b00] text-black' 
                              : 'border-[#606060]'
                          }`}>
                            {isSelected && '✓'}
                          </div>

                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-2 mb-1 flex-wrap">
                              <span className="text-[#808080] text-sm">
                                {getTypeIcon(feature.type)}
                              </span>
                              <span className="text-[12px] text-[#e0e0e0] font-mono font-bold">
                                {feature.name}
                              </span>
                              <span 
                                className="text-[8px] font-bold px-1.5 py-0.5"
                                style={{ backgroundColor: relevance.bg, color: relevance.text }}
                              >
                                {relevance.label}
                              </span>
                              <span className="text-[8px] text-[#606060] uppercase">
                                {feature.type}
                              </span>
                            </div>

                            <p className="text-[10px] text-[#808080] leading-relaxed">
                              {feature.description}
                            </p>

                            <div className="mt-1 text-[9px] text-[#505050] font-mono truncate">
                              SQL: {feature.sql_template}
                            </div>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>

                {error && (
                  <div className="p-3 bg-[#ff174420] border border-[#ff1744] text-[#ff1744] text-[11px]">
                    ❌ {error}
                  </div>
                )}
              </motion.div>
            )}

            
            {step === 4 && (
              <motion.div
                key="step4"
                initial={{ opacity: 0, x: 20 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: -20 }}
                className="space-y-4"
              >
                {/* Success/Warning Banner */}
                {sqlValidation?.valid === false ? (
                  <div className="p-4 bg-[#ff174420] border border-[#ff1744] text-center">
                    <div className="text-[#ff1744] text-lg mb-1">⚠️ SQL VALIDATION WARNING</div>
                    <div className="text-[11px] text-[#808080] mb-2">
                      The generated SQL may have errors. Review before executing.
                    </div>
                    <div className="text-[10px] text-[#ff1744] font-mono bg-[#0a0a0a] p-2 text-left overflow-x-auto">
                      {sqlValidation.error?.substring(0, 200)}...
                    </div>
                  </div>
                ) : (
                  <div className="p-4 bg-[#00c85310] border border-[#00c853] text-center">
                    <div className="text-[#00c853] text-lg mb-1">✓ ML DATASET PIPELINE READY</div>
                    <div className="text-[11px] text-[#808080]">
                      <span className="text-[#ff6b00]">{selectedFeatures.size} features</span> + 
                      <span className="text-[#00c853]"> target ({targetSql?.target_name})</span> from 
                      <span className="text-[#ff6b00]"> {selectedTable}</span>
                      {groupingColumn && <> grouped by <span className="text-[#2196f3]">{groupingColumn}</span></>}
                    </div>
                  </div>
                )}

                {/* SQL Code Block */}
                <div className="relative">
                  <div className="flex items-center justify-between px-3 py-2 bg-[#1a1a1a] border border-[#2a2a2a] border-b-0">
                    <span className="text-[10px] text-[#808080] font-mono">POSTGRESQL</span>
                    <button
                      type="button"
                      onClick={handleCopySql}
                      className={`btn-terminal text-[10px] px-3 ${isCopied ? 'text-[#00c853]' : ''}`}
                    >
                      {isCopied ? '✓ COPIED!' : '📋 COPY'}
                    </button>
                  </div>
                  <pre className="bg-[#0a0a0a] border border-[#2a2a2a] p-4 overflow-x-auto text-[11px] font-mono text-[#00bcd4] leading-relaxed max-h-[350px] overflow-y-auto">
                    {generatedSql}
                  </pre>
                </div>

                {/* Next Steps */}
                <div className="p-3 bg-[#121212] border border-[#2a2a2a]">
                  <div className="text-[10px] text-[#808080] uppercase tracking-wide mb-2">
                    NEXT STEPS
                  </div>
                  <ul className="text-[11px] text-[#606060] space-y-1">
                    <li>• Copy the SQL and run it in your database client</li>
                    <li>• Export results to CSV for model training</li>
                    <li>• The target column <span className="text-[#00c853]">{targetSql?.target_name}</span> is ready for ML</li>
                  </ul>
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        {/* Footer Actions */}
        <div className="flex items-center justify-between px-4 py-3 bg-[#121212] border-t border-[#2a2a2a]">
          {/* Left: Back button */}
          <div>
            {step > 1 && (
              <button
                type="button"
                onClick={() => setStep(step - 1)}
                disabled={isLoading}
                className="btn-terminal text-[10px] px-4"
              >
                ← BACK
              </button>
            )}
          </div>

          {/* Right: Primary action */}
          <div className="flex items-center gap-3">
            {isLoading && (
              <span className="text-[11px] text-[#ffc107] animate-pulse">
                {step === 2 ? 'DRAFTING TARGET...' : step === 3 ? 'LOADING FEATURES...' : 'BUILDING SQL...'}
              </span>
            )}

            {step === 1 && (
              <button
                type="button"
                onClick={handleProceedToTarget}
                disabled={!selectedTable || !targetGoal.trim() || !manualGroupingColumn}
                className="btn-primary text-[11px] px-6 py-2 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                NEXT →
              </button>
            )}

            {step === 2 && (
              <>
                {/* Show warning if target is unusable */}
                {targetPreview && !targetPreview.is_usable && (
                  <span className="text-[11px] text-[#ff1744]">
                    🚫 Cannot proceed with unusable target
                  </span>
                )}
                <button
                  type="button"
                  onClick={handleConfirmTarget}
                  disabled={!targetSql || isLoading || isPreviewLoading || (targetPreview && !targetPreview.is_usable)}
                  className="btn-primary text-[11px] px-6 py-2 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {targetPreview?.warnings?.some(w => w.severity === 'high' || w.severity === 'critical') 
                    ? '⚠️ PROCEED ANYWAY' 
                    : '✓ CONFIRM & GET FEATURES'}
                </button>
              </>
            )}

            {step === 3 && (
              <button
                type="button"
                onClick={handleGenerateSql}
                disabled={selectedFeatures.size === 0 || isLoading}
                className="btn-primary text-[11px] px-6 py-2 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                GENERATE SQL ({selectedFeatures.size} + target)
              </button>
            )}

            {step === 4 && (
              <button
                type="button"
                onClick={onClose}
                className="btn-primary text-[11px] px-6 py-2"
              >
                DONE
              </button>
            )}
          </div>
        </div>
      </motion.div>
    </div>
  );
}
