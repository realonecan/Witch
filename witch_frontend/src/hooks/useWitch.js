import { useState } from 'react';
import apiClient from '../api/client';

export function useWitch() {
  // File mode state
  const [sessionId, setSessionId] = useState(null);
  const [messages, setMessages] = useState([]);
  const [plotJson, setPlotJson] = useState(null);
  const [isProcessing, setIsProcessing] = useState(false);
  const [fileMetadata, setFileMetadata] = useState(null);

  // Database mode state
  const [dbSessionId, setDbSessionId] = useState(null);
  const [dbTables, setDbTables] = useState([]);
  const [dbName, setDbName] = useState(null);

  // Determine which mode we're in
  const isDbMode = !!dbSessionId;
  const hasActiveSession = !!sessionId || !!dbSessionId;

  const uploadFile = async (file) => {
    setIsProcessing(true);

    try {
      const formData = new FormData();
      formData.append('file', file);

      const response = await apiClient.post('/upload', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      });

      console.log('Upload response:', response.data);

      const { session_id, filename, preview } = response.data;

      // Clear any DB session when uploading a file
      setDbSessionId(null);
      setDbTables([]);
      setDbName(null);

      // Set session ID first
      setSessionId(session_id);

      // Build file metadata object
      const metadata = {
        filename: filename,
        columns: preview.columns || [],
        dtypes: preview.dtypes || {},
        rowCount: preview.rows?.length || 0,
        sampleRows: preview.rows || [],
      };

      console.log('Setting fileMetadata:', metadata);
      setFileMetadata(metadata);

      // Add success message
      setMessages((prev) => [
        ...prev,
        {
          role: 'bot',
          text: `✨ File "${filename}" uploaded successfully! Found ${preview.columns?.length || 0} columns. Ask me anything about your data.`,
        },
      ]);

      // Clear any previous plot
      setPlotJson(null);
    } catch (error) {
      console.error('Upload error:', error);
      const errorMessage =
        error.response?.data?.detail || error.message || 'Failed to upload file';
      setMessages((prev) => [
        ...prev,
        { role: 'bot', text: `❌ Error: ${errorMessage}` },
      ]);
    } finally {
      setIsProcessing(false);
    }
  };

  const connectDatabase = async (credentials) => {
    setIsProcessing(true);

    try {
      const response = await apiClient.post('/connect-db', credentials);

      console.log('DB Connect response:', response.data);

      const { session_id, tables, status, message } = response.data;

      if (status === 'connected') {
        // Clear file session when connecting to DB
        setSessionId(null);
        setFileMetadata(null);
        setPlotJson(null);

        // Set DB session
        setDbSessionId(session_id);
        setDbTables(tables || []);
        setDbName(credentials.database);

        // Add success message
        setMessages((prev) => [
          ...prev,
          {
            role: 'bot',
            text: `🗄️ Connected to database "${credentials.database}"! Found ${tables?.length || 0} tables: ${tables?.slice(0, 5).join(', ')}${tables?.length > 5 ? '...' : ''}. Ask me anything in plain English.`,
          },
        ]);

        return { success: true };
      } else {
        throw new Error(message || 'Failed to connect');
      }
    } catch (error) {
      console.error('DB Connect error:', error);
      const errorMessage =
        error.response?.data?.detail || error.message || 'Failed to connect to database';
      setMessages((prev) => [
        ...prev,
        { role: 'bot', text: `❌ Database Error: ${errorMessage}` },
      ]);
      return { success: false, error: errorMessage };
    } finally {
      setIsProcessing(false);
    }
  };

  const sendMessage = async (text) => {
    if (!hasActiveSession || !text.trim()) return;

    // Add user message immediately
    setMessages((prev) => [...prev, { role: 'user', text }]);
    setIsProcessing(true);

    try {
      if (isDbMode) {
        // Database mode - call /db-chat
        const response = await apiClient.post('/db-chat', {
          session_id: dbSessionId,
          message: text,
        });

        console.log('DB Chat response:', response.data);

        const { sql_query, result, data, status } = response.data;

        // Build the response message
        let botMessage = '';
        let tableData = null;

        if (status === 'error') {
          botMessage = `❌ ${result || 'An error occurred'}`;
        } else {
          // Show the clean result text first (this is the main message)
          if (result) {
            botMessage += result;
          }

          // If there's data with multiple rows, prepare table data for rendering
          // Show all rows if ≤ 50, otherwise limit to 15 rows
          if (data && Array.isArray(data) && data.length > 0) {
            const maxRowsToShow = data.length <= 50 ? data.length : 15;
            tableData = {
              columns: Object.keys(data[0]),
              rows: data.slice(0, maxRowsToShow),
              totalRows: data.length,
            };
          }
        }

        setMessages((prev) => [
          ...prev,
          { 
            role: 'bot', 
            text: botMessage || 'Query executed successfully.',
            sql: sql_query || null,
            tableData: tableData,
          },
        ]);
      } else {
        // File mode - call /chat
        const response = await apiClient.post('/chat', {
          session_id: sessionId,
          message: text,
        });

        console.log('Chat response:', response.data);

        const { result, plot_json, status } = response.data;

        // Add bot response
        if (result) {
          setMessages((prev) => [
            ...prev,
            {
              role: 'bot',
              text: status === 'error' ? `❌ ${result}` : result,
            },
          ]);
        }

        // Parse and set plot JSON if present
        if (plot_json) {
          try {
            const parsedPlot = JSON.parse(plot_json);
            setPlotJson(parsedPlot);
            if (!result) {
              setMessages((prev) => [
                ...prev,
                { role: 'bot', text: '📊 Chart generated! Check the visualization panel.' },
              ]);
            }
          } catch (parseError) {
            console.error('Failed to parse plot JSON:', parseError);
          }
        }
      }
    } catch (error) {
      console.error('Chat error:', error);
      const errorMessage =
        error.response?.data?.detail || error.message || 'Failed to process message';
      setMessages((prev) => [
        ...prev,
        { role: 'bot', text: `❌ Error: ${errorMessage}` },
      ]);
    } finally {
      setIsProcessing(false);
    }
  };

  const resetSession = async () => {
    if (!sessionId) return;

    setIsProcessing(true);

    try {
      await apiClient.post('/reset', { session_id: sessionId });

      setMessages((prev) => [
        ...prev,
        { role: 'bot', text: '🔄 Data has been reset to original state.' },
      ]);
      setPlotJson(null);
    } catch (error) {
      console.error('Reset error:', error);
      const errorMessage =
        error.response?.data?.detail || error.message || 'Failed to reset session';
      setMessages((prev) => [
        ...prev,
        { role: 'bot', text: `❌ Error: ${errorMessage}` },
      ]);
    } finally {
      setIsProcessing(false);
    }
  };

  const undoLastAction = async () => {
    if (!sessionId) return;

    setIsProcessing(true);

    try {
      const response = await apiClient.post('/undo', { session_id: sessionId });

      console.log('Undo response:', response.data);

      const { status, message, row_count } = response.data;

      if (status === 'success') {
        // Update file metadata with new row count
        if (row_count !== null && fileMetadata) {
          setFileMetadata((prev) => ({
            ...prev,
            rowCount: row_count,
          }));
        }

        setMessages((prev) => [
          ...prev,
          { role: 'bot', text: `↩️ ${message}` },
        ]);
      } else {
        setMessages((prev) => [
          ...prev,
          { role: 'bot', text: `⚠️ ${message}` },
        ]);
      }
    } catch (error) {
      console.error('Undo error:', error);
      const errorMessage =
        error.response?.data?.detail || error.message || 'Failed to undo';
      setMessages((prev) => [
        ...prev,
        { role: 'bot', text: `❌ Error: ${errorMessage}` },
      ]);
    } finally {
      setIsProcessing(false);
    }
  };

  const disconnectDatabase = () => {
    setDbSessionId(null);
    setDbTables([]);
    setDbName(null);
    setMessages((prev) => [
      ...prev,
      { role: 'bot', text: '🔌 Disconnected from database.' },
    ]);
  };

  const auditTable = async (tableName, options = {}) => {
    if (!dbSessionId) return null;

    try {
      const response = await apiClient.post('/audit-table', {
        session_id: dbSessionId,
        table_name: tableName,
        sample_size: options.sampleSize,
      });

      console.log('Audit response:', response.data);
      return response.data;
    } catch (error) {
      console.error('Audit error:', error);
      const errorMessage =
        error.response?.data?.detail || error.message || 'Failed to audit table';
      return { error: errorMessage };
    }
  };

  const getAuditHistory = async () => {
    if (!dbSessionId) return [];

    try {
      const response = await apiClient.post('/audit-history', {
        session_id: dbSessionId,
      });

      console.log('Audit history response:', response.data);
      return response.data.audits || [];
    } catch (error) {
      console.error('Audit history error:', error);
      return [];
    }
  };

  // =========================================================================
  
  // =========================================================================

  
  const defineGrain = async (params) => {
    if (!dbSessionId) return { error: 'No database session' };

    try {
      const response = await apiClient.post('/define-grain', {
        session_id: dbSessionId,
        ...params,
      });
      console.log('Define grain response:', response.data);
      return response.data;
    } catch (error) {
      console.error('Define grain error:', error);
      return { error: error.response?.data?.detail || error.message };
    }
  };

  const previewGrain = async (params) => {
    if (!dbSessionId) return { error: 'No database session' };

    try {
      const response = await apiClient.post('/preview-grain', {
        session_id: dbSessionId,
        ...params,
      });
      console.log('Preview grain response:', response.data);
      return response.data;
    } catch (error) {
      console.error('Preview grain error:', error);
      return { error: error.response?.data?.detail || error.message };
    }
  };

  
  const defineTarget = async (params) => {
    if (!dbSessionId) return { error: 'No database session' };

    try {
      const response = await apiClient.post('/define-target', {
        session_id: dbSessionId,
        ...params,
      });
      console.log('Define target response:', response.data);
      return response.data;
    } catch (error) {
      console.error('Define target error:', error);
      return { error: error.response?.data?.detail || error.message };
    }
  };

  const getTargetDistribution = async (params) => {
    if (!dbSessionId) return { error: 'No database session' };

    try {
      const response = await apiClient.post('/target-distribution', {
        session_id: dbSessionId,
        ...params,
      });
      console.log('Target distribution response:', response.data);
      return response.data;
    } catch (error) {
      console.error('Target distribution error:', error);
      return { error: error.response?.data?.detail || error.message };
    }
  };

  
  const listFeatureTemplates = async () => {
    try {
      const response = await apiClient.get('/list-feature-templates');
      console.log('Feature templates response:', response.data);
      return response.data;
    } catch (error) {
      console.error('List feature templates error:', error);
      return { error: error.response?.data?.detail || error.message };
    }
  };

  const generateFeature = async (params) => {
    if (!dbSessionId) return { error: 'No database session' };

    try {
      const response = await apiClient.post('/generate-feature', {
        session_id: dbSessionId,
        ...params,
      });
      console.log('Generate feature response:', response.data);
      return response.data;
    } catch (error) {
      console.error('Generate feature error:', error);
      return { error: error.response?.data?.detail || error.message };
    }
  };

  
  const assembleDataset = async (params) => {
    if (!dbSessionId) return { error: 'No database session' };

    try {
      const response = await apiClient.post('/assemble-dataset', {
        session_id: dbSessionId,
        ...params,
      });
      console.log('Assemble dataset response:', response.data);
      return response.data;
    } catch (error) {
      console.error('Assemble dataset error:', error);
      return { error: error.response?.data?.detail || error.message };
    }
  };

  
  const applyMissingStrategy = async (params) => {
    try {
      const response = await apiClient.post('/apply-missing-strategy', params);
      console.log('Apply missing strategy response:', response.data);
      return response.data;
    } catch (error) {
      console.error('Apply missing strategy error:', error);
      return { error: error.response?.data?.detail || error.message };
    }
  };

  const recommendMissingStrategy = async (templateType) => {
    try {
      const response = await apiClient.post('/recommend-missing-strategy', {
        template_type: templateType,
      });
      console.log('Recommend missing strategy response:', response.data);
      return response.data;
    } catch (error) {
      console.error('Recommend missing strategy error:', error);
      return { error: error.response?.data?.detail || error.message };
    }
  };

  
  const validateDatasetSql = async (params) => {
    if (!dbSessionId) return { error: 'No database session' };

    try {
      const response = await apiClient.post('/validate-dataset-sql', {
        session_id: dbSessionId,
        ...params,
      });
      console.log('Validate dataset SQL response:', response.data);
      return response.data;
    } catch (error) {
      console.error('Validate dataset SQL error:', error);
      return { error: error.response?.data?.detail || error.message };
    }
  };

  
  const exportDataset = async (params) => {
    if (!dbSessionId) return { error: 'No database session' };

    try {
      const response = await apiClient.post('/export-dataset', {
        session_id: dbSessionId,
        ...params,
      });
      console.log('Export dataset response:', response.data);
      return response.data;
    } catch (error) {
      console.error('Export dataset error:', error);
      return { error: error.response?.data?.detail || error.message };
    }
  };

  const clearAll = () => {
    setSessionId(null);
    setMessages([]);
    setPlotJson(null);
    setFileMetadata(null);
    setDbSessionId(null);
    setDbTables([]);
    setDbName(null);
    setIsProcessing(false);
  };

  return {
    // State
    sessionId,
    messages,
    plotJson,
    isProcessing,
    fileMetadata,

    // DB State
    dbSessionId,
    dbTables,
    dbName,
    isDbMode,
    hasActiveSession,

    // Actions (existing)
    uploadFile,
    sendMessage,
    resetSession,
    undoLastAction,
    clearAll,
    connectDatabase,
    disconnectDatabase,
    auditTable,
    getAuditHistory,

    
    defineGrain,
    previewGrain,
    defineTarget,
    getTargetDistribution,
    listFeatureTemplates,
    generateFeature,
    assembleDataset,
    applyMissingStrategy,
    recommendMissingStrategy,
    validateDatasetSql,
    exportDataset,
  };
}
