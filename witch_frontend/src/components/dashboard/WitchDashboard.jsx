import { useState, useEffect, useRef } from 'react';
import { useWitch } from '../../hooks/useWitch';
import { ChatWindow } from '../chat/ChatWindow';
import { ChartRenderer } from '../visuals/ChartRenderer';
import { DatabaseModal } from './DatabaseModal';
import { DataHealthDashboard } from './DataHealthDashboard';
import { DatabaseInsights } from './DatabaseInsights';
import { JoinExplorer } from './JoinExplorer';
import { FeatureLabV2 as FeatureLab } from './FeatureLabV2';
import { TableList } from './TableList';
import { FeatureWizard } from '../wizard';

export function WitchDashboard() {
  const {
    sessionId,
    messages,
    plotJson,
    isProcessing,
    fileMetadata,
    uploadFile,
    sendMessage,
    resetSession,
    undoLastAction,
    dbSessionId,
    dbTables,
    dbName,
    isDbMode,
    hasActiveSession,
    connectDatabase,
    disconnectDatabase,
    auditTable,
    getAuditHistory,
  } = useWitch();

  const [isPanelOpen, setIsPanelOpen] = useState(false);
  const [isDbModalOpen, setIsDbModalOpen] = useState(false);
  const [isTableListOpen, setIsTableListOpen] = useState(false);
  const [isAuditing, setIsAuditing] = useState(false);
  const [auditData, setAuditData] = useState(null);
  const [auditHistory, setAuditHistory] = useState([]);
  const [isFeatureLabOpen, setIsFeatureLabOpen] = useState(false);
  const [isWizardOpen, setIsWizardOpen] = useState(false);
  const [activeView, setActiveView] = useState('chat');
  const prevPlotJsonRef = useRef(null);

  // Auto-open panel ONLY when a NEW plot arrives
  useEffect(() => {
    if (plotJson !== null && plotJson !== prevPlotJsonRef.current) {
      setIsPanelOpen(true);
      prevPlotJsonRef.current = plotJson;
    }
  }, [plotJson]);

  const closePanel = () => {
    setIsPanelOpen(false);
  };

  const openPanel = () => {
    setIsPanelOpen(true);
  };

  const handleOpenDbModal = () => {
    setIsDbModalOpen(true);
  };

  const handleCloseDbModal = () => {
    setIsDbModalOpen(false);
  };

  const handleOpenWizard = () => {
    if (!dbSessionId) {
      setIsDbModalOpen(true);
      return;
    }
    setIsWizardOpen(true);
  };

  const handleOpenInsights = () => {
    setActiveView('insights');
  };

  const handleOpenChat = () => {
    setActiveView('chat');
  };

  const handleOpenJoin = () => {
    setActiveView('join');
  };

  const handleConnectDb = async (credentials) => {
    const result = await connectDatabase(credentials);
    if (result.success) {
      setIsDbModalOpen(false);
    }
  };

  const handleAuditTable = async (tableName) => {
    setIsAuditing(true);
    const result = await auditTable(tableName);
    setIsAuditing(false);

    if (result && !result.error) {
      setAuditData(result);
      setIsTableListOpen(false);
      // Refresh audit history
      refreshAuditHistory();
    } else {
      // Show error in messages or alert
      console.error('Audit failed:', result?.error);
    }
  };

  const refreshAuditHistory = async () => {
    const history = await getAuditHistory();
    setAuditHistory(history);
  };

  // Load audit history when table list opens
  useEffect(() => {
    if (isTableListOpen && isDbMode) {
      refreshAuditHistory();
    }
  }, [isTableListOpen, isDbMode]);

  const closeAuditDashboard = () => {
    setAuditData(null);
  };

  useEffect(() => {
    if (!dbSessionId) {
      setIsWizardOpen(false);
    }
  }, [dbSessionId]);

  useEffect(() => {
    if (dbSessionId) {
      setActiveView('insights');
    } else {
      setActiveView('chat');
    }
  }, [dbSessionId]);

  useEffect(() => {
    if (activeView !== 'chat') {
      setIsTableListOpen(false);
      setIsPanelOpen(false);
    }
  }, [activeView]);

  // Get current timestamp
  const now = new Date();
  const timestamp = now.toLocaleTimeString('en-US', { hour12: false });
  const datestamp = now.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric', year: 'numeric' });

  return (
    <div className="h-screen flex flex-col overflow-hidden bg-[#0a0a0a]">
      {/* Top Status Bar - Bloomberg style */}
      <header className="flex items-center justify-between px-3 py-1 bg-[#1a1a1a] border-b border-[#2a2a2a]">
        <div className="flex items-center gap-6">
          {/* Logo/Brand */}
          <div className="flex items-center gap-2">
            <div className="w-2 h-2 bg-[#ff6b00]"></div>
            <span className="font-bold text-[#ff6b00] tracking-wider text-sm">WITCH</span>
            <span className="text-[#606060] text-xs">TERMINAL</span>
          </div>

          {/* Mode indicator */}
          <div className="flex items-center gap-4 text-xs">
            {isDbMode ? (
              <>
                <span className="text-[#606060]">MODE:</span>
                <span className="text-[#00c853]">DATABASE</span>
                <span className="text-[#2a2a2a]">|</span>
                <span className="text-[#606060]">CONN:</span>
                <span className="text-[#ffc107]">{dbName?.toUpperCase()}</span>
                <span className="text-[#2a2a2a]">|</span>
                <span className="text-[#606060]">TABLES:</span>
                <span className="text-white">{dbTables?.length || 0}</span>
              </>
            ) : fileMetadata ? (
              <>
                <span className="text-[#606060]">MODE:</span>
                <span className="text-[#2196f3]">FILE</span>
                <span className="text-[#2a2a2a]">|</span>
                <span className="text-[#606060]">FILE:</span>
                <span className="text-[#ffc107]">{fileMetadata.filename?.toUpperCase()}</span>
                <span className="text-[#2a2a2a]">|</span>
                <span className="text-[#606060]">ROWS:</span>
                <span className="text-white">{(fileMetadata.rowCount || 0).toLocaleString()}</span>
                <span className="text-[#2a2a2a]">|</span>
                <span className="text-[#606060]">COLS:</span>
                <span className="text-white">{fileMetadata.columns?.length || 0}</span>
              </>
            ) : (
              <>
                <span className="text-[#606060]">MODE:</span>
                <span className="text-[#808080]">STANDBY</span>
              </>
            )}
          </div>
        </div>

        <div className="flex items-center gap-6 text-xs">
          {/* Status */}
          <div className="flex items-center gap-2">
            <span className="text-[#606060]">STATUS:</span>
            {isProcessing || isAuditing ? (
              <span className="text-[#ffc107]">{isAuditing ? 'AUDITING...' : 'PROCESSING...'}</span>
            ) : hasActiveSession ? (
              <span className="text-[#00c853]">● ONLINE</span>
            ) : (
              <span className="text-[#808080]">○ IDLE</span>
            )}
          </div>

          {/* Timestamp */}
          <div className="flex items-center gap-3 text-[#606060]">
            <span>{datestamp}</span>
            <span className="text-white font-mono">{timestamp}</span>
          </div>
        </div>
      </header>

      {/* Secondary toolbar */}
      <div className="flex items-center justify-between px-3 py-1 bg-[#121212] border-b border-[#2a2a2a]">
        <div className="flex items-center gap-1">
          {isDbMode && (
            <>
              <button
                type="button"
                onClick={handleOpenInsights}
                className={`btn-terminal text-[10px] py-1 ${activeView === 'insights' ? 'text-[#ff6b00]' : ''}`}
              >
                [F1] INSIGHTS
              </button>
              <button
                type="button"
                onClick={handleOpenChat}
                className={`btn-terminal text-[10px] py-1 ${activeView === 'chat' ? 'text-[#00c853]' : ''}`}
              >
                [F2] CHAT
              </button>
            </>
          )}
          {plotJson && !isPanelOpen && activeView === 'chat' && (
            <button
              type="button"
              onClick={openPanel}
              className="btn-terminal text-[10px] py-1"
            >
              [F3] VIEW CHART
            </button>
          )}
          {isDbMode && dbTables?.length > 0 && activeView === 'chat' && (
            <button
              type="button"
              onClick={() => setIsTableListOpen(!isTableListOpen)}
              className="btn-terminal text-[10px] py-1"
            >
              [F5] TABLES {isTableListOpen ? '▼' : '▶'}
            </button>
          )}
          {isDbMode && dbTables?.length > 0 && (
            <>
              <button
                type="button"
                onClick={() => setIsFeatureLabOpen(true)}
                className="btn-terminal text-[10px] py-1"
              >
                [F6] ⚗️ FEATURE LAB
              </button>
              <button
                type="button"
                onClick={handleOpenWizard}
                className="btn-terminal text-[10px] py-1"
              >
                [F7] WIZARD
              </button>
              <button
                type="button"
                onClick={handleOpenJoin}
                className={`btn-terminal text-[10px] py-1 ${activeView === 'join' ? 'text-[#ff6b00]' : ''}`}
              >
                [F8] JOIN EXPLORER
              </button>
            </>
          )}
        </div>

        <div className="flex items-center gap-3 text-[11px] text-[#707070]">
          {isDbMode && <span>F1:INSIGHTS</span>}
          {isDbMode && <span>F2:CHAT</span>}
          <span>F3:CHART</span>
          <span>F4:HISTORY</span>
          {isDbMode && activeView === 'chat' && <span className="text-[#ff6b00]">F5:TABLES</span>}
          {isDbMode && <span className="text-[#00c853]">F6:FEATURE LAB</span>}
          {isDbMode && <span className="text-[#ffc107]">F7:WIZARD</span>}
          {isDbMode && <span className="text-[#ff6b00]">F8:JOIN EXPLORER</span>}
        </div>
      </div>

      {/* Main Content Area */}
      <div className="flex-1 flex overflow-hidden">
        {/* Table List Sidebar (when open) */}
        {isTableListOpen && isDbMode && activeView === 'chat' && (
          <aside className="w-72 flex-shrink-0 border-r border-[#2a2a2a] overflow-auto bg-[#0a0a0a]">
            <TableList
              tables={dbTables}
              onAuditTable={handleAuditTable}
              isAuditing={isAuditing}
              auditHistory={auditHistory}
            />
          </aside>
        )}

        {/* Main Panel */}
        <main className={`flex-1 flex flex-col overflow-hidden transition-all duration-200 ${isPanelOpen && plotJson && activeView === 'chat' ? 'w-1/2' : 'w-full'
          }`}>
          {activeView === 'insights' && isDbMode ? (
            <DatabaseInsights
              dbSessionId={dbSessionId}
              dbName={dbName}
              auditTable={auditTable}
              onShowAudit={setAuditData}
            />
          ) : activeView === 'join' && isDbMode ? (
            <JoinExplorer
              dbSessionId={dbSessionId}
              dbTables={dbTables}
            />
          ) : (
            <>
              <div className="terminal-header">
                {isDbMode ? '?-+ DATABASE QUERY INTERFACE' : hasActiveSession ? '?-+ DATA ANALYSIS INTERFACE' : '?-+ COMMAND INTERFACE'}
              </div>
              <div className="flex-1 overflow-hidden">
                <ChatWindow
                  messages={messages}
                  onSendMessage={sendMessage}
                  isProcessing={isProcessing}
                  onUpload={uploadFile}
                  onReset={resetSession}
                  onUndo={undoLastAction}
                  onOpenDbModal={handleOpenDbModal}
                  onDisconnectDb={disconnectDatabase}
                  fileMetadata={fileMetadata}
                  sessionId={sessionId}
                  dbSessionId={dbSessionId}
                  dbName={dbName}
                  isDbMode={isDbMode}
                  hasActiveSession={hasActiveSession}
                />
              </div>
            </>
          )}
        </main>

        {/* Chart Panel - Slide in from right */}
        {isPanelOpen && plotJson && activeView === 'chat' && (
          <aside className="w-1/2 max-w-[800px] flex flex-col border-l border-[#2a2a2a] bg-[#0a0a0a]">
            {/* Chart Panel Header */}
            <div className="flex items-center justify-between terminal-header">
              <span>◆ VISUALIZATION OUTPUT</span>
              <button
                type="button"
                onClick={closePanel}
                className="text-black hover:bg-[#cc5500] px-2 py-0.5 text-[10px] font-bold"
              >
                [X] CLOSE
              </button>
            </div>

            {/* Chart Content */}
            <div className="flex-1 p-4 overflow-auto bg-[#121212]">
              <ChartRenderer plotJson={plotJson} />
            </div>
          </aside>
        )}
      </div>

      {/* Bottom status bar */}
      <footer className="flex items-center justify-between px-3 py-1 bg-[#1a1a1a] border-t border-[#2a2a2a] text-[11px]">
        <div className="flex items-center gap-4">
          <span className="text-[#808080]">WITCH ANALYTICS TERMINAL v1.0</span>
          <span className="text-[#3a3a3a]">|</span>
          <span className="text-[#707070]">SESSION: <span className="text-[#909090]">{sessionId?.slice(0, 8) || dbSessionId?.slice(0, 8) || 'NONE'}</span></span>
        </div>
        <div className="flex items-center gap-4">
          <span className="text-[#707070]">MSGS: <span className="text-[#909090]">{messages.length}</span></span>
          <span className="text-[#3a3a3a]">|</span>
          <span className="text-[#00c853]">READY</span>
        </div>
      </footer>

      {/* Database Connection Modal */}
      <DatabaseModal
        isOpen={isDbModalOpen}
        onClose={handleCloseDbModal}
        onConnect={handleConnectDb}
        isConnecting={isProcessing}
      />

      {/* Data Health Dashboard Modal */}
      {auditData && (
        <DataHealthDashboard
          auditData={auditData}
          onClose={closeAuditDashboard}
        />
      )}

      {/* Feature Engineering Lab Modal */}
      <FeatureLab
        isOpen={isFeatureLabOpen}
        onClose={() => setIsFeatureLabOpen(false)}
        dbSessionId={dbSessionId}
        tables={dbTables}
      />

      {isWizardOpen && dbSessionId && (
        <div className="fixed inset-0 z-50">
          <FeatureWizard
            sessionId={dbSessionId}
            connectionStatus="connected"
            onClose={() => setIsWizardOpen(false)}
          />
        </div>
      )}
    </div>
  );
}
