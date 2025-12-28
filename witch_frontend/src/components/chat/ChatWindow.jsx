import { useState, useRef, useEffect } from 'react';

// Component for rendering bot messages with optional SQL toggle and table
function BotMessage({ message, index }) {
  const [showSql, setShowSql] = useState(false);

  return (
    <div className="max-w-full">
      {/* Main message text */}
      <div className="text-[#e0e0e0] leading-relaxed whitespace-pre-wrap">
        {message.text}
      </div>

      {/* Table data preview */}
      {message.tableData && (
        <div className="mt-3 overflow-x-auto border border-[#2a2a2a]">
          <table className="terminal-table">
            <thead>
              <tr>
                <th className="text-[10px] text-[#606060]">#</th>
                {message.tableData.columns.map((col, i) => (
                  <th key={i} className="text-[11px]">
                    {col.toUpperCase()}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {message.tableData.rows.map((row, rowIdx) => (
                <tr key={rowIdx}>
                  <td className="text-[#606060] text-[10px]">{rowIdx + 1}</td>
                  {message.tableData.columns.map((col, colIdx) => (
                    <td key={colIdx} className="text-[12px]">
                      {String(row[col] ?? '').length > 30 
                        ? String(row[col]).slice(0, 28) + '…' 
                        : String(row[col] ?? '')}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
          {message.tableData.totalRows > message.tableData.rows.length && (
            <div className="px-3 py-2 text-[10px] text-[#606060] bg-[#0a0a0a] border-t border-[#2a2a2a]">
              ... {message.tableData.totalRows - message.tableData.rows.length} MORE ROWS (TOTAL: {message.tableData.totalRows})
            </div>
          )}
        </div>
      )}

      {/* SQL Toggle Button */}
      {message.sql && (
        <div className="mt-2">
          <button
            onClick={() => setShowSql(!showSql)}
            className="text-[10px] text-[#606060] hover:text-[#ff6b00] transition-colors uppercase tracking-wide"
          >
            [{showSql ? '-' : '+'}] SQL QUERY
          </button>

          {/* SQL Code Block */}
          {showSql && (
            <pre className="mt-2 p-3 bg-[#050505] border border-[#2a2a2a] text-[#00bcd4] text-[11px] overflow-x-auto">
              <code>{message.sql}</code>
            </pre>
          )}
        </div>
      )}
    </div>
  );
}

export function ChatWindow({
  messages = [],
  onSendMessage,
  isProcessing = false,
  onUpload,
  onReset,
  onUndo,
  onOpenDbModal,
  onDisconnectDb,
  fileMetadata = null,
  sessionId = null,
  dbSessionId = null,
  dbName = null,
  isDbMode = false,
  hasActiveSession = false,
}) {
  const [inputValue, setInputValue] = useState('');
  const messagesEndRef = useRef(null);
  const fileInputRef = useRef(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const handleSubmit = (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (!inputValue.trim() || isProcessing) return;
    onSendMessage(inputValue);
    setInputValue('');
  };

  const handleFileChange = (e) => {
    const file = e.target.files?.[0];
    if (file) {
      onUpload(file);
    }
    e.target.value = '';
  };

  const triggerFileUpload = () => {
    fileInputRef.current?.click();
  };

  return (
    <div className="flex flex-col h-full bg-[#0a0a0a]">
      {/* Action Bar */}
      {hasActiveSession && (
        <div className="flex items-center gap-1 px-3 py-2 bg-[#121212] border-b border-[#2a2a2a]">
          {/* Show Undo/Reset only for file mode */}
          {!isDbMode && (
            <>
              <button
                type="button"
                onClick={onUndo}
                disabled={isProcessing}
                className="btn-terminal text-[10px] py-1"
              >
                UNDO
              </button>
              <button
                type="button"
                onClick={onReset}
                disabled={isProcessing}
                className="btn-terminal text-[10px] py-1"
              >
                RESET
              </button>
            </>
          )}

          {/* Disconnect DB button for DB mode */}
          {isDbMode && (
            <button
              type="button"
              onClick={onDisconnectDb}
              disabled={isProcessing}
              className="btn-terminal text-[10px] py-1 text-[#ff1744]"
            >
              DISCONNECT
            </button>
          )}

          <button
            type="button"
            onClick={triggerFileUpload}
            disabled={isProcessing}
            className="btn-terminal text-[10px] py-1"
          >
            LOAD FILE
          </button>

          <button
            type="button"
            onClick={onOpenDbModal}
            disabled={isProcessing}
            className="btn-terminal text-[10px] py-1"
          >
            CONNECT DB
          </button>
        </div>
      )}

      {/* Messages Area */}
      <div className="flex-1 overflow-y-auto p-3">
        {/* Welcome State */}
        {messages.length === 0 && !hasActiveSession && (
          <div className="flex flex-col items-center justify-center h-full text-center">
            {/* Witch Image */}
            <div className="mb-6">
              <img 
                src="/images/witch.png" 
                alt="Witch on broomstick" 
                className="w-48 h-auto opacity-90 hover:opacity-100 transition-opacity"
                style={{ filter: 'drop-shadow(0 0 20px rgba(255, 107, 0, 0.3))' }}
              />
            </div>
            
            {/* Title */}
            <pre className="text-[#ff6b00] text-[10px] leading-tight mb-2 font-mono">
{`█ █ █ █ ▀█▀ ▄▀▀ █ █`}
            </pre>
            <pre className="text-[#ff6b00] text-[10px] leading-tight mb-6 font-mono">
{`▀▄▀▄▀ █  █  █   █▀█`}
            </pre>

            <div className="text-[#808080] text-sm mb-2">WITCH ANALYTICS TERMINAL</div>
            <div className="text-[#606060] text-xs mb-8">DATA ANALYSIS & VISUALIZATION SYSTEM</div>

            <div className="border border-[#2a2a2a] bg-[#121212] p-6 max-w-md">
              <div className="text-[#ff6b00] text-xs mb-4 uppercase tracking-wider">
                ◆ SELECT DATA SOURCE
              </div>

              <div className="flex flex-col gap-3">
                <button
                  onClick={triggerFileUpload}
                  disabled={isProcessing}
                  className="btn-terminal-primary w-full py-3 text-xs"
                >
                  [1] LOAD FILE (.CSV, .XLSX)
                </button>

                <button
                  onClick={onOpenDbModal}
                  disabled={isProcessing}
                  className="btn-terminal w-full py-3 text-xs"
                >
                  [2] CONNECT DATABASE
                </button>
              </div>

              <div className="mt-6 pt-4 border-t border-[#2a2a2a]">
                <div className="text-[10px] text-[#606060] uppercase tracking-wide mb-2">FEATURES:</div>
                <div className="grid grid-cols-2 gap-2 text-[10px]">
                  <div className="text-[#00c853]">• NATURAL LANGUAGE</div>
                  <div className="text-[#00c853]">• SQL GENERATION</div>
                  <div className="text-[#00c853]">• AUTO CHARTS</div>
                  <div className="text-[#00c853]">• DATA EXPORT</div>
                </div>
              </div>
            </div>

            <div className="mt-6 text-[11px] text-[#909090]">
              PRESS [1] OR [2] TO BEGIN
            </div>
          </div>
        )}

        {/* Messages */}
        {messages.map((message, index) => (
          <div
            key={index}
            className={`mb-4 ${message.role === 'user' ? 'pl-8' : ''}`}
          >
            {/* Timestamp & Role Label */}
            <div className="flex items-center gap-2 mb-1">
              <span className="text-[10px] text-[#606060]">
                {new Date().toLocaleTimeString('en-US', { hour12: false })}
              </span>
              <span className={`text-[10px] font-bold uppercase tracking-wide ${
                message.role === 'user' ? 'text-[#2196f3]' : 'text-[#ff6b00]'
              }`}>
                {message.role === 'user' ? '► USER' : '◄ WITCH'}
              </span>
            </div>

            {/* Message Content */}
            <div className={`pl-4 border-l-2 ${
              message.role === 'user' 
                ? 'border-[#2196f3] text-[#e0e0e0]' 
                : 'border-[#ff6b00]'
            }`}>
              {message.role === 'user' ? (
                <div className="text-[#e0e0e0] leading-relaxed whitespace-pre-wrap">
                  {message.text}
                </div>
              ) : (
                <BotMessage message={message} index={index} />
              )}
            </div>
          </div>
        ))}

        {/* Processing Indicator */}
        {isProcessing && (
          <div className="mb-4">
            <div className="flex items-center gap-2 mb-1">
              <span className="text-[10px] text-[#606060]">
                {new Date().toLocaleTimeString('en-US', { hour12: false })}
              </span>
              <span className="text-[10px] font-bold uppercase tracking-wide text-[#ffc107]">
                ◄ PROCESSING
              </span>
            </div>
            <div className="pl-4 border-l-2 border-[#ffc107] flex items-center gap-3">
              <img 
                src="/images/witch.png" 
                alt="Processing" 
                className="w-12 h-auto animate-bounce"
                style={{ 
                  filter: 'drop-shadow(0 0 8px rgba(255, 193, 7, 0.5))',
                  animationDuration: '1s'
                }}
              />
              <div className="text-[#ffc107] text-sm">
                {isDbMode ? 'CASTING SQL SPELL...' : 'BREWING ANALYSIS...'}
                <span className="inline-block w-2 h-4 bg-[#ffc107] ml-1 animate-pulse"></span>
              </div>
            </div>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* Input Area */}
      <input
        type="file"
        ref={fileInputRef}
        onChange={handleFileChange}
        accept=".csv,.xlsx,.xls"
        className="hidden"
      />

      {hasActiveSession && (
        <div className="border-t border-[#2a2a2a] bg-[#121212] p-3">
          <form onSubmit={handleSubmit}>
            <div className="flex items-center gap-2">
              <span className="text-[#ff6b00] text-sm font-bold">►</span>
              <input
                type="text"
                value={inputValue}
                onChange={(e) => setInputValue(e.target.value)}
                placeholder={isDbMode ? 'ENTER QUERY...' : 'ENTER COMMAND...'}
                disabled={isProcessing}
                className="input-terminal flex-1"
                autoFocus
              />
              <button
                type="submit"
                disabled={isProcessing || !inputValue.trim()}
                className="btn-terminal-primary px-6"
              >
                EXEC
              </button>
            </div>
          </form>
          <div className="mt-2 text-[11px] text-[#808080]">
            <span className="text-[#606060]">TIP:</span> {isDbMode 
              ? '"SHOW TOP 10 CUSTOMERS BY REVENUE" | "COUNT ORDERS BY STATUS"'
              : '"SHOW CHART OF SALES BY REGION" | "FILTER WHERE AMOUNT > 1000"'
            }
          </div>
        </div>
      )}
    </div>
  );
}
