import { useState } from 'react';
import { motion } from 'framer-motion';

/**
 * ExportPanel
 * 
 * Export dataset to CSV with metadata:
 * - Format selection (CSV)
 * - Row limit option
 * - Export progress
 * - Download links
 */
export function ExportPanel({
    exportDataset,
    datasetSql,
    isExporting,
    onExport,
}) {
    const [format, setFormat] = useState('csv');
    const [rowLimit, setRowLimit] = useState('');
    const [exportResult, setExportResult] = useState(null);
    const [error, setError] = useState(null);
    const [localExporting, setLocalExporting] = useState(false);

    const handleExport = async () => {
        setLocalExporting(true);
        setError(null);
        setExportResult(null);

        try {
            const result = await exportDataset({
                format,
                row_limit: rowLimit ? parseInt(rowLimit, 10) : null,
            });

            if (result.error) {
                setError(result.error);
            } else {
                setExportResult(result);
                if (onExport) onExport(result);
            }
        } catch (err) {
            setError(err.message || 'Export failed');
        } finally {
            setLocalExporting(false);
        }
    };

    const isRunning = isExporting || localExporting;

    return (
        <motion.div
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
            exit={{ opacity: 0, x: -20 }}
            className="space-y-4"
        >
            {/* Header */}
            <div className="text-[11px] text-[#808080] uppercase tracking-wide">
                📦 EXPORT DATASET
            </div>

            {/* Export Options */}
            <div className="p-4 bg-[#121212] border border-[#2a2a2a] space-y-4">
                {/* Format */}
                <div>
                    <label className="block text-[10px] text-[#606060] uppercase mb-2">
                        Export Format
                    </label>
                    <div className="flex gap-2">
                        {['csv', 'parquet'].map((f) => (
                            <button
                                key={f}
                                type="button"
                                onClick={() => setFormat(f)}
                                disabled={f === 'parquet'}
                                className={`px-4 py-2 text-[11px] font-mono border transition-all ${format === f
                                        ? 'bg-[#ff6b00] border-[#ff6b00] text-black'
                                        : f === 'parquet'
                                            ? 'bg-[#0a0a0a] border-[#1a1a1a] text-[#404040] cursor-not-allowed'
                                            : 'bg-[#0a0a0a] border-[#2a2a2a] text-[#808080] hover:border-[#3a3a3a]'
                                    }`}
                            >
                                {f.toUpperCase()}
                                {f === 'parquet' && ' (coming soon)'}
                            </button>
                        ))}
                    </div>
                </div>

                {/* Row Limit */}
                <div>
                    <label className="block text-[10px] text-[#606060] uppercase mb-2">
                        Row Limit (optional)
                    </label>
                    <input
                        type="number"
                        value={rowLimit}
                        onChange={(e) => setRowLimit(e.target.value)}
                        placeholder="Leave empty for all rows"
                        className="input-terminal w-full text-[12px]"
                        min="1"
                    />
                    <p className="text-[9px] text-[#505050] mt-1">
                        Limit export to first N rows. Useful for testing.
                    </p>
                </div>
            </div>

            {/* Error */}
            {error && (
                <div className="p-3 bg-[#ff174420] border border-[#ff1744] text-[#ff1744] text-[11px]">
                    ❌ {error}
                </div>
            )}

            {/* Export Button */}
            {!exportResult && (
                <button
                    type="button"
                    onClick={handleExport}
                    disabled={isRunning}
                    className="btn-terminal w-full text-[11px] py-3 disabled:opacity-50"
                >
                    {isRunning ? '⏳ EXPORTING...' : '📥 EXPORT DATASET'}
                </button>
            )}

            {/* Export Result */}
            {exportResult && (
                <div className="p-4 bg-[#00c85310] border border-[#00c853] space-y-4">
                    <div className="flex items-center gap-3">
                        <span className="text-[24px]">✓</span>
                        <div>
                            <div className="text-[14px] text-[#00c853] font-bold">
                                EXPORT COMPLETE
                            </div>
                            <div className="text-[11px] text-[#808080]">
                                {exportResult.row_count?.toLocaleString()} rows exported
                            </div>
                        </div>
                    </div>

                    {/* File Paths */}
                    <div className="space-y-2 text-[11px] font-mono">
                        <div className="flex items-center gap-2">
                            <span className="text-[#ff6b00]">📄 DATA:</span>
                            <span className="text-[#e0e0e0] break-all">{exportResult.file_path}</span>
                        </div>
                        {exportResult.metadata_path && (
                            <div className="flex items-center gap-2">
                                <span className="text-[#2196f3]">📋 META:</span>
                                <span className="text-[#e0e0e0] break-all">{exportResult.metadata_path}</span>
                            </div>
                        )}
                    </div>

                    {/* New Export Button */}
                    <button
                        type="button"
                        onClick={() => setExportResult(null)}
                        className="btn-terminal text-[10px] px-4 py-2"
                    >
                        ↻ EXPORT AGAIN
                    </button>
                </div>
            )}

            {/* Info */}
            <div className="p-3 bg-[#2196f310] border border-[#2196f3] text-[10px] text-[#2196f3]">
                ℹ️ Export includes a metadata JSON file with grain definition, target, features, and validation summary for full reproducibility.
            </div>
        </motion.div>
    );
}
