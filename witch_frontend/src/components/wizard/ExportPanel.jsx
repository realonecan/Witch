import { useEffect, useMemo, useState } from 'react';
import api from '../../api/client';

const statusClass = (status) => {
    if (!status) {
        return 'text-[var(--color-terminal-text-dim)]';
    }
    const lower = String(status).toLowerCase();
    if (lower === 'success' || lower === 'ok' || lower === 'valid') {
        return 'text-[var(--color-terminal-green)]';
    }
    if (lower === 'warning') {
        return 'text-[var(--color-terminal-yellow)]';
    }
    if (lower === 'error' || lower === 'invalid') {
        return 'text-[var(--color-terminal-red)]';
    }
    return 'text-[var(--color-terminal-text-dim)]';
};

const formatIssue = (issue) => {
    if (!issue) {
        return '';
    }
    const message = issue.message || issue;
    const code = issue.code ? `[${issue.code}] ` : '';
    return `${code}${message}`;
};

export default function ExportPanel({
    sessionId,
    wizardState,
    onBack,
    onRestart
}) {
    const datasetSql = wizardState.quality?.dataset_sql || '';
    const featureSqls = useMemo(() => wizardState.features?.feature_sqls || [], [wizardState.features]);
    const featureCount = wizardState.quality?.feature_count
        ?? wizardState.features?.feature_count
        ?? featureSqls.length;

    const entityName = wizardState.entity?.column_name || 'entity_id';
    const targetName = wizardState.target?.distribution?.target_name
        || wizardState.target?.definition?.target_name
        || 'target';

    const [validationResult, setValidationResult] = useState(null);
    const [validationError, setValidationError] = useState(null);
    const [validationLoading, setValidationLoading] = useState(false);
    const [exportResult, setExportResult] = useState(null);
    const [exportError, setExportError] = useState(null);
    const [exportLoading, setExportLoading] = useState(false);
    const [rowLimit, setRowLimit] = useState('');
    const [copied, setCopied] = useState(false);

    useEffect(() => {
        if (!datasetSql) {
            setValidationLoading(false);
            return;
        }
        validateDataset();
    }, [datasetSql, sessionId, featureSqls]);

    const validateDataset = async () => {
        setValidationLoading(true);
        setValidationError(null);
        setValidationResult(null);
        try {
            const payload = {
                session_id: sessionId,
                dataset_sql: datasetSql,
            };
            if (featureSqls.length) {
                payload.feature_sqls = featureSqls;
            }
            const res = await api.post('/validate-dataset-sql', payload);
            setValidationResult(res.data);
        } catch (err) {
            setValidationError(err.response?.data?.detail || 'Validation failed');
        } finally {
            setValidationLoading(false);
        }
    };

    const handleExport = async () => {
        if (!validationResult?.valid) {
            setExportError('Fix validation errors before exporting.');
            return;
        }
        setExportLoading(true);
        setExportError(null);
        try {
            const res = await api.post('/export-dataset', {
                session_id: sessionId,
                format: 'csv',
                row_limit: rowLimit ? Number(rowLimit) : null,
            });
            setExportResult(res.data);
        } catch (err) {
            setExportError(err.response?.data?.detail || 'Export failed');
        } finally {
            setExportLoading(false);
        }
    };

    const copyToClipboard = async () => {
        if (!datasetSql) {
            return;
        }
        try {
            await navigator.clipboard.writeText(datasetSql);
            setCopied(true);
            setTimeout(() => setCopied(false), 2000);
        } catch (err) {
            setCopied(false);
        }
    };

    const datasetLines = datasetSql ? datasetSql.split('\n') : [];
    const canExport = Boolean(validationResult?.valid) && !exportLoading;
    const missingDataset = !datasetSql;

    return (
        <div className="p-6 space-y-6">
            <div className="terminal-header">
                STEP 7 OF 7: EXPORT
            </div>

            {missingDataset && (
                <div className="terminal-panel p-4 border-[var(--color-terminal-red)] bg-[#1a0a0a] text-[var(--color-terminal-red)] text-sm">
                    Dataset SQL not found. Go back to Step 6 and assemble the dataset.
                </div>
            )}

            {!missingDataset && (
                <div className="terminal-panel">
                    <div className="terminal-header">DATASET SUMMARY</div>
                    <div className="p-4 grid grid-cols-2 gap-4 text-sm">
                        <div>
                            <span className="text-[var(--color-terminal-text-dim)]">Entity:</span>
                            <span className="data-value ml-2">{entityName}</span>
                        </div>
                        <div>
                            <span className="text-[var(--color-terminal-text-dim)]">Target:</span>
                            <span className="data-value ml-2">{targetName}</span>
                        </div>
                        <div>
                            <span className="text-[var(--color-terminal-text-dim)]">Features:</span>
                            <span className="data-value ml-2">{featureCount}</span>
                        </div>
                        <div>
                            <span className="text-[var(--color-terminal-text-dim)]">Dataset SQL:</span>
                            <span className="data-value ml-2">
                                {datasetLines.length ? `${datasetLines.length} lines` : 'n/a'}
                            </span>
                        </div>
                    </div>
                </div>
            )}

            {!missingDataset && (
                <div className="terminal-panel">
                    <div className="terminal-header flex items-center justify-between">
                        <span>VALIDATION</span>
                        <button onClick={validateDataset} className="btn-terminal text-xs">
                            {validationLoading ? 'VALIDATING...' : 'RE-VALIDATE'}
                        </button>
                    </div>
                    <div className="p-4 space-y-3 text-sm">
                        {validationLoading && (
                            <div className="text-[var(--color-terminal-text-dim)]">Validating dataset SQL...</div>
                        )}
                        {validationError && (
                            <div className="text-[var(--color-terminal-red)]">{validationError}</div>
                        )}
                        {validationResult && (
                            <div>
                                <div>
                                    <span className="text-[var(--color-terminal-text-dim)]">Status:</span>
                                    <span className={`ml-2 ${statusClass(validationResult.status)}`}>
                                        {validationResult.status}
                                    </span>
                                </div>
                                <div className="mt-2">
                                    <span className="text-[var(--color-terminal-text-dim)]">Valid:</span>
                                    <span className={`ml-2 ${validationResult.valid ? 'text-[var(--color-terminal-green)]' : 'text-[var(--color-terminal-red)]'}`}>
                                        {validationResult.valid ? 'YES' : 'NO'}
                                    </span>
                                </div>
                            </div>
                        )}

                        {validationResult?.errors?.length > 0 && (
                            <div className="mt-3 text-[var(--color-terminal-red)] space-y-1">
                                <div className="font-semibold">Errors</div>
                                {validationResult.errors.map((issue, idx) => (
                                    <div key={idx}>{formatIssue(issue)}</div>
                                ))}
                            </div>
                        )}

                        {validationResult?.warnings?.length > 0 && (
                            <div className="mt-3 text-[var(--color-terminal-yellow)] space-y-1">
                                <div className="font-semibold">Warnings</div>
                                {validationResult.warnings.map((issue, idx) => (
                                    <div key={idx}>{formatIssue(issue)}</div>
                                ))}
                            </div>
                        )}

                        {validationResult?.info?.length > 0 && (
                            <div className="mt-3 text-[var(--color-terminal-text-dim)] space-y-1">
                                <div className="font-semibold">Info</div>
                                {validationResult.info.map((issue, idx) => (
                                    <div key={idx}>{formatIssue(issue)}</div>
                                ))}
                            </div>
                        )}
                    </div>
                </div>
            )}

            {!missingDataset && (
                <div className="terminal-panel">
                    <div className="terminal-header">EXPORT OPTIONS</div>
                    <div className="p-4 space-y-3 text-sm">
                        <div className="flex items-center gap-3">
                            <label className="text-[var(--color-terminal-text-dim)]">Row limit (optional):</label>
                            <input
                                type="number"
                                min={1}
                                value={rowLimit}
                                onChange={(e) => setRowLimit(e.target.value)}
                                className="input-terminal w-32 text-center"
                            />
                        </div>
                        {exportError && (
                            <div className="text-[var(--color-terminal-red)]">{exportError}</div>
                        )}
                        {exportResult && (
                            <div className="space-y-1">
                                <div>
                                    <span className="text-[var(--color-terminal-text-dim)]">File:</span>
                                    <span className="data-value ml-2">{exportResult.file_path}</span>
                                </div>
                                <div>
                                    <span className="text-[var(--color-terminal-text-dim)]">Metadata:</span>
                                    <span className="data-value ml-2">{exportResult.metadata_path}</span>
                                </div>
                                <div>
                                    <span className="text-[var(--color-terminal-text-dim)]">Rows:</span>
                                    <span className="data-value ml-2">{exportResult.row_count?.toLocaleString?.() ?? exportResult.row_count}</span>
                                </div>
                            </div>
                        )}
                        <button
                            onClick={handleExport}
                            disabled={!canExport}
                            className={`btn-terminal-primary px-6 ${!canExport ? 'opacity-50 cursor-not-allowed' : ''}`}
                        >
                            {exportLoading ? 'EXPORTING...' : 'EXPORT DATASET'}
                        </button>
                    </div>
                </div>
            )}

            {!missingDataset && (
                <div className="terminal-panel">
                    <div className="terminal-header flex items-center justify-between">
                        <span>SQL PREVIEW (first 30 lines)</span>
                        <button onClick={copyToClipboard} className="btn-terminal text-xs">
                            {copied ? 'COPIED' : 'COPY SQL'}
                        </button>
                    </div>
                    <div className="p-4 bg-[#050505] max-h-80 overflow-auto">
                        <pre className="text-xs text-[var(--color-terminal-text)] whitespace-pre-wrap font-mono">
                            {datasetLines.slice(0, 30).join('\n')}
                            {datasetLines.length > 30 && (
                                <span className="text-[var(--color-terminal-text-dim)]">
                                    {'\n'}... ({datasetLines.length - 30} more lines)
                                </span>
                            )}
                        </pre>
                    </div>
                </div>
            )}

            <div className="flex justify-between">
                <button onClick={onBack} className="btn-terminal">
                    BACK
                </button>
                <button onClick={onRestart} className="btn-terminal">
                    BACK TO SCHEMA
                </button>
            </div>
        </div>
    );
}
