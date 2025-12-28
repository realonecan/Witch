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
    if (typeof issue === 'string') {
        return issue;
    }
    if (issue.message) {
        return issue.message;
    }
    return JSON.stringify(issue);
};

const formatValue = (value) => {
    if (value === null || value === undefined) {
        return '-';
    }
    if (typeof value === 'object') {
        try {
            return JSON.stringify(value);
        } catch (err) {
            return String(value);
        }
    }
    return String(value);
};

const formatPercent = (value) => {
    if (typeof value !== 'number') {
        return '-';
    }
    return `${(value * 100).toFixed(1)}%`;
};

export default function QualityChecker({
    sessionId,
    featuresConfig,
    onQualityApprove,
    onBack
}) {
    const [result, setResult] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    const featureInputs = useMemo(() => featuresConfig?.feature_sqls || [], [featuresConfig]);

    useEffect(() => {
        assembleDataset();
    }, [sessionId, featureInputs]);

    const assembleDataset = async () => {
        if (!sessionId) {
            setError('Missing session ID.');
            setLoading(false);
            return;
        }
        if (!featureInputs.length) {
            setError('No features defined. Go back and generate features first.');
            setLoading(false);
            return;
        }

        setLoading(true);
        setError(null);
        setResult(null);
        try {
            const res = await api.post('/assemble-dataset', {
                session_id: sessionId,
                features: featureInputs,
                run_quality_checks: true,
            });
            setResult(res.data);
        } catch (err) {
            setError(err.response?.data?.detail || 'Failed to assemble dataset');
        } finally {
            setLoading(false);
        }
    };

    const handleContinue = () => {
        if (!result || result.status === 'error') {
            setError('Fix dataset assembly errors before continuing.');
            return;
        }
        onQualityApprove({
            dataset_sql: result.dataset_sql,
            quality_report: result.quality_report,
            warnings: result.warnings,
            errors: result.errors,
            status: result.status,
            feature_count: result.feature_count,
        });
    };

    const qualityReport = result?.quality_report;
    const datasetLineCount = result?.dataset_sql ? result.dataset_sql.split('\n').length : 0;

    const renderSummary = (title, data) => {
        if (!data || Object.keys(data).length === 0) {
            return null;
        }
        return (
            <div className="terminal-panel">
                <div className="terminal-header">{title}</div>
                <div className="p-4 grid grid-cols-2 gap-3 text-sm">
                    {Object.entries(data).map(([key, value]) => (
                        <div key={key} className="flex gap-2">
                            <span className="text-[var(--color-terminal-text-dim)]">{key}:</span>
                            <span className="data-value">{formatValue(value)}</span>
                        </div>
                    ))}
                </div>
            </div>
        );
    };

    if (loading) {
        return (
            <div className="flex items-center justify-center h-64">
                <span className="text-[var(--color-terminal-text-dim)]">RUNNING QUALITY CHECKS...</span>
            </div>
        );
    }

    return (
        <div className="p-6 space-y-6">
            <div className="terminal-header">
                STEP 6 OF 7: QUALITY CHECKS
            </div>

            {error && (
                <div className="terminal-panel p-4 border-[var(--color-terminal-red)] bg-[#1a0a0a] text-[var(--color-terminal-red)] text-sm">
                    {error}
                </div>
            )}

            {result && (
                <div className="terminal-panel">
                    <div className="terminal-header">ASSEMBLY SUMMARY</div>
                    <div className="p-4 grid grid-cols-2 gap-4 text-sm">
                        <div>
                            <span className="text-[var(--color-terminal-text-dim)]">Status:</span>
                            <span className={`ml-2 ${statusClass(result.status)}`}>{result.status || 'unknown'}</span>
                        </div>
                        <div>
                            <span className="text-[var(--color-terminal-text-dim)]">Features:</span>
                            <span className="data-value ml-2">
                                {result.feature_count ?? featureInputs.length}
                            </span>
                        </div>
                        <div>
                            <span className="text-[var(--color-terminal-text-dim)]">Quality:</span>
                            <span className={`ml-2 ${statusClass(qualityReport?.overall_status)}`}>
                                {qualityReport?.overall_status || 'n/a'}
                            </span>
                        </div>
                        <div>
                            <span className="text-[var(--color-terminal-text-dim)]">Dataset SQL:</span>
                            <span className="data-value ml-2">
                                {datasetLineCount ? `${datasetLineCount} lines` : 'n/a'}
                            </span>
                        </div>
                    </div>
                </div>
            )}

            {result?.errors?.length > 0 && (
                <div className="terminal-panel p-4 border-[var(--color-terminal-red)] bg-[#1a0a0a] text-[var(--color-terminal-red)] text-sm space-y-1">
                    <div className="text-[var(--color-terminal-red)] font-semibold">ASSEMBLY ERRORS</div>
                    {result.errors.map((issue, idx) => (
                        <div key={idx}>{formatIssue(issue)}</div>
                    ))}
                </div>
            )}

            {result?.warnings?.length > 0 && (
                <div className="terminal-panel p-4 border-[var(--color-terminal-yellow)] bg-[#1a1a0a] text-[var(--color-terminal-yellow)] text-sm space-y-1">
                    <div className="text-[var(--color-terminal-yellow)] font-semibold">ASSEMBLY WARNINGS</div>
                    {result.warnings.map((warn, idx) => (
                        <div key={idx}>{formatIssue(warn)}</div>
                    ))}
                </div>
            )}

            {renderSummary('GRAIN SUMMARY', qualityReport?.grain)}
            {renderSummary('TARGET SUMMARY', qualityReport?.target)}
            {renderSummary('FEATURE SUMMARY', qualityReport?.features)}

            {qualityReport?.joinability_checks?.length > 0 && (
                <div className="terminal-panel">
                    <div className="terminal-header">JOINABILITY CHECKS</div>
                    <div className="overflow-x-auto">
                        <table className="terminal-table">
                            <thead>
                                <tr>
                                    <th>Check</th>
                                    <th>Match Rate</th>
                                    <th>Matched</th>
                                    <th>Unmatched</th>
                                    <th>Status</th>
                                    <th>Warning</th>
                                </tr>
                            </thead>
                            <tbody>
                                {qualityReport.joinability_checks.map((check, idx) => (
                                    <tr key={`${check.name}-${idx}`}>
                                        <td className="data-value">{check.name}</td>
                                        <td>{formatPercent(check.match_rate)}</td>
                                        <td>{check.matched_rows?.toLocaleString?.() ?? check.matched_rows}</td>
                                        <td>{check.unmatched_rows?.toLocaleString?.() ?? check.unmatched_rows}</td>
                                        <td className={statusClass(check.status)}>{check.status}</td>
                                        <td className="text-xs text-[var(--color-terminal-text-dim)]">
                                            {check.warning || '-'}
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </div>
            )}

            {qualityReport?.leakage_checks?.length > 0 && (
                <div className="terminal-panel">
                    <div className="terminal-header">LEAKAGE CHECKS</div>
                    <div className="overflow-x-auto">
                        <table className="terminal-table">
                            <thead>
                                <tr>
                                    <th>Feature</th>
                                    <th>Leakage</th>
                                    <th>Sample Size</th>
                                    <th>Status</th>
                                    <th>Message</th>
                                </tr>
                            </thead>
                            <tbody>
                                {qualityReport.leakage_checks.map((check, idx) => (
                                    <tr key={`${check.feature_name}-${idx}`}>
                                        <td className="data-value">{check.feature_name}</td>
                                        <td className={check.leakage_detected ? 'text-[var(--color-terminal-red)]' : 'text-[var(--color-terminal-green)]'}>
                                            {check.leakage_detected ? 'YES' : 'NO'}
                                        </td>
                                        <td>{check.sample_size?.toLocaleString?.() ?? check.sample_size}</td>
                                        <td className={statusClass(check.status)}>{check.status}</td>
                                        <td className="text-xs text-[var(--color-terminal-text-dim)]">
                                            {check.message || '-'}
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </div>
            )}

            {qualityReport?.errors?.length > 0 && (
                <div className="terminal-panel p-4 border-[var(--color-terminal-red)] bg-[#1a0a0a] text-[var(--color-terminal-red)] text-sm space-y-1">
                    <div className="text-[var(--color-terminal-red)] font-semibold">QUALITY ERRORS</div>
                    {qualityReport.errors.map((err, idx) => (
                        <div key={idx}>{formatIssue(err)}</div>
                    ))}
                </div>
            )}

            {qualityReport?.warnings?.length > 0 && (
                <div className="terminal-panel p-4 border-[var(--color-terminal-yellow)] bg-[#1a1a0a] text-[var(--color-terminal-yellow)] text-sm space-y-1">
                    <div className="text-[var(--color-terminal-yellow)] font-semibold">QUALITY WARNINGS</div>
                    {qualityReport.warnings.map((warn, idx) => (
                        <div key={idx}>{formatIssue(warn)}</div>
                    ))}
                </div>
            )}

            {qualityReport?.recommendations?.length > 0 && (
                <div className="terminal-panel p-4 text-sm space-y-1">
                    <div className="font-semibold">RECOMMENDATIONS</div>
                    {qualityReport.recommendations.map((rec, idx) => (
                        <div key={idx}>{rec}</div>
                    ))}
                </div>
            )}

            <div className="flex justify-between">
                <button onClick={onBack} className="btn-terminal">
                    BACK
                </button>
                <button
                    onClick={handleContinue}
                    disabled={!result || result.status === 'error'}
                    className={`btn-terminal-primary px-6 ${!result || result.status === 'error' ? 'opacity-50 cursor-not-allowed' : ''}`}
                >
                    CONTINUE
                </button>
            </div>
        </div>
    );
}
