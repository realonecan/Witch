import { useState } from 'react';
import { motion } from 'framer-motion';

/**
 * ValidationPanel
 * 
 * Displays validation results:
 * - Errors (blocking)
 * - Warnings
 * - Info messages
 */
export function ValidationPanel({
    validationResult,
    isValidating,
    onValidate,
    onProceed,
}) {
    const { valid, errors = [], warnings = [], info = [] } = validationResult || {};

    const getSeverityStyle = (severity) => {
        switch (severity) {
            case 'error':
                return { bg: '#ff174420', border: '#ff1744', text: '#ff1744', icon: '❌' };
            case 'warning':
                return { bg: '#ff6b0020', border: '#ff6b00', text: '#ff6b00', icon: '⚠️' };
            default:
                return { bg: '#2196f320', border: '#2196f3', text: '#2196f3', icon: 'ℹ️' };
        }
    };

    return (
        <motion.div
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
            exit={{ opacity: 0, x: -20 }}
            className="space-y-4"
        >
            {/* Header */}
            <div className="flex items-center justify-between">
                <div className="text-[11px] text-[#808080] uppercase tracking-wide">
                    🔍 SQL VALIDATION
                </div>
                {onValidate && (
                    <button
                        type="button"
                        onClick={onValidate}
                        disabled={isValidating}
                        className="btn-terminal text-[10px] px-4 py-1 disabled:opacity-50"
                    >
                        {isValidating ? '⏳ VALIDATING...' : '↻ RE-VALIDATE'}
                    </button>
                )}
            </div>

            {/* Loading State */}
            {isValidating && (
                <div className="p-6 bg-[#121212] border border-[#2a2a2a] text-center">
                    <span className="text-[12px] text-[#ffc107] animate-pulse">
                        ⏳ RUNNING VALIDATION CHECKS...
                    </span>
                </div>
            )}

            {/* Results */}
            {!isValidating && validationResult && (
                <>
                    {/* Status Banner */}
                    <div className={`p-4 border ${valid
                            ? 'bg-[#00c85310] border-[#00c853]'
                            : 'bg-[#ff174420] border-[#ff1744]'
                        }`}>
                        <div className="flex items-center gap-3">
                            <span className="text-[20px]">{valid ? '✓' : '✕'}</span>
                            <div>
                                <div className={`text-[14px] font-bold ${valid ? 'text-[#00c853]' : 'text-[#ff1744]'}`}>
                                    {valid ? 'VALIDATION PASSED' : 'VALIDATION FAILED'}
                                </div>
                                <div className="text-[11px] text-[#808080]">
                                    {errors.length} errors • {warnings.length} warnings • {info.length} info
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* Errors */}
                    {errors.length > 0 && (
                        <div className="space-y-2">
                            <div className="text-[10px] text-[#ff1744] uppercase tracking-wide">
                                ERRORS ({errors.length})
                            </div>
                            {errors.map((issue, i) => {
                                const style = getSeverityStyle('error');
                                return (
                                    <div key={i} className={`p-3 bg-[${style.bg}] border border-[${style.border}]`}
                                        style={{ backgroundColor: style.bg, borderColor: style.border }}>
                                        <div className="flex items-start gap-2">
                                            <span>{style.icon}</span>
                                            <div className="flex-1">
                                                <div className="text-[11px] text-[#e0e0e0] font-mono">
                                                    [{issue.code}] {issue.message}
                                                </div>
                                                {issue.location && (
                                                    <div className="text-[10px] text-[#808080] mt-1">
                                                        Location: {issue.location}
                                                    </div>
                                                )}
                                                {issue.suggestion && (
                                                    <div className="text-[10px] text-[#00c853] mt-1">
                                                        💡 {issue.suggestion}
                                                    </div>
                                                )}
                                            </div>
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    )}

                    {/* Warnings */}
                    {warnings.length > 0 && (
                        <div className="space-y-2">
                            <div className="text-[10px] text-[#ff6b00] uppercase tracking-wide">
                                WARNINGS ({warnings.length})
                            </div>
                            {warnings.map((issue, i) => {
                                const style = getSeverityStyle('warning');
                                return (
                                    <div key={i} className="p-3 border"
                                        style={{ backgroundColor: style.bg, borderColor: style.border }}>
                                        <div className="flex items-start gap-2">
                                            <span>{style.icon}</span>
                                            <div className="flex-1">
                                                <div className="text-[11px] text-[#e0e0e0] font-mono">
                                                    [{issue.code}] {issue.message}
                                                </div>
                                                {issue.suggestion && (
                                                    <div className="text-[10px] text-[#00c853] mt-1">
                                                        💡 {issue.suggestion}
                                                    </div>
                                                )}
                                            </div>
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    )}

                    {/* Proceed Button */}
                    {valid && onProceed && (
                        <button
                            type="button"
                            onClick={onProceed}
                            className="btn-terminal w-full text-[11px] py-3 bg-[#00c853] text-black hover:bg-[#00a844]"
                        >
                            ✓ VALIDATION PASSED - CONTINUE TO EXPORT
                        </button>
                    )}
                </>
            )}
        </motion.div>
    );
}
