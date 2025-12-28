import React from 'react';

const STEPS = [
    { id: 1, name: 'Schema', short: 'Schema' },
    { id: 2, name: 'Tables', short: 'Tables' },
    { id: 3, name: 'Grain', short: 'Grain' },
    { id: 4, name: 'Target', short: 'Target' },
    { id: 5, name: 'Features', short: 'Features' },
    { id: 6, name: 'Quality', short: 'Quality' },
    { id: 7, name: 'Export', short: 'Export' },
];

export default function WizardBreadcrumb({ currentStep, completedSteps, onStepClick, onClose }) {
    return (
        <div className="terminal-panel border-b-2 border-[var(--color-terminal-border)]">
            {/* Header */}
            <div className="flex items-center justify-between px-4 py-2 border-b border-[var(--color-terminal-border)]">
                <div className="flex items-center gap-3">
                    <span className="text-[var(--color-terminal-orange)] text-lg">⚗️</span>
                    <span className="text-[var(--color-terminal-text-bright)] font-semibold tracking-wider text-sm">
                        ML FEATURE SQL GENERATOR
                    </span>
                </div>
                <button
                    type="button"
                    onClick={onClose}
                    className="btn-terminal text-xs px-3 py-1"
                >
                    ✕ CLOSE
                </button>
            </div>

            {/* Steps */}
            <div className="flex items-center px-4 py-3 gap-2 overflow-x-auto">
                {STEPS.map((step, idx) => {
                    const isCompleted = completedSteps.includes(step.id);
                    const isCurrent = currentStep === step.id;
                    const isClickable = isCompleted || step.id === currentStep;

                    return (
                        <React.Fragment key={step.id}>
                            {idx > 0 && (
                                <span className="text-[var(--color-terminal-text-dim)] text-xs">→</span>
                            )}
                            <button
                                onClick={() => isClickable && onStepClick(step.id)}
                                disabled={!isClickable}
                                className={`
                  flex items-center gap-2 px-3 py-1.5 text-xs font-medium tracking-wide
                  transition-colors duration-100
                  ${isCurrent
                                        ? 'bg-[var(--color-terminal-orange)] text-black'
                                        : isCompleted
                                            ? 'text-[var(--color-terminal-green)] hover:bg-[var(--color-terminal-bg-light)] cursor-pointer'
                                            : 'text-[var(--color-terminal-text-dim)] cursor-not-allowed'
                                    }
                `}
                            >
                                <span className="text-xs">
                                    {isCompleted ? '✓' : `①②③④⑤⑥⑦`.charAt(step.id - 1)}
                                </span>
                                <span>{step.short}</span>
                            </button>
                        </React.Fragment>
                    );
                })}
            </div>
        </div>
    );
}
