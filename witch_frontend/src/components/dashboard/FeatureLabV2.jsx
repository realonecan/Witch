import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { useWitch } from '../../hooks/useWitch';
import { GrainDefiner } from './GrainDefiner';
import { TargetDefiner } from './TargetDefiner';
import { FeatureBuilder } from './FeatureBuilder';
import { MissingHandler } from './MissingHandler';
import { ValidationPanel } from './ValidationPanel';
import { ExportPanel } from './ExportPanel';
import apiClient from '../../api/client';

/**
 * FeatureLabV2 - ML Dataset Preparation Workflow
 * 
 * 6-Step Bloomberg-style wizard:
 * 1. Define Grain (entity + observation date)
 * 2. Define Target (column + positive values)
 * 3. Generate Features (template-based)
 * 4. Handle Missing (strategy selection)
 * 5. Validate (SQL checks)
 * 6. Export (CSV + metadata)
 */
export function FeatureLabV2({
    isOpen,
    onClose,
    dbSessionId,
    tables = [],
}) {
    // Use the witch hook for API calls
    const {
        defineGrain,
        previewGrain,
        defineTarget,
        getTargetDistribution,
        listFeatureTemplates,
        generateFeature,
        assembleDataset,
        applyMissingStrategy,
        validateDatasetSql,
        exportDataset,
    } = useWitch();

    // Step tracking (1-6)
    const [step, setStep] = useState(1);

    // Workflow state
    const [grainConfig, setGrainConfig] = useState(null);
    const [targetConfig, setTargetConfig] = useState(null);
    const [features, setFeatures] = useState([]);
    const [missingStrategies, setMissingStrategies] = useState([]);
    const [validationResult, setValidationResult] = useState(null);
    const [datasetSql, setDatasetSql] = useState(null);
    const [exportResult, setExportResult] = useState(null);

    // UI state
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState(null);

    // Step labels
    const stepLabels = [
        'DEFINE GRAIN',
        'DEFINE TARGET',
        'BUILD FEATURES',
        'HANDLE MISSING',
        'VALIDATE',
        'EXPORT'
    ];

    // Reset when modal opens
    useEffect(() => {
        if (isOpen) {
            setStep(1);
            setGrainConfig(null);
            setTargetConfig(null);
            setFeatures([]);
            setMissingStrategies([]);
            setValidationResult(null);
            setDatasetSql(null);
            setExportResult(null);
            setError(null);
        }
    }, [isOpen]);

    // Helper: get table columns
    const getTableColumns = async (tableName) => {
        try {
            const response = await apiClient.post('/table-columns', {
                session_id: dbSessionId,
                table_name: tableName,
            });
            return response.data;
        } catch (err) {
            console.error('Failed to get columns:', err);
            return { columns: [] };
        }
    };

    
    const handleGrainDefined = (grain) => {
        setGrainConfig(grain);
        setStep(2);
    };

    
    const handleTargetDefined = async (target) => {
        setTargetConfig(target);
        setStep(3);
    };

    
    const handleFeaturesGenerated = (generatedFeatures) => {
        setFeatures(generatedFeatures);
        setStep(4);
    };

    
    const handleMissingApplied = async () => {
        setIsLoading(true);
        setError(null);

        try {
            // Assemble dataset
            const result = await assembleDataset({
                grain: grainConfig,
                target: targetConfig,
                features: features,
                missing_strategies: missingStrategies,
            });

            if (result.error) {
                setError(result.error);
            } else {
                setDatasetSql(result.dataset_sql);
                // Auto-validate
                await handleValidate(result.dataset_sql);
                setStep(5);
            }
        } catch (err) {
            setError(err.message);
        } finally {
            setIsLoading(false);
        }
    };

    
    const handleValidate = async (sql) => {
        setIsLoading(true);
        try {
            const result = await validateDatasetSql({
                dataset_sql: sql || datasetSql,
            });
            setValidationResult(result);
        } catch (err) {
            setValidationResult({ valid: false, errors: [{ message: err.message }] });
        } finally {
            setIsLoading(false);
        }
    };

    
    const handleProceedToExport = () => {
        if (validationResult?.valid) {
            setStep(6);
        }
    };

    // Navigation
    const canGoBack = step > 1;
    const goBack = () => {
        if (step > 1) setStep(step - 1);
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
                        <span>ML DATASET BUILDER</span>
                        <span className="text-[10px] text-[#606060]">v2</span>
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
                    {stepLabels.map((label, i) => {
                        const s = i + 1;
                        return (
                            <div key={s} className="flex items-center flex-shrink-0">
                                <div className={`flex items-center justify-center w-6 h-6 rounded-full text-[10px] font-bold ${step === s
                                    ? 'bg-[#ff6b00] text-black'
                                    : step > s
                                        ? 'bg-[#00c853] text-black'
                                        : 'bg-[#2a2a2a] text-[#606060]'
                                    }`}>
                                    {step > s ? '✓' : s}
                                </div>
                                <span className={`ml-2 text-[10px] font-mono whitespace-nowrap ${step === s ? 'text-[#ff6b00]' : 'text-[#606060]'
                                    }`}>
                                    {label}
                                </span>
                                {s < 6 && <div className="w-4 h-px bg-[#2a2a2a] mx-2" />}
                            </div>
                        );
                    })}
                </div>

                {/* Content */}
                <div className="flex-1 overflow-auto p-4">
                    <AnimatePresence mode="wait">
                        
                        {step === 1 && (
                            <GrainDefiner
                                key="step1"
                                dbSessionId={dbSessionId}
                                tables={tables}
                                onGrainDefined={handleGrainDefined}
                                defineGrain={defineGrain}
                                previewGrain={previewGrain}
                                getTableColumns={getTableColumns}
                            />
                        )}

                        
                        {step === 2 && (
                            <TargetDefiner
                                key="step2"
                                dbSessionId={dbSessionId}
                                tableName={grainConfig?.entity_table}
                                groupingColumn={grainConfig?.entity_id_column}
                                defineTarget={defineTarget}
                                getTargetDistribution={getTargetDistribution}
                                onTargetDefined={handleTargetDefined}
                            />
                        )}

                        
                        {step === 3 && (
                            <FeatureBuilder
                                key="step3"
                                dbSessionId={dbSessionId}
                                tables={tables}
                                grainConfig={grainConfig}
                                listFeatureTemplates={listFeatureTemplates}
                                generateFeature={generateFeature}
                                getTableColumns={getTableColumns}
                                onFeaturesGenerated={handleFeaturesGenerated}
                            />
                        )}

                        
                        {step === 4 && (
                            <MissingHandler
                                key="step4"
                                features={features}
                                applyMissingStrategy={applyMissingStrategy}
                                onStrategiesApplied={(result) => {
                                    setMissingStrategies(result.strategies);
                                    handleMissingApplied();
                                }}
                            />
                        )}

                        
                        {step === 5 && (
                            <ValidationPanel
                                key="step5"
                                validationResult={validationResult}
                                isValidating={isLoading}
                                onValidate={() => handleValidate(datasetSql)}
                                onProceed={handleProceedToExport}
                            />
                        )}

                        
                        {step === 6 && (
                            <ExportPanel
                                key="step6"
                                exportDataset={exportDataset}
                                datasetSql={datasetSql}
                            />
                        )}
                    </AnimatePresence>
                </div>

                {/* Footer */}
                <div className="flex items-center justify-between px-4 py-2 bg-[#121212] border-t border-[#2a2a2a]">
                    <div>
                        {canGoBack && step < 5 && (
                            <button
                                type="button"
                                onClick={goBack}
                                className="btn-terminal text-[10px] px-4 py-1"
                            >
                                ← BACK
                            </button>
                        )}
                    </div>
                    <div className="text-[10px] text-[#606060]">
                        STEP {step} OF 6
                    </div>
                </div>
            </motion.div>
        </div>
    );
}
