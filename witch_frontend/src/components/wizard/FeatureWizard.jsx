import { useState } from 'react';
import WizardBreadcrumb from './WizardBreadcrumb';
import SchemaExplorer from './SchemaExplorer';
import TableSelector from './TableSelector';
import GrainDefiner from './GrainDefiner';
import TargetDefiner from './TargetDefiner';
import FeatureBuilder from './FeatureBuilder';
import QualityChecker from './QualityChecker';
import ExportPanel from './ExportPanel';

export default function FeatureWizard({ sessionId, connectionStatus, onClose }) {
    const [currentStep, setCurrentStep] = useState(1);
    const [completedSteps, setCompletedSteps] = useState([]);
    const [wizardState, setWizardState] = useState({
        entity: null,
        tables: [],
        grain: null,
        target: null,
        features: null,
        quality: null,
    });

    const markStepComplete = (step) => {
        if (!completedSteps.includes(step)) {
            setCompletedSteps([...completedSteps, step]);
        }
    };

    const goToStep = (step) => {
        setCurrentStep(step);
    };

    // Step handlers
    const handleEntitySelect = (entity) => {
        setWizardState(prev => ({ ...prev, entity }));
        markStepComplete(1);
        setCurrentStep(2);
    };

    const handleTablesSelect = (tables) => {
        setWizardState(prev => ({ ...prev, tables }));
        markStepComplete(2);
        setCurrentStep(3);
    };

    const handleGrainDefine = (grain) => {
        setWizardState(prev => ({ ...prev, grain }));
        markStepComplete(3);
        setCurrentStep(4);
    };

    const handleTargetDefine = (target) => {
        setWizardState(prev => ({ ...prev, target }));
        markStepComplete(4);
        setCurrentStep(5);
    };

    const handleFeaturesDefine = (features) => {
        setWizardState(prev => ({ ...prev, features }));
        markStepComplete(5);
        setCurrentStep(6);
    };

    const handleQualityApprove = (quality) => {
        setWizardState(prev => ({ ...prev, quality }));
        markStepComplete(6);
        setCurrentStep(7);
    };

    const handleRestart = () => {
        setCurrentStep(1);
        setCompletedSteps([]);
        setWizardState({
            entity: null,
            tables: [],
            grain: null,
            target: null,
            features: null,
            quality: null,
        });
    };

    const renderCurrentStep = () => {
        switch (currentStep) {
            case 1:
                return (
                    <SchemaExplorer
                        sessionId={sessionId}
                        connectionStatus={connectionStatus}
                        onEntitySelect={handleEntitySelect}
                    />
                );
            case 2:
                return (
                    <TableSelector
                        sessionId={sessionId}
                        selectedEntity={wizardState.entity}
                        onTablesSelect={handleTablesSelect}
                        onBack={() => goToStep(1)}
                    />
                );
            case 3:
                return (
                    <GrainDefiner
                        sessionId={sessionId}
                        selectedEntity={wizardState.entity}
                        selectedTables={wizardState.tables}
                        onGrainDefine={handleGrainDefine}
                        onBack={() => goToStep(2)}
                    />
                );
            case 4:
                return (
                    <TargetDefiner
                        sessionId={sessionId}
                        selectedTables={wizardState.tables}
                        grainDefinition={wizardState.grain?.definition}
                        onTargetDefine={handleTargetDefine}
                        onBack={() => goToStep(3)}
                    />
                );
            case 5:
                return (
                    <FeatureBuilder
                        sessionId={sessionId}
                        selectedTables={wizardState.tables}
                        selectedEntity={wizardState.entity}
                        grainDefinition={wizardState.grain?.definition}
                        onFeaturesDefine={handleFeaturesDefine}
                        onBack={() => goToStep(4)}
                    />
                );
            case 6:
                return (
                    <QualityChecker
                        sessionId={sessionId}
                        featuresConfig={wizardState.features}
                        onQualityApprove={handleQualityApprove}
                        onBack={() => goToStep(5)}
                    />
                );
            case 7:
                return (
                    <ExportPanel
                        sessionId={sessionId}
                        wizardState={wizardState}
                        onBack={() => goToStep(6)}
                        onRestart={handleRestart}
                    />
                );
            default:
                return null;
        }
    };

    return (
        <div className="min-h-screen bg-[var(--color-terminal-bg)] flex flex-col">
            {/* Breadcrumb Navigation */}
            <WizardBreadcrumb
                currentStep={currentStep}
                completedSteps={completedSteps}
                onStepClick={goToStep}
                onClose={onClose}
            />

            {/* Main Content */}
            <div className="flex-1 overflow-y-auto">
                {renderCurrentStep()}
            </div>
        </div>
    );
}
