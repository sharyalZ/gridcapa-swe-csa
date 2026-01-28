package com.farao_community.farao.swe_csa.app.security_evaluator;

import com.farao_community.farao.swe_csa.api.exception.CsaInternalException;
import com.farao_community.farao.swe_csa.app.dichotomy.DichotomyStepResult;
import com.farao_community.farao.swe_csa.app.dichotomy.ParallelDichotomiesResult;
import com.powsybl.computation.ComputationManager;
import com.powsybl.contingency.Contingency;
import com.powsybl.glsk.commons.ZonalData;
import com.powsybl.iidm.modification.scalable.Scalable;
import com.powsybl.iidm.network.Network;
import com.powsybl.loadflow.LoadFlowParameters;
import com.powsybl.openrao.commons.OpenRaoException;
import com.powsybl.openrao.commons.PhysicalParameter;
import com.powsybl.openrao.commons.Unit;
import com.powsybl.openrao.data.crac.api.Crac;
import com.powsybl.openrao.data.crac.api.Instant;
import com.powsybl.openrao.data.crac.api.RemedialAction;
import com.powsybl.openrao.data.crac.api.State;
import com.powsybl.openrao.data.crac.api.cnec.Cnec;
import com.powsybl.openrao.data.crac.api.cnec.CnecValue;
import com.powsybl.openrao.data.crac.api.networkaction.NetworkAction;
import com.powsybl.openrao.data.crac.impl.AngleCnecValue;
import com.powsybl.openrao.data.crac.impl.VoltageCnecValue;
import com.powsybl.openrao.data.raoresult.api.RaoResult;
import com.powsybl.openrao.monitoring.MonitoringInput;
import com.powsybl.openrao.monitoring.results.CnecResult;
import com.powsybl.openrao.monitoring.results.MonitoringResult;
import com.powsybl.openrao.monitoring.results.RaoResultWithAngleMonitoring;
import com.powsybl.openrao.monitoring.results.RaoResultWithVoltageMonitoring;
import com.powsybl.openrao.util.AbstractNetworkPool;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.MDC;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.farao_community.farao.swe_csa.app.security_evaluator.ResultValidatorHelper.*;

public class SweCsaRaoResultValidator {

    private final String loadFlowProvider;
    private final LoadFlowParameters loadFlowParameters;
    Map<PhysicalParameter, Unit> parameterToUnitMap = new HashMap<>();
    private final Logger businessLogger;

    public SweCsaRaoResultValidator(String loadFlowProvider, LoadFlowParameters loadFlowParameters, Logger businessLogger) {
        this.loadFlowProvider = loadFlowProvider;
        this.loadFlowParameters = loadFlowParameters;
        this.parameterToUnitMap.put(PhysicalParameter.ANGLE, Unit.DEGREE);
        this.parameterToUnitMap.put(PhysicalParameter.VOLTAGE, Unit.KILOVOLT);
        this.parameterToUnitMap.put(PhysicalParameter.FLOW, Unit.AMPERE);
        this.businessLogger = businessLogger;
    }

    /**
     * Check if the network is secure and update raoResults with Angle/Voltage monitoring
     * when considering Cnecs and RAs from 2 borders
     *
     * @param network                                  the network
     * @param parallelDichotomiesResult                results of dichotomy computation after validating and monitoring for each border separately
     * @param frEsCrac                                 crac of fr-es border
     * @param ptEsCrac                                 crac of pt-es border
     * @param scalableZonalDataFilteredForSweCountries used for the redispatching in the case of Angle monitoring
     * @return ParallelDichotomiesResult updated parallelDichotomiesResult after validating for both borders at once and
     * updating angle/voltage monitoring results
     */
    public ParallelDichotomiesResult validateNetworkForTwoBorders(Network network, ParallelDichotomiesResult parallelDichotomiesResult, Crac frEsCrac, Crac ptEsCrac, ZonalData<Scalable> scalableZonalDataFilteredForSweCountries) {
        RaoResult frEsRaoResult = parallelDichotomiesResult.getFrEsResult().getRaoResult();
        RaoResult ptEsRaoResult = parallelDichotomiesResult.getPtEsResult().getRaoResult();
        try {
            // Check if all the flowCnecs in two borders are secure after applying all RAs from two borders
            TwoBordersFlowCnecSecurityChecker twoBordersFlowCnecSecurityChecker = new TwoBordersFlowCnecSecurityChecker(network, frEsCrac, ptEsCrac, frEsRaoResult, ptEsRaoResult, Runtime.getRuntime().availableProcessors(), businessLogger, loadFlowProvider, loadFlowParameters);
            Pair<Boolean, Boolean> isSecurePair = twoBordersFlowCnecSecurityChecker.check();
            if ((isSecurePair.getLeft() || isSecurePair.getRight()) && (!frEsCrac.getAngleCnecs().isEmpty() || !ptEsCrac.getAngleCnecs().isEmpty())) {
                // If angleCnecs exist, Angle monitoring
                // Fixme: if one AngleCnecList is not empty and another one is emtpy. The angle monitoring is processed for the border without angle cnec?
                Pair<RaoResult, RaoResult> raoResultPair = updateRaoResultsWithAngleMonitoringForTwoBorders(network, frEsCrac, ptEsCrac, scalableZonalDataFilteredForSweCountries, frEsRaoResult, ptEsRaoResult);
                frEsRaoResult = raoResultPair.getLeft();
                ptEsRaoResult = raoResultPair.getRight();
                isSecurePair = Pair.of(frEsRaoResult.isSecure(PhysicalParameter.FLOW, PhysicalParameter.ANGLE) && isSecurePair.getLeft(), ptEsRaoResult.isSecure(PhysicalParameter.FLOW, PhysicalParameter.ANGLE) && isSecurePair.getRight());
                if (isSecurePair.getRight() && isSecurePair.getLeft()) {
                    businessLogger.info("Angle monitoring secure for both borders, Final result will contain Angle monitoring results");
                } else {
                    businessLogger.info("Angle monitoring unsecure for at least one border");
                }
            }

            if ((isSecurePair.getLeft() || isSecurePair.getRight()) && (!frEsCrac.getVoltageCnecs().isEmpty() || !ptEsCrac.getVoltageCnecs().isEmpty())) {
                // If voltageCnecs exist, Voltage monitoring
                Pair<RaoResult, RaoResult> raoResultPair = updateRaoResultsWithVoltageMonitoringForTwoBorders(network, frEsCrac, ptEsCrac, frEsRaoResult, ptEsRaoResult);
                frEsRaoResult = raoResultPair.getLeft();
                ptEsRaoResult = raoResultPair.getRight();
                isSecurePair = Pair.of(frEsRaoResult.isSecure(PhysicalParameter.FLOW, PhysicalParameter.VOLTAGE) && isSecurePair.getLeft(), ptEsRaoResult.isSecure(PhysicalParameter.FLOW, PhysicalParameter.VOLTAGE) && isSecurePair.getRight());
                if (isSecurePair.getRight() && isSecurePair.getLeft()) {
                    businessLogger.info("Voltage monitoring secure for both borders, Final result will contain Voltage monitoring results");
                } else {
                    businessLogger.info("Voltage monitoring unsecure for at least one border");
                }
            }

            DichotomyStepResult frEsDichotomyResult = DichotomyStepResult.fromNetworkValidationResult(frEsRaoResult, isSecurePair.getLeft(), parallelDichotomiesResult.getFrEsResult().getRaoSuccessResponse(), parallelDichotomiesResult.getCounterTradingValues());
            DichotomyStepResult ptEsDichotomyResult = DichotomyStepResult.fromNetworkValidationResult(ptEsRaoResult, isSecurePair.getRight(), parallelDichotomiesResult.getPtEsResult().getRaoSuccessResponse(), parallelDichotomiesResult.getCounterTradingValues());
            // Return the updated parallelDichotomiesResult
            return new ParallelDichotomiesResult(ptEsDichotomyResult, frEsDichotomyResult, parallelDichotomiesResult.getCounterTradingValues());
        } catch (Exception e) {
            throw new CsaInternalException(MDC.get("gridcapaTaskId"), "RAO run failed", e);
        }
    }

    private Pair<RaoResult, RaoResult> updateRaoResultsWithAngleMonitoringForTwoBorders(Network network, Crac frEsCrac, Crac ptEsCrac, ZonalData<Scalable> scalableZonalDataFilteredForSweCountries, RaoResult frEsRaoResult, RaoResult ptEsRaoResult) {
        MonitoringInput frEsAngleMonitoringInput = MonitoringInput.buildWithAngle(network, frEsCrac, frEsRaoResult, scalableZonalDataFilteredForSweCountries).build();
        MonitoringInput ptEsAngleMonitoringInput = MonitoringInput.buildWithAngle(network, ptEsCrac, ptEsRaoResult, scalableZonalDataFilteredForSweCountries).build();
        Pair<MonitoringResult, MonitoringResult> angleMonitoringResultPair = runMonitoringForTwoBorders(frEsAngleMonitoringInput, ptEsAngleMonitoringInput, Runtime.getRuntime().availableProcessors());
        RaoResult frEsRaoResultWithAngleMonitoring = new RaoResultWithAngleMonitoring(frEsRaoResult, angleMonitoringResultPair.getLeft());
        RaoResult ptEsRaoResultWithAngleMonitoring = new RaoResultWithAngleMonitoring(ptEsRaoResult, angleMonitoringResultPair.getRight());
        return Pair.of(frEsRaoResultWithAngleMonitoring, ptEsRaoResultWithAngleMonitoring);
    }

    private Pair<RaoResult, RaoResult> updateRaoResultsWithVoltageMonitoringForTwoBorders(Network network, Crac frEsCrac, Crac ptEsCrac, RaoResult frEsRaoResult, RaoResult ptEsRaoResult) {
        MonitoringInput frEsAngleMonitoringInput = MonitoringInput.buildWithVoltage(network, frEsCrac, frEsRaoResult).build();
        MonitoringInput ptEsAngleMonitoringInput = MonitoringInput.buildWithVoltage(network, ptEsCrac, ptEsRaoResult).build();
        Pair<MonitoringResult, MonitoringResult> voltageMonitoringResultPair = runMonitoringForTwoBorders(frEsAngleMonitoringInput, ptEsAngleMonitoringInput, Runtime.getRuntime().availableProcessors());
        RaoResult frEsRaoResultWithAngleMonitoring = new RaoResultWithVoltageMonitoring(frEsRaoResult, voltageMonitoringResultPair.getLeft());
        RaoResult ptEsRaoResultWithAngleMonitoring = new RaoResultWithVoltageMonitoring(ptEsRaoResult, voltageMonitoringResultPair.getRight());
        return Pair.of(frEsRaoResultWithAngleMonitoring, ptEsRaoResultWithAngleMonitoring);
    }

    private Pair<MonitoringResult, MonitoringResult> runMonitoringForTwoBorders(MonitoringInput frEsMonitoringInput, MonitoringInput ptEsMonitoringInput, int numberOfLoadFlowsInParallel) {
        // Inspired by the class Monitoring
        PhysicalParameter physicalParameter = frEsMonitoringInput.getPhysicalParameter();
        Network inputNetwork = frEsMonitoringInput.getNetwork();
        Crac frEsCrac = frEsMonitoringInput.getCrac();
        Crac ptEsCrac = ptEsMonitoringInput.getCrac();
        RaoResult frEsRaoResult = frEsMonitoringInput.getRaoResult();
        RaoResult ptEsRaoResult = ptEsMonitoringInput.getRaoResult();
        // Initial two empty monitoringResults
        MonitoringResult frEsMonitoringResult = new MonitoringResult(physicalParameter, Collections.emptySet(), Collections.emptyMap(), Cnec.SecurityStatus.SECURE);
        MonitoringResult ptEsMonitoringResult = new MonitoringResult(physicalParameter, Collections.emptySet(), Collections.emptyMap(), Cnec.SecurityStatus.SECURE);

        businessLogger.info("----- {} monitoring for two borders [start]", physicalParameter);
        Set<Cnec> frEsCnecs = frEsCrac.getCnecs(physicalParameter);
        Set<Cnec> ptEsCnecs = ptEsCrac.getCnecs(physicalParameter);
        if (frEsCnecs.isEmpty() && ptEsCnecs.isEmpty()) {
            // Note: is this redundant and incoherent with validateNetworkForTwoBorders?
            businessLogger.warn("No Cnecs of type '{}' defined.", physicalParameter);
            businessLogger.info("----- {} monitoring for two borders [end]", physicalParameter);
            return Pair.of(frEsMonitoringResult, ptEsMonitoringResult);
        }

        // Preventive states
        State frEsPreventiveState = frEsCrac.getPreventiveState();
        State ptEsPreventiveState = ptEsCrac.getPreventiveState();
        if (frEsPreventiveState != null || ptEsPreventiveState != null) {

            applyOptimalRemedialActions(frEsPreventiveState, inputNetwork, frEsRaoResult);
            applyOptimalRemedialActions(ptEsPreventiveState, inputNetwork, ptEsRaoResult);

            Set<Cnec> frEsPreventiveStateCnecs = frEsCrac.getCnecs(physicalParameter, frEsPreventiveState);
            Set<Cnec> ptEsPreventiveStateCnecs = ptEsCrac.getCnecs(physicalParameter, ptEsPreventiveState);

            Pair<MonitoringResult, MonitoringResult> preventiveMonitoringResultsPair = monitorCnecsForTwoBorders(frEsPreventiveState, ptEsPreventiveState, frEsPreventiveStateCnecs, ptEsPreventiveStateCnecs, inputNetwork, frEsMonitoringInput, ptEsMonitoringInput);

            MonitoringResult leftResult = preventiveMonitoringResultsPair.getLeft();
            if (leftResult != null) {
                frEsMonitoringResult.combine(leftResult);
            }
            MonitoringResult rightResult = preventiveMonitoringResultsPair.getRight();
            if (rightResult != null) {
                ptEsMonitoringResult.combine(rightResult);
            }

        }

        // Contingency States
        Set<State> frEsContingencyStates = frEsCrac.getCnecs(physicalParameter).stream().map(Cnec::getState).filter(state -> !state.isPreventive()).collect(Collectors.toSet());
        Set<State> ptEsContingencyStates = ptEsCrac.getCnecs(physicalParameter).stream().map(Cnec::getState).filter(state -> !state.isPreventive()).collect(Collectors.toSet());
        if (!frEsContingencyStates.isEmpty()) {
            try (AbstractNetworkPool networkPool = AbstractNetworkPool.create(inputNetwork, inputNetwork.getVariantManager().getWorkingVariantId(), Math.min(numberOfLoadFlowsInParallel, frEsContingencyStates.size()), true)) {
                List<ForkJoinTask<Object>> frEsTasks = frEsContingencyStates.stream().map(frEsState -> networkPool.submit(() -> {
                    Network networkClone = networkPool.getAvailableNetwork();
                    Contingency contingency = frEsState.getContingency().orElseThrow();
                    Instant instant = frEsState.getInstant();
                    State ptEsState = ptEsCrac.getState(contingency, instant);
                    if (Objects.nonNull(ptEsState)) {
                        // If ptEsState exist in frEs crac, retrieve it from the ptEsCOStates list
                        ptEsContingencyStates.remove(ptEsState);
                    }
                    if (!contingency.isValid(networkClone)) {
                        businessLogger.warn("Unable to apply contingency " + contingency.getId());
                        Pair<MonitoringResult, MonitoringResult> faileMonitonringResults = makeFailedMonitoringResultForStateWithNaNCnecResults(frEsMonitoringInput, ptEsMonitoringInput, physicalParameter, frEsState, ptEsState, "Unable to apply contingency " + contingency.getId());
                        if (Objects.nonNull(faileMonitonringResults.getLeft())) {
                            frEsMonitoringResult.combine(faileMonitonringResults.getLeft());
                        }
                        if (Objects.nonNull(faileMonitonringResults.getRight())) {
                            ptEsMonitoringResult.combine(faileMonitonringResults.getRight());
                        }
                        networkPool.releaseUsedNetwork(networkClone);
                        return null;
                    } else {
                        contingency.toModification().apply(networkClone, (ComputationManager) null);
                        applyOptimalRemedialActionsOnContingencyState(frEsState, networkClone, frEsCrac, frEsRaoResult);
                        Set<Cnec> frEsCurrentStateCnecs = frEsCrac.getCnecs(physicalParameter, frEsState);
                        Set<Cnec> ptEsCurrentStateCnecs = new HashSet<>();

                        // If the state exists in both borders
                        if (ptEsState != null) {
                            applyOptimalRemedialActionsOnContingencyState(ptEsState, networkClone, ptEsCrac, ptEsRaoResult);
                            ptEsCurrentStateCnecs = ptEsCrac.getCnecs(physicalParameter, ptEsState);
                        }
                        Pair<MonitoringResult, MonitoringResult> currentStateMonitoringResults = monitorCnecsForTwoBorders(frEsState, ptEsState, frEsCurrentStateCnecs, ptEsCurrentStateCnecs, networkClone, frEsMonitoringInput, ptEsMonitoringInput);
                        currentStateMonitoringResults.getLeft().printConstraints().forEach(businessLogger::info);
                        frEsMonitoringResult.combine(currentStateMonitoringResults.getLeft());
                        if (ptEsState != null) {
                            currentStateMonitoringResults.getRight().printConstraints().forEach(businessLogger::info);
                            ptEsMonitoringResult.combine(currentStateMonitoringResults.getRight());
                        }
                        networkPool.releaseUsedNetwork(networkClone);
                        return null;
                    }
                })).toList();
                for (ForkJoinTask<Object> task : frEsTasks) {
                    try {
                        task.get();
                    } catch (ExecutionException e) {
                        throw new OpenRaoException(e);
                    }
                }
                networkPool.shutdownAndAwaitTermination(24, TimeUnit.HOURS);

            } catch (Exception e) {
                Thread.currentThread().interrupt();
                frEsMonitoringResult.setStatusToFailure();
                ptEsMonitoringResult.setStatusToFailure();
            }
        }

        if (!ptEsContingencyStates.isEmpty()) {
            try (AbstractNetworkPool networkPool = AbstractNetworkPool.create(inputNetwork, inputNetwork.getVariantManager().getWorkingVariantId(), Math.min(numberOfLoadFlowsInParallel, ptEsContingencyStates.size()), true)) {
                List<ForkJoinTask<Object>> ptEsTasks = ptEsContingencyStates.stream().map(ptEsState -> networkPool.submit(() -> {
                    Network networkClone = networkPool.getAvailableNetwork();
                    Contingency contingency = ptEsState.getContingency().orElseThrow();
                    if (!contingency.isValid(networkClone)) {
                        businessLogger.warn("Unable to apply contingency " + contingency.getId());
                        Pair<MonitoringResult, MonitoringResult> faileMonitonringResults = makeFailedMonitoringResultForStateWithNaNCnecResults(ptEsMonitoringInput, null, physicalParameter, ptEsState, null, "Unable to apply contingency " + contingency.getId());
                        ptEsMonitoringResult.combine(faileMonitonringResults.getLeft());
                        networkPool.releaseUsedNetwork(networkClone);
                        return null;
                    } else {
                        contingency.toModification().apply(networkClone, (ComputationManager) null);
                        applyOptimalRemedialActionsOnContingencyState(ptEsState, networkClone, ptEsCrac, ptEsRaoResult);
                        Set<Cnec> ptEsCurrentStateCnecs = ptEsCrac.getCnecs(physicalParameter, ptEsState);
                        Pair<MonitoringResult, MonitoringResult> currentStateMonitoringResults = monitorCnecsForTwoBorders(ptEsState, null, ptEsCurrentStateCnecs, null, networkClone, ptEsMonitoringInput, null);
                        currentStateMonitoringResults.getLeft().printConstraints().forEach(businessLogger::info);
                        ptEsMonitoringResult.combine(currentStateMonitoringResults.getLeft());
                        networkPool.releaseUsedNetwork(networkClone);
                        return null;
                    }
                })).toList();
                for (ForkJoinTask<Object> task : ptEsTasks) {
                    try {
                        task.get();
                    } catch (ExecutionException e) {
                        throw new OpenRaoException(e);
                    }
                }
                networkPool.shutdownAndAwaitTermination(24, TimeUnit.HOURS);

            } catch (Exception e) {
                Thread.currentThread().interrupt();
                ptEsMonitoringResult.setStatusToFailure();
            }
        }

        businessLogger.info("----- {} monitoring [end]", physicalParameter);
        frEsMonitoringResult.printConstraints().forEach(businessLogger::info);
        ptEsMonitoringResult.printConstraints().forEach(businessLogger::info);
        return Pair.of(frEsMonitoringResult, ptEsMonitoringResult);
    }

    private Pair<MonitoringResult, MonitoringResult> monitorCnecsForTwoBorders(State primaryState, State secondaryState, Set<Cnec> primaryCnecs, Set<Cnec> secondaryCnecs, Network network, MonitoringInput primaryMonitoringInput, MonitoringInput secondaryMonitoringInput) {
        PhysicalParameter physicalParameter = primaryMonitoringInput.getPhysicalParameter();
        Unit unit = parameterToUnitMap.get(physicalParameter);
        Set<CnecResult> primaryCnecResults = new HashSet<>();
        Set<CnecResult> secondaryCnecResults = new HashSet<>();
        businessLogger.info("-- '{}' Monitoring at state '{}' for two borders [start]", physicalParameter, primaryState);
        boolean lfSuccess = computeLoadFlow(network, loadFlowProvider, loadFlowParameters);
        if (!lfSuccess) {
            String failureReason = String.format("Load-flow computation failed at state %s. Skipping this state.", primaryState);
            return makeFailedMonitoringResultForStateWithNaNCnecResults(primaryMonitoringInput, secondaryMonitoringInput, physicalParameter, primaryState, secondaryState, failureReason);
        } else {
            List<AppliedNetworkActionsResult> appliedNetworkActionsResultList = new ArrayList<>();
            processMonitoringCnecs(primaryCnecs, primaryState, primaryMonitoringInput, primaryCnecResults, appliedNetworkActionsResultList, network, unit, physicalParameter, businessLogger);
            processMonitoringCnecs(secondaryCnecs, secondaryState, secondaryMonitoringInput, secondaryCnecResults, appliedNetworkActionsResultList, network, unit, physicalParameter, businessLogger);
            // Re-dispatch the network (for angleCnecs)
            redispatchNetworkActions(network, appliedNetworkActionsResultList, primaryMonitoringInput.getScalableZonalData(), businessLogger);
            if (appliedNetworkActionsResultList.stream().map(AppliedNetworkActionsResult::getAppliedNetworkActions).findAny().isPresent()) {
                lfSuccess = computeLoadFlow(network, loadFlowProvider, loadFlowParameters);
                if (!lfSuccess) {
                    businessLogger.warn("Load-flow computation failed at state {} after applying RAs. Skipping this state.", primaryState);
                    MonitoringResult primaryMonitoringResult = new MonitoringResult(physicalParameter, primaryCnecResults, Map.of(primaryState, Collections.emptySet()), Cnec.SecurityStatus.FAILURE);
                    MonitoringResult secondaryMonitoringResult = new MonitoringResult(physicalParameter, secondaryCnecResults, Map.of(secondaryState, Collections.emptySet()), Cnec.SecurityStatus.FAILURE);
                    return Pair.of(primaryMonitoringResult, secondaryMonitoringResult);
                }

                primaryCnecResults.clear();
                primaryCnecs.forEach(cnec -> {
                    CnecResult primaryCnecResult = new CnecResult(cnec, unit, cnec.computeValue(network, unit), cnec.computeMargin(network, unit), cnec.computeSecurityStatus(network, unit));
                    primaryCnecResults.add(primaryCnecResult);
                });
                secondaryCnecResults.clear();
                secondaryCnecs.forEach(cnec -> {
                    CnecResult secondaryCnecResult = new CnecResult(cnec, unit, cnec.computeValue(network, unit), cnec.computeMargin(network, unit), cnec.computeSecurityStatus(network, unit));
                    secondaryCnecResults.add(secondaryCnecResult);
                });
            }

            Cnec.SecurityStatus primaryMonitoringResultStatus = Cnec.SecurityStatus.SECURE;
            if (primaryCnecResults.stream().anyMatch(cnecResult -> cnecResult.getMargin() < 0)) {
                primaryMonitoringResultStatus = MonitoringResult.combineStatuses(primaryCnecResults.stream().map(CnecResult::getCnecSecurityStatus).toArray(Cnec.SecurityStatus[]::new));
            }
            Cnec.SecurityStatus secondaryMonitoringResultStatus = Cnec.SecurityStatus.SECURE;
            if (secondaryCnecResults.stream().anyMatch(cnecResult -> cnecResult.getMargin() < 0)) {
                secondaryMonitoringResultStatus = MonitoringResult.combineStatuses(secondaryCnecResults.stream().map(CnecResult::getCnecSecurityStatus).toArray(Cnec.SecurityStatus[]::new));
            }

            businessLogger.info("-- '{}' Monitoring at state '{}' [end]", physicalParameter, primaryState);
            // Fixme: null point exception error here
            Map<State, Set<RemedialAction>> primaryAppliedRas = primaryState != null ? Map.of(primaryState, appliedNetworkActionsResultList.stream().flatMap(r -> r.getAppliedNetworkActions().stream()).collect(Collectors.toSet())) : Collections.emptyMap();
            Map<State, Set<RemedialAction>> secondaryAppliedRas = secondaryState != null ? Map.of(secondaryState, appliedNetworkActionsResultList.stream().flatMap(r -> r.getAppliedNetworkActions().stream()).collect(Collectors.toSet())) : Collections.emptyMap();

            return Pair.of(new MonitoringResult(physicalParameter, primaryCnecResults, primaryAppliedRas, primaryMonitoringResultStatus), new MonitoringResult(physicalParameter, secondaryCnecResults, secondaryAppliedRas, secondaryMonitoringResultStatus));
        }
    }

    private Pair<MonitoringResult, MonitoringResult> makeFailedMonitoringResultForStateWithNaNCnecResults(MonitoringInput primaryMonitoringInput, MonitoringInput secondaryMonitoringInput, PhysicalParameter physicalParameter, State primaryState, State secondaryState, String failureReason) {
        Set<CnecResult> frEsCnecResults = new HashSet();
        Set<CnecResult> ptEsCnecResults = new HashSet();
        CnecValue cnecValue = physicalParameter.equals(PhysicalParameter.ANGLE) ? new AngleCnecValue(Double.NaN) : new VoltageCnecValue(Double.NaN, Double.NaN);
        MonitoringResult ptEsMonitoringResult = null;
        MonitoringResult frEsMonitoringResult = null;
        if (primaryState != null) {
            primaryMonitoringInput.getCrac().getCnecs(primaryState).forEach(cnec -> frEsCnecResults.add(new CnecResult(cnec, parameterToUnitMap.get(physicalParameter), cnecValue, Double.NaN, Cnec.SecurityStatus.FAILURE)));
            frEsMonitoringResult = new MonitoringResult(physicalParameter, frEsCnecResults, Map.of(primaryState, Collections.emptySet()), Cnec.SecurityStatus.FAILURE);
        }
        if (secondaryState != null) {
            // TODO: check the condition here
            secondaryMonitoringInput.getCrac().getCnecs(secondaryState).forEach(cnec -> ptEsCnecResults.add(new CnecResult(cnec, parameterToUnitMap.get(physicalParameter), cnecValue, Double.NaN, Cnec.SecurityStatus.FAILURE)));
            ptEsMonitoringResult = new MonitoringResult(physicalParameter, ptEsCnecResults, Map.of(secondaryState, Collections.emptySet()), Cnec.SecurityStatus.FAILURE);

        }
        businessLogger.warn(failureReason);
        return Pair.of(frEsMonitoringResult, ptEsMonitoringResult);
    }

    private void processMonitoringCnecs(Set<Cnec> cnecs, State state, MonitoringInput monitoringInput, Set<CnecResult> cnecResults, List<AppliedNetworkActionsResult> appliedNetworkActionsResultList, Network network, Unit unit, PhysicalParameter physicalParameter, Logger businessLogger) {
        cnecs.forEach(cnec -> {
            if (cnec.computeMargin(network, unit) < 0) {
                Set<NetworkAction> availableNetworkActions = getNetworkActionsAssociatedToCnec(state, monitoringInput.getCrac(), cnec, physicalParameter);

                if (!availableNetworkActions.isEmpty()) {
                    AppliedNetworkActionsResult result = applyNetworkActions(network, availableNetworkActions, cnec.getId(), monitoringInput);
                    if (!result.getAppliedNetworkActions().isEmpty()) {
                        appliedNetworkActionsResultList.add(result);
                    }
                }
            }

            CnecResult cnecResult = new CnecResult(cnec, unit, cnec.computeValue(network, unit), cnec.computeMargin(network, unit), cnec.computeSecurityStatus(network, unit));
            cnecResults.add(cnecResult);
        });
    }
}
