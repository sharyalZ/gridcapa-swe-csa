package com.farao_community.farao.swe_csa.app.security_evaluator;

import com.powsybl.computation.ComputationManager;
import com.powsybl.contingency.Contingency;
import com.powsybl.iidm.network.Network;
import com.powsybl.loadflow.LoadFlowParameters;
import com.powsybl.openrao.commons.PhysicalParameter;
import com.powsybl.openrao.commons.Unit;
import com.powsybl.openrao.data.crac.api.State;
import com.powsybl.openrao.data.crac.api.cnec.Cnec;
import com.powsybl.openrao.util.AbstractNetworkPool;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.farao_community.farao.swe_csa.app.security_evaluator.ResultValidatorHelper.*;

public class TwoBordersFlowCnecSecurityChecker {
    private final Network network;
    private final List<BorderContext> borders;
    private final Integer numberOfLoadFlowsInParallel;
    private final Logger businessLogger;
    private final String loadFlowProvider;
    private final LoadFlowParameters loadFlowParameters;

    public TwoBordersFlowCnecSecurityChecker(Network network, List<BorderContext> borders, int parallelism, Logger logger, String loadFlowProvider, LoadFlowParameters loadFlowParams
    ) {
        this.network = network;
        this.borders = borders;
        this.numberOfLoadFlowsInParallel = parallelism;
        this.businessLogger = logger;
        this.loadFlowProvider = loadFlowProvider;
        this.loadFlowParameters = loadFlowParams;
    }

    /**
     * Check if all the flowCnecs is secure when considering the application of
     * RAs from 2 borders
     *
     * @return True if all flowCnecs are secure and False if there is at least
     * one flowCnec is not secure
     */
    public Map<Border, Boolean>  check() {
        businessLogger.info("----- Monitoring flow CNECs for borders [start]");
        PhysicalParameter parameter = PhysicalParameter.FLOW;
        Unit unit = Unit.AMPERE;

        // all borders start as secure
        Map<Border, Boolean> securityResultPerBorder = new EnumMap<>(Border.class);
        borders.forEach(ctx -> securityResultPerBorder.put(ctx.border(), true));

        // if no flow cnecs then return true
        boolean noFlowCnecs = borders.stream().allMatch(ctx -> ctx.crac().getCnecs(parameter).isEmpty());
        if (noFlowCnecs) {
            businessLogger.warn("No Flow CNECs defined for any border.");
            businessLogger.info("----- Monitoring flow CNECs for borders [end]");
            return securityResultPerBorder;
        }

        // Preventive evaluation
        borders.forEach(ctx -> applyOptimalRemedialActions(ctx.crac().getPreventiveState(), network, ctx.raoResult()));
        if (!computeLoadFlow(network, loadFlowProvider, loadFlowParameters)) {
            businessLogger.warn("Load-flow computation failed at preventive state during security evaluation.");
            borders.forEach(ctx -> securityResultPerBorder.put(ctx.border(), false));
            return securityResultPerBorder;
        }
        for (BorderContext ctx : borders) {
            State preventiveState = ctx.crac().getPreventiveState();
            if (preventiveState != null && !checkMargins(ctx.crac(), preventiveState, parameter, network, unit)) {
                securityResultPerBorder.put(ctx.border(), false);
            }
        }

        // If all borders failed already, stop
        if (securityResultPerBorder.values().stream().noneMatch(v -> v)) {
            return securityResultPerBorder;
        }

        // Contingency states evaluation
        Map<State, EnumSet<Border>> contingencyStates = BorderStateMapper.mapContingencyStates(borders, parameter);
        if (contingencyStates.isEmpty()) {
            businessLogger.info("No contingency states present for network security evaluation.");
            businessLogger.info("----- Monitoring flow CNECs for borders [end]");
            return securityResultPerBorder;
        }
        try (AbstractNetworkPool networkPool = AbstractNetworkPool.create(network, network.getVariantManager().getWorkingVariantId(), Math.min(numberOfLoadFlowsInParallel, contingencyStates.size()), true)) {
            List<ForkJoinTask<Map<Border, Boolean>>> tasks = contingencyStates.entrySet().stream()
                    .map(entry -> submitParallelEvaluation(networkPool, entry.getKey(), entry.getValue(), parameter, unit))
                    .toList();
            for (ForkJoinTask<Map<Border, Boolean>> task : tasks) {
                Map<Border, Boolean> stateResult = task.get();
                for (Map.Entry<Border, Boolean> entry : stateResult.entrySet()) {
                    if (!entry.getValue()) {
                        securityResultPerBorder.put(entry.getKey(), false);
                    }
                }
            }
            networkPool.shutdownAndAwaitTermination(24, TimeUnit.HOURS);
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            borders.forEach(ctx -> securityResultPerBorder.put(ctx.border(), false));
            return securityResultPerBorder;
        }
        businessLogger.info("----- Monitoring flow CNECs for borders [end]");
        return securityResultPerBorder;
    }

    private ForkJoinTask<Map<Border, Boolean>> submitParallelEvaluation(AbstractNetworkPool networkPool, State state, EnumSet<Border> impactedBorders, PhysicalParameter parameter, Unit unit) {
        return networkPool.submit(() -> evaluateState(networkPool, state, impactedBorders, parameter, unit));
    }

    private Map<Border, Boolean> evaluateState(AbstractNetworkPool networkPool, State state, EnumSet<Border> impactedBorders, PhysicalParameter parameter, Unit unit) throws InterruptedException {
        Network networkClone = networkPool.getAvailableNetwork();
        Map<Border, Boolean> result = new EnumMap<>(Border.class);
        try {
            // Apply contingency for the given state
            if (!ResultValidatorHelper.applyContingency(state, networkClone)) {
                impactedBorders.forEach(side -> result.put(side, false));
                return result;
            }
            // Apply remedial actions for all impacted borders
            for (Border border : impactedBorders) {
                BorderContext ctx = BorderContext.find(borders, border);
                State borderState = ctx.crac().getState(state.getContingency().orElseThrow(), state.getInstant());
                if (borderState != null) {
                    applyOptimalRemedialActionsOnContingencyState(borderState, networkClone, ctx.crac(), ctx.raoResult());
                }
            }
            // Compute load flow
            if (!computeLoadFlow(network, loadFlowProvider, loadFlowParameters)) {
                businessLogger.warn("Load-flow computation failed at {} state", state);
                impactedBorders.forEach(side -> result.put(side, false));
                return result;
            }
            // Evaluate security for each impacted border
            for (Border border : impactedBorders) {
                BorderContext ctx = BorderContext.find(borders, border);
                State borderState = ctx.crac().getState(state.getContingency().orElseThrow(), state.getInstant());
                if (borderState == null) {
                    result.put(border, false);
                    continue;
                }
                boolean secure =  checkMargins(ctx.crac(), borderState, parameter, networkClone, unit);
                result.put(border, secure);
            }
            return result;
        } finally {
            networkPool.releaseUsedNetwork(networkClone);
        }
    }
}

