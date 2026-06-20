package com.farao_community.farao.swe_csa.app.security_evaluator;

import com.powsybl.action.*;
import com.powsybl.contingency.Contingency;
import com.powsybl.glsk.commons.CountryEICode;
import com.powsybl.glsk.commons.ZonalData;
import com.powsybl.iidm.modification.scalable.Scalable;
import com.powsybl.iidm.network.*;
import com.powsybl.loadflow.LoadFlow;
import com.powsybl.loadflow.LoadFlowParameters;
import com.powsybl.loadflow.LoadFlowResult;
import com.powsybl.openrao.commons.OpenRaoException;
import com.powsybl.openrao.commons.PhysicalParameter;
import com.powsybl.openrao.commons.Unit;
import com.powsybl.openrao.commons.logs.OpenRaoLoggerProvider;
import com.powsybl.openrao.data.crac.api.Crac;
import com.powsybl.openrao.data.crac.api.RemedialAction;
import com.powsybl.openrao.data.crac.api.State;
import com.powsybl.openrao.data.crac.api.cnec.Cnec;
import com.powsybl.openrao.data.crac.api.networkaction.NetworkAction;
import com.powsybl.openrao.data.crac.api.usagerule.OnConstraint;
import com.powsybl.openrao.data.raoresult.api.RaoResult;
import com.powsybl.openrao.monitoring.MonitoringInput;
import com.powsybl.openrao.monitoring.redispatching.RedispatchAction;
import org.slf4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

import static com.powsybl.openrao.commons.logs.OpenRaoLoggerProvider.BUSINESS_WARNS;

public final class ResultValidatorHelper {
    private ResultValidatorHelper() {
    }

    public static void applyOptimalRemedialActionsOnContingencyState(State state, Network network, Crac crac, RaoResult raoResult) {
        if (state.getInstant().isCurative()) {
            Optional<Contingency> contingency = state.getContingency();
            crac.getStates((Contingency) contingency.orElseThrow()).forEach(contingencyState -> applyOptimalRemedialActions(contingencyState, network, raoResult));
        } else {
            applyOptimalRemedialActions(state, network, raoResult);
        }
    }

    public static void applyOptimalRemedialActions(State state, Network network, RaoResult raoResult) {
        if (state == null) {
            return;
        }
        raoResult.getActivatedNetworkActionsDuringState(state).forEach(na -> na.apply(network));
        raoResult.getActivatedRangeActionsDuringState(state).forEach(ra -> ra.apply(network, raoResult.getOptimizedSetPointOnState(state, ra)));
    }

    public static AppliedNetworkActionsResult applyNetworkActions(Network network, Set<NetworkAction> availableNetworkActions, String cnecId, MonitoringInput monitoringInput) {
        AppliedNetworkActionsResult appliedNetworkActionsResult;
        Set<RemedialAction> appliedNetworkActions = new TreeSet<>(Comparator.comparing(RemedialAction::getId));
        if (monitoringInput.getPhysicalParameter().equals(PhysicalParameter.VOLTAGE)) {
            for (NetworkAction na : availableNetworkActions) {
                na.apply(network);
                appliedNetworkActions.add(na);
            }

            appliedNetworkActionsResult = new AppliedNetworkActionsResult.AppliedNetworkActionsResultBuilder().withAppliedNetworkActions(appliedNetworkActions)
                    .withNetworkElementsToBeExcluded(new HashSet<>()).withPowerToBeRedispatched(new EnumMap<>(Country.class)).build();
        } else {
            boolean networkActionOk = false;
            EnumMap<Country, Double> powerToBeRedispatched = new EnumMap(Country.class);
            Set<String> networkElementsToBeExcluded = new HashSet();

            for (NetworkAction na : availableNetworkActions) {
                EnumMap<Country, Double> tempPowerToBeRedispatched = new EnumMap(powerToBeRedispatched);

                for (Action ea : na.getElementaryActions()) {
                    networkActionOk = checkElementaryActionAndStoreInjection(ea, network, cnecId, na.getId(), networkElementsToBeExcluded, tempPowerToBeRedispatched, monitoringInput.getScalableZonalData());
                    if (!networkActionOk) {
                        break;
                    }
                }

                if (networkActionOk) {
                    na.apply(network);
                    appliedNetworkActions.add(na);
                    powerToBeRedispatched.putAll(tempPowerToBeRedispatched);
                }
            }

            appliedNetworkActionsResult = (new AppliedNetworkActionsResult.AppliedNetworkActionsResultBuilder()).withAppliedNetworkActions(appliedNetworkActions).withNetworkElementsToBeExcluded(networkElementsToBeExcluded).withPowerToBeRedispatched(powerToBeRedispatched).build();
        }

        OpenRaoLoggerProvider.BUSINESS_LOGS.info("Applied the following remedial action(s) in order to reduce constraints on CNEC \"{}\": {}", cnecId, appliedNetworkActions.stream().map(com.powsybl.openrao.data.crac.api.Identifiable::getId).collect(Collectors.joining(", ")));
        return appliedNetworkActionsResult;
    }

    /**
     * 1) Checks a network action's elementary action : it must be a Generator or a Load injection setpoint,
     * with a defined country.
     * 2) Stores applied injections on network
     * Returns false if network action must be filtered.
     */
    public static boolean checkElementaryActionAndStoreInjection(Action ea, Network network, String angleCnecId, String naId, Set<String> networkElementsToBeExcluded, Map<Country, Double> powerToBeRedispatched, ZonalData<Scalable> scalableZonalData) {
        if (!(ea instanceof LoadAction) && !(ea instanceof GeneratorAction)) {
            BUSINESS_WARNS.warn("Remedial action {} of AngleCnec {} is ignored : it has an elementary action that's not an injection setpoint.", naId, angleCnecId);
            return false;
        }
        Identifiable<?> ne = getInjectionSetpointIdentifiable(ea, network);

        if (ne == null) {
            BUSINESS_WARNS.warn("Remedial action {} of AngleCnec {} is ignored : it has no elementary actions.", naId, angleCnecId);
            return false;
        }

        Optional<Substation> substation = ((Injection<?>) ne).getTerminal().getVoltageLevel().getSubstation();
        if (substation.isEmpty()) {
            BUSINESS_WARNS.warn("Remedial action {} of AngleCnec {} is ignored : it has an elementary action that doesn't have a substation.", naId, angleCnecId);
            return false;
        } else {
            Optional<Country> country = substation.get().getCountry();
            if (country.isEmpty()) {
                BUSINESS_WARNS.warn("Remedial action {} of AngleCnec {} is ignored : it has an elementary action that doesn't have a country.", naId, angleCnecId);
                return false;
            } else {
                checkGlsks(country.get(), naId, angleCnecId, scalableZonalData);
                if (ne.getType().equals(IdentifiableType.GENERATOR)) {
                    powerToBeRedispatched.merge(country.get(), ((Generator) ne).getTargetP() - ((GeneratorAction) ea).getActivePowerValue().getAsDouble(), Double::sum);
                } else if (ne.getType().equals(IdentifiableType.LOAD)) {
                    powerToBeRedispatched.merge(country.get(), -((Load) ne).getP0() + ((LoadAction) ea).getActivePowerValue().getAsDouble(), Double::sum);
                } else {
                    BUSINESS_WARNS.warn("Remedial action {} of AngleCnec {} is ignored : it has an injection setpoint that's neither a generator nor a load.", naId, angleCnecId);
                    return false;
                }
                networkElementsToBeExcluded.add(ne.getId());
            }
        }
        return true;
    }

    public static void checkGlsks(Country country, String naId, String angleCnecId, ZonalData<Scalable> scalableZonalData) {
        Set<Country> glskCountries = new TreeSet(Comparator.comparing(Country::getName));
        if (Objects.isNull(scalableZonalData)) {
            String error = "ScalableZonalData undefined (no GLSK given)";
            OpenRaoLoggerProvider.BUSINESS_LOGS.error(error, new Object[0]);
            throw new OpenRaoException(error);
        } else {
            for (String zone : scalableZonalData.getDataPerZone().keySet()) {
                glskCountries.add((new CountryEICode(zone)).getCountry());
            }

            if (!glskCountries.contains(country)) {
                throw new OpenRaoException(String.format("INFEASIBLE Angle Monitoring : Glsks were not defined for country %s. Remedial action %s of AngleCnec %s is ignored.", country.getName(), naId, angleCnecId));
            }
        }
    }

    public static Identifiable<?> getInjectionSetpointIdentifiable(Action ea, Network network) {
        if (ea instanceof GeneratorAction generatorAction) {
            return network.getIdentifiable(generatorAction.getGeneratorId());
        } else if (ea instanceof LoadAction loadAction) {
            return network.getIdentifiable(loadAction.getLoadId());
        } else if (ea instanceof DanglingLineAction danglingLineAction) {
            return network.getIdentifiable(danglingLineAction.getDanglingLineId());
        } else if (ea instanceof ShuntCompensatorPositionAction shuntCompensatorPositionAction) {
            return network.getIdentifiable(shuntCompensatorPositionAction.getShuntCompensatorId());
        } else {
            return null;
        }
    }

    public static boolean computeLoadFlow(Network network, String loadFlowProvider, LoadFlowParameters loadFlowParameters) {
        OpenRaoLoggerProvider.TECHNICAL_LOGS.info("Load-flow computation [start]");
        LoadFlowResult loadFlowResult = LoadFlow.find(loadFlowProvider).run(network, loadFlowParameters);
        if (loadFlowResult.isFailed()) {
            OpenRaoLoggerProvider.BUSINESS_WARNS.warn("LoadFlow error.");
        }

        OpenRaoLoggerProvider.TECHNICAL_LOGS.info("Load-flow computation [end]");
        return loadFlowResult.isFullyConverged();
    }

    public static Set<NetworkAction> getNetworkActionsAssociatedToCnec(State state, Crac crac, Cnec cnec, PhysicalParameter physicalParameter) {
        Set<RemedialAction<?>> availableRemedialActions =
                crac.getRemedialActions().stream()
                        .filter(remedialAction ->
                                remedialAction.getUsageRules().stream().filter(OnConstraint.class::isInstance)
                                        .map(OnConstraint.class::cast)
                                        .anyMatch(onConstraint -> onConstraint.getCnec().equals(cnec)))
                        .collect(Collectors.toSet());
        if (availableRemedialActions.isEmpty()) {
            BUSINESS_WARNS.warn("{} Cnec {} in state {} has no associated RA. {} constraint cannot be secured.", physicalParameter, cnec.getId(), state.getId(), physicalParameter);
            return Collections.emptySet();
        } else if (state.isPreventive()) {
            BUSINESS_WARNS.warn("{} Cnec {} is constrained in preventive state, it cannot be secured.", physicalParameter, cnec.getId());
            return Collections.emptySet();
        }
        // Convert remedial actions to network actions
        return availableRemedialActions.stream().filter(remedialAction -> {
            if (remedialAction instanceof NetworkAction) {
                return true;
            } else {
                BUSINESS_WARNS.warn("Remedial action {} of Cnec {} in state {} is ignored : it's not a network action.", remedialAction.getId(), cnec.getId(), state.getId());
                return false;
            }
        }).map(NetworkAction.class::cast).collect(Collectors.toSet());
    }

    public static boolean checkMargins(Crac crac, State state, PhysicalParameter parameter, Network network, Unit unit) {
        if (state == null) {
            return true;
        }

        return crac.getCnecs(parameter, state).stream()
                .noneMatch(cnec -> cnec.computeMargin(network, unit) < 0.0);
    }

    public static void redispatchNetworkActions(Network network, List<AppliedNetworkActionsResult> appliedNetworkActionsResults, ZonalData<Scalable> scalableZonalData, Logger businessLogger) {
        appliedNetworkActionsResults.forEach(appliedNetworkActionsResult -> appliedNetworkActionsResult.getPowerToBeRedispatched().forEach((key, value) -> {
            businessLogger.info("Redispatching {} MW in {} [start]", value, key);
            List<Scalable> countryScalables = scalableZonalData.getDataPerZone().entrySet().stream().filter(entry -> key.equals((new CountryEICode((String) entry.getKey())).getCountry())).map(Map.Entry::getValue).toList();
            if (countryScalables.size() > 1) {
                throw new OpenRaoException(String.format("> 1 (%s) glskPoints defined for country %s", countryScalables.size(), key.getName()));
            } else {
                (new RedispatchAction(value, appliedNetworkActionsResult.getNetworkElementsToBeExcluded(), (Scalable) countryScalables.get(0))).apply(network);
                businessLogger.info("Redispatching {} MW in {} [end]", value, key);
            }
        }));
    }

}
