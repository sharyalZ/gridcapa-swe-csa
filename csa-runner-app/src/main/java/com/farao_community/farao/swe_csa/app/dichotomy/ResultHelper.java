package com.farao_community.farao.swe_csa.app.dichotomy;

import com.farao_community.farao.swe_csa.api.exception.CsaInvalidDataException;
import com.farao_community.farao.swe_csa.api.results.CounterTradeRangeActionResult;
import com.farao_community.farao.swe_csa.api.results.CounterTradingResult;
import com.farao_community.farao.swe_csa.app.rao_result.RaoResultWithCounterTradeRangeActions;
import com.powsybl.glsk.commons.ZonalData;
import com.powsybl.iidm.modification.scalable.Scalable;
import com.powsybl.iidm.network.Country;
import com.powsybl.iidm.network.Network;
import com.powsybl.openrao.data.crac.api.Crac;
import com.powsybl.openrao.data.crac.api.Identifiable;
import com.powsybl.openrao.data.crac.api.rangeaction.CounterTradeRangeAction;
import com.powsybl.openrao.data.raoresult.api.RaoResult;
import com.powsybl.openrao.monitoring.Monitoring;
import com.powsybl.openrao.monitoring.MonitoringInput;
import com.powsybl.openrao.raoapi.parameters.RaoParameters;
import com.powsybl.openrao.raoapi.parameters.extensions.LoadFlowAndSensitivityParameters;
import org.slf4j.MDC;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ResultHelper {

    public RaoResult updateRaoResultWithAngleMonitoring(Network network, Crac crac, ZonalData<Scalable> scalableZonalDataFilteredForSweCountries, RaoResult raoResult, RaoParameters raoParameters) {
        MonitoringInput angleMonitoringInput = MonitoringInput.buildWithAngle(network, crac, raoResult, scalableZonalDataFilteredForSweCountries).build();
        return Monitoring.runAngleAndUpdateRaoResult(LoadFlowAndSensitivityParameters.getLoadFlowProvider(raoParameters), LoadFlowAndSensitivityParameters.getSensitivityWithLoadFlowParameters(raoParameters).getLoadFlowParameters(), Runtime.getRuntime().availableProcessors(), angleMonitoringInput);
    }

    public RaoResult updateRaoResultWithVoltageMonitoring(Network network, Crac crac, RaoResult raoResult, RaoParameters raoParameters) {
        MonitoringInput input = MonitoringInput.buildWithVoltage(network, crac, raoResult).build();
        return Monitoring.runVoltageAndUpdateRaoResult(
            LoadFlowAndSensitivityParameters.getLoadFlowProvider(raoParameters),
            LoadFlowAndSensitivityParameters.getSensitivityWithLoadFlowParameters(raoParameters).getLoadFlowParameters(),
            Runtime.getRuntime().availableProcessors(),
            input
        );
    }

    public RaoResultWithCounterTradeRangeActions updateRaoResultWithCounterTradingRangeActions(
            Crac crac,
            Index index,
            RaoResult raoResult,
            String border) {

        Map<CounterTradeRangeAction, CounterTradeRangeActionResult> resultMap = new HashMap<>();
        List<String> flowCnecs = SweCsaRaoValidator.getBorderFlowCnecs(crac, border)
                .stream()
                .map(Identifiable::getId)
                .toList();
        Set<CounterTradeRangeAction> ctActions = crac.getCounterTradeRangeActions();

        switch (border) {
            case "PT-ES" -> {
                double value = Math.abs(index.getPtEsLowestSecureStep().getLeft());
                CounterTradeRangeAction ctRaPtes = findCounterTradingAction(ctActions, Country.PT, Country.ES);
                resultMap.put(ctRaPtes, new CounterTradeRangeActionResult(ctRaPtes.getId(), value, flowCnecs));

                CounterTradeRangeAction ctRaEsPt = findCounterTradingAction(ctActions, Country.ES, Country.PT);
                resultMap.put(ctRaEsPt, new CounterTradeRangeActionResult(ctRaEsPt.getId(), -value, flowCnecs));
            }
            case "FR-ES" -> {
                double value = Math.abs(index.getFrEsLowestSecureStep().getLeft());
                CounterTradeRangeAction ctRaFrEs = findCounterTradingAction(ctActions, Country.FR, Country.ES);
                resultMap.put(ctRaFrEs, new CounterTradeRangeActionResult(ctRaFrEs.getId(), value, flowCnecs));
                CounterTradeRangeAction ctRaEsFr = findCounterTradingAction(ctActions, Country.ES, Country.FR);
                resultMap.put(ctRaEsFr, new CounterTradeRangeActionResult(ctRaEsFr.getId(), -value, flowCnecs));
            }
            default -> throw new IllegalArgumentException("Unsupported border: " + border);
        }

        return new RaoResultWithCounterTradeRangeActions(raoResult, new CounterTradingResult(resultMap));
    }

    private static CounterTradeRangeAction findCounterTradingAction(Set<CounterTradeRangeAction> ctActions, Country exportingCountry, Country importingCountry) {
        return ctActions.stream()
                .filter(action -> action.getExportingCountry() == exportingCountry && action.getImportingCountry() == importingCountry)
                .findFirst()
                .orElseThrow(() -> new CsaInvalidDataException(MDC.get("gridcapaTaskId"), String.format("No CounterTradeRangeAction found for '%s' → '%s'", exportingCountry.getName(), importingCountry.getName())));
    }
}
