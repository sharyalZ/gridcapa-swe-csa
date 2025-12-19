package com.farao_community.farao.swe_csa.app.dichotomy;

/*
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import com.farao_community.farao.dichotomy.api.exceptions.GlskLimitationException;
import com.farao_community.farao.dichotomy.api.exceptions.ShiftingException;
import com.farao_community.farao.gridcapa_swe_commons.shift.CountryBalanceComputation;
import com.farao_community.farao.gridcapa_swe_commons.shift.GeneratorLimitsHandler;
import com.farao_community.farao.gridcapa_swe_commons.shift.ScalableGeneratorConnector;
import com.farao_community.farao.swe_csa.app.shift.ShiftDispatcher;
import com.powsybl.computation.local.LocalComputationManager;
import com.powsybl.glsk.commons.ZonalData;
import com.powsybl.iidm.modification.scalable.Scalable;
import com.powsybl.iidm.modification.scalable.ScalingParameters;
import com.powsybl.iidm.network.Country;
import com.powsybl.iidm.network.Network;
import com.powsybl.loadflow.LoadFlow;
import com.powsybl.loadflow.LoadFlowResult;
import com.powsybl.openrao.commons.EICode;
import com.powsybl.openrao.raoapi.parameters.RaoParameters;
import com.powsybl.openrao.raoapi.parameters.extensions.LoadFlowAndSensitivityParameters;

import java.util.*;

import static com.powsybl.openrao.commons.logs.OpenRaoLoggerProvider.BUSINESS_LOGS;
import static com.powsybl.openrao.commons.logs.OpenRaoLoggerProvider.BUSINESS_WARNS;

/**
 * @author Jean-Pierre Arnould {@literal <jean-pierre.arnould at rte-france.com>}
 */

public final class SweCsaNetworkShifter {
    private static final double DEFAULT_SHIFT_TOLERANCE = 1; // in MW
    private static final int DEFAULT_MAX_SHIFT_ITERATIONS = 10;
    public static final String EI_CODE_FR = new EICode(Country.FR).getAreaCode();
    public static final String EI_CODE_PT = new EICode(Country.PT).getAreaCode();
    public static final String EI_CODE_ES = new EICode(Country.ES).getAreaCode();
    private final ZonalData<Scalable> zonalScalable;
    private final double shiftTolerance;
    private final int maxShiftIterations;
    private final double esFrInitialExchange;
    private final double esPtInitialExchange;

    private final ShiftDispatcher shiftDispatcher;

    public SweCsaNetworkShifter(ZonalData<Scalable> zonalScalable, double esFrInitialExchange, double esPtInitialExchange, ShiftDispatcher shiftDispatcher) {
        this(zonalScalable, esFrInitialExchange, esPtInitialExchange, shiftDispatcher, DEFAULT_SHIFT_TOLERANCE, DEFAULT_MAX_SHIFT_ITERATIONS);
    }

    private SweCsaNetworkShifter(ZonalData<Scalable> zonalScalable, double esFrInitialExchange, double esPtInitialExchange, ShiftDispatcher shiftDispatcher, double shiftEpsilon, int maxShiftIterations) {
        this.zonalScalable = zonalScalable;
        this.shiftDispatcher = shiftDispatcher;
        this.shiftTolerance = shiftEpsilon;
        this.maxShiftIterations = maxShiftIterations;
        this.esFrInitialExchange = esFrInitialExchange;
        this.esPtInitialExchange = esPtInitialExchange;
    }

    public void applyCounterTrading(CounterTradingValues counterTradingValues, Network network, RaoParameters raoParameters) throws GlskLimitationException, ShiftingException {
        BUSINESS_LOGS.info("Starting shift on network {}", network.getVariantManager().getWorkingVariantId());

        // Compute the initial estimation of country net position scaling values, given the counter-trading values
        Map<String, Double> scalingValueEstimationPerCountry = shiftDispatcher.dispatch(counterTradingValues);

        // Compute target exchange values, given the counter-trading values
        Map<String, Double> targetExchanges = Map.of(
            DichotomyDirection.ES_FR.toString(), esFrInitialExchange - Math.signum(esFrInitialExchange) * Math.abs(counterTradingValues.frEsCt()),
            DichotomyDirection.ES_PT.toString(), esPtInitialExchange - Math.signum(esPtInitialExchange) * Math.abs(counterTradingValues.ptEsCt())
        );
        BUSINESS_LOGS.info("Target exchanges: PT->ES: {}, FR-ES: {}", -targetExchanges.get(DichotomyDirection.ES_PT.toString()), -targetExchanges.get(DichotomyDirection.ES_FR.toString()));

        shiftExchangeValues(network, targetExchanges, scalingValueEstimationPerCountry, raoParameters);
    }

    void shiftExchangeValues(Network network, Map<String, Double> targetExchanges, Map<String, Double> scalingValueEstimationPerCountry, RaoParameters raoParameters) throws ShiftingException, GlskLimitationException {
        ScalableGeneratorConnector scalableGeneratorConnector = new ScalableGeneratorConnector(zonalScalable);
        GeneratorLimitsHandler generatorLimitsHandler = new GeneratorLimitsHandler(zonalScalable);
        Map<String, Double> scalingValuePerCountry = new HashMap<>(scalingValueEstimationPerCountry);
        try {
            int iterationCounter = 0;
            boolean shiftSucceed = false;
            Map<String, Double> mismatchPerBorder;

            String initialVariantId = network.getVariantManager().getWorkingVariantId();
            String processedVariantId = initialVariantId + " PROCESSED COPY";
            String workingVariantCopyId = initialVariantId + " WORKING COPY";
            preProcessNetwork(network, scalableGeneratorConnector, generatorLimitsHandler, initialVariantId, processedVariantId, workingVariantCopyId);
            do {
                // Step 1: Perform the scaling given the current estimation
                shiftNetPositions(network, scalingValuePerCountry);

                // Step 2: Compute exchanges mismatch
                mismatchPerBorder = computeExchangeValuesMismatch(network, workingVariantCopyId, targetExchanges, raoParameters);

                // Step 3: Checks balance adjustment results
                if (mismatchPerBorder.values().stream().allMatch(mismatch -> Math.abs(mismatch) < shiftTolerance)) {
                    BUSINESS_LOGS.info("Tolerance of {} reached, shift succeeded after {} iteration(s)", shiftTolerance, ++iterationCounter);
                    network.getVariantManager().cloneVariant(workingVariantCopyId, initialVariantId, true);
                    shiftSucceed = true;
                } else {
                    // Reset current variant with initial state for each iteration (keeping pre-processing)
                    network.getVariantManager().cloneVariant(processedVariantId, workingVariantCopyId, true);
                    updateScalingValuesWithMismatch(scalingValuePerCountry, mismatchPerBorder);
                    ++iterationCounter;
                }
            } while (iterationCounter < maxShiftIterations && !shiftSucceed);

            // Step 4 : check after iteration max and out of tolerance
            if (!shiftSucceed) {
                String message = String.format("Balancing adjustment out of tolerances: mismatch on ES-PT = %.2f , mismatch on ES-FR =  %.2f", mismatchPerBorder.get(DichotomyDirection.ES_PT.toString()), mismatchPerBorder.get(DichotomyDirection.ES_FR.toString()));
                BUSINESS_LOGS.error(message);
                throw new ShiftingException(message);
            }

            // Step 5: Reset current variant with initial state
            network.getVariantManager().setWorkingVariant(initialVariantId);
            network.getVariantManager().removeVariant(processedVariantId);
            network.getVariantManager().removeVariant(workingVariantCopyId);
        } finally {
            // here set working variant generators pmin and pmax values to initial values
            //generatorLimitsHandler.resetInitialPminPmax(network);
        }
    }

    static Map<String, Double> computeExchangeValuesMismatch(Network network, String workingVariantCopyId, Map<String, Double> targetExchanges, RaoParameters raoParameters) throws ShiftingException {
        Map<String, Double> mismatchPerBorder;
        LoadFlowResult loadFlowResult = LoadFlow.run(network, workingVariantCopyId, LocalComputationManager.getDefault(), LoadFlowAndSensitivityParameters.getSensitivityWithLoadFlowParameters(raoParameters).getLoadFlowParameters());
        if (!loadFlowResult.isFullyConverged()) {
            String message = String.format("Load-flow computation diverged on network '%s' during balancing adjustment", network.getId());
            throw new ShiftingException(message);
        }
        Map<String, Double> bordersExchanges = CountryBalanceComputation.computeSweBordersExchanges(network);
        double mismatchEsPt = targetExchanges.get(DichotomyDirection.ES_PT.toString()) - bordersExchanges.get(DichotomyDirection.ES_PT.toString());
        double mismatchEsFr = targetExchanges.get(DichotomyDirection.ES_FR.toString()) - bordersExchanges.get(DichotomyDirection.ES_FR.toString());
        mismatchPerBorder = Map.of(DichotomyDirection.ES_FR.toString(), mismatchEsFr, DichotomyDirection.ES_PT.toString(), mismatchEsPt);
        BUSINESS_LOGS.info("Resulting exchanges: PT->ES: {}, FR->ES: {}", -bordersExchanges.get(DichotomyDirection.ES_PT.toString()), -bordersExchanges.get(DichotomyDirection.ES_FR.toString()));
        BUSINESS_LOGS.info("Mismatch: PT->ES: {}, FR->ES: {}", mismatchEsPt, mismatchEsFr);
        return mismatchPerBorder;
    }

    private void shiftNetPositions(Network network, Map<String, Double> scalingValuePerCountry) throws GlskLimitationException {
        String logTargetCountriesShift = String.format("Target shifts by country: [ES = %.2f, FR = %.2f, PT = %.2f]",
            scalingValuePerCountry.get(EI_CODE_ES), scalingValuePerCountry.get(EI_CODE_FR), scalingValuePerCountry.get(EI_CODE_PT));
        BUSINESS_LOGS.info(logTargetCountriesShift);

        List<String> limitingCountries = new ArrayList<>();
        for (Map.Entry<String, Double> entry : scalingValuePerCountry.entrySet()) {
            String zoneId = entry.getKey();
            double asked = entry.getValue();
            String logApplyingVariationOnZone = String.format("Applying variation on zone %s (target: %.2f)", zoneId, asked);
            BUSINESS_LOGS.info(logApplyingVariationOnZone);
            ScalingParameters scalingParameters = new ScalingParameters();
            scalingParameters.setPriority(ScalingParameters.Priority.RESPECT_OF_VOLUME_ASKED);
            scalingParameters.setReconnect(true);
            double done = zonalScalable.getData(zoneId).scale(network, asked, scalingParameters);
            if (Math.abs(done - asked) > shiftTolerance) {
                String logWarnIncompleteVariation = String.format("Incomplete variation on zone %s (target: %.2f, done: %.2f)", zoneId, asked, done);
                BUSINESS_WARNS.warn(logWarnIncompleteVariation);
                limitingCountries.add(zoneId);
            }
        }
        if (!limitingCountries.isEmpty()) {
            StringJoiner sj = new StringJoiner(", ", "There are GLSK limitation(s) in ", ".");
            limitingCountries.forEach(sj::add);
            BUSINESS_WARNS.warn("{}", sj.toString());
            throw new GlskLimitationException(sj.toString());
        }
    }

    private void preProcessNetwork(Network network, ScalableGeneratorConnector scalableGeneratorConnector, GeneratorLimitsHandler generatorLimitsHandler, String initialVariantId, String processedVariantId, String workingVariantCopyId) throws ShiftingException {
        network.getVariantManager().cloneVariant(initialVariantId, processedVariantId, true);
        network.getVariantManager().setWorkingVariant(processedVariantId);
        scalableGeneratorConnector.fillGeneratorsInitialState(network, Set.of(Country.ES, Country.FR, Country.PT));
        generatorLimitsHandler.setPminPmaxToDefaultValue(network, Set.of(Country.ES, Country.PT));
        network.getVariantManager().cloneVariant(processedVariantId, workingVariantCopyId, true);
        network.getVariantManager().setWorkingVariant(workingVariantCopyId);
    }

    public void updateScalingValuesWithMismatch(Map<String, Double> scalingValuesByCountry, Map<String, Double> mismatchPerBorder) {
        BUSINESS_LOGS.info("Adjusting target shifts to reduce mismatch");
        scalingValuesByCountry.put(EI_CODE_FR, scalingValuesByCountry.get(EI_CODE_FR) - mismatchPerBorder.get(DichotomyDirection.ES_FR.toString()));
        scalingValuesByCountry.put(EI_CODE_PT, scalingValuesByCountry.get(EI_CODE_PT) - mismatchPerBorder.get(DichotomyDirection.ES_PT.toString()));
        scalingValuesByCountry.put(EI_CODE_ES, scalingValuesByCountry.get(EI_CODE_ES) + mismatchPerBorder.get(DichotomyDirection.ES_PT.toString()) + mismatchPerBorder.get(DichotomyDirection.ES_FR.toString()));
    }
}
