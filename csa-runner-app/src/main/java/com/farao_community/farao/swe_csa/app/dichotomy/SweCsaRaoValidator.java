package com.farao_community.farao.swe_csa.app.dichotomy;

/*
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/**
 * @author Jean-Pierre Arnould {@literal <jean-pierre.arnould at rte-france.com>}
 */

import com.farao_community.farao.rao_runner.api.exceptions.RaoRunnerException;
import com.farao_community.farao.rao_runner.api.resource.RaoRequest;
import com.farao_community.farao.rao_runner.api.resource.AbstractRaoResponse;
import com.farao_community.farao.rao_runner.api.resource.RaoFailureResponse;
import com.farao_community.farao.rao_runner.api.resource.RaoSuccessResponse;
import com.farao_community.farao.rao_runner.starter.RaoRunnerClient;
import com.farao_community.farao.swe_csa.api.exception.CsaInternalException;
import com.farao_community.farao.swe_csa.api.exception.RaoInterruptionException;
import com.farao_community.farao.swe_csa.api.resource.CsaRequest;
import com.farao_community.farao.swe_csa.app.FileExporter;
import com.powsybl.glsk.commons.ZonalData;
import com.powsybl.iidm.modification.scalable.Scalable;
import com.powsybl.iidm.network.Network;
import com.powsybl.openrao.commons.PhysicalParameter;
import com.powsybl.openrao.commons.Unit;
import com.powsybl.openrao.data.crac.api.Crac;
import com.powsybl.openrao.data.crac.api.cnec.FlowCnec;
import com.powsybl.openrao.data.raoresult.api.RaoResult;
import com.powsybl.openrao.data.raoresult.io.json.RaoResultJsonImporter;
import com.powsybl.openrao.raoapi.parameters.RaoParameters;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class SweCsaRaoValidator {

    private final FileExporter fileExporter;
    private final RaoRunnerClient raoRunnerClient;
    private final ResultHelper resultHelper = new ResultHelper();

    private final Logger businessLogger;

    public SweCsaRaoValidator(FileExporter fileExporter, RaoRunnerClient raoRunnerClient, Logger businessLogger) {
        this.fileExporter = fileExporter;
        this.raoRunnerClient = raoRunnerClient;
        this.businessLogger = businessLogger;
    }

    public DichotomyStepResult validateNetworkForPortugueseBorder(Network network, Crac crac, String cracUri, ZonalData<Scalable> scalableZonalData, RaoParameters raoParameters, CsaRequest csaRequest, String raoParametersUrl, CounterTradingValues counterTradingValues) {
        return validateNetworkForBorder(network, crac, cracUri, csaRequest, raoParametersUrl, counterTradingValues, "PT-ES", scalableZonalData, raoParameters);
    }

    public DichotomyStepResult validateNetworkForFrenchBorder(Network network, Crac crac, String cracUri, ZonalData<Scalable> scalableZonalData, RaoParameters raoParameters, CsaRequest csaRequest, String raoParametersUrl, CounterTradingValues counterTradingValues) {
        return validateNetworkForBorder(network, crac, cracUri, csaRequest, raoParametersUrl, counterTradingValues, "FR-ES", scalableZonalData, raoParameters);
    }

    private DichotomyStepResult validateNetworkForBorder(Network network, Crac crac, String cracUri, CsaRequest csaRequest, String raoParametersUrl, CounterTradingValues counterTradingValues, String border, ZonalData<Scalable> scalableZonalDataFilteredForSweCountries, RaoParameters raoParameters) {
        RaoRequest raoRequest = buildRaoRequest(counterTradingValues.print(), csaRequest.getBusinessTimestamp(), csaRequest.getId(), network, cracUri, raoParametersUrl, border);

        try {
            businessLogger.info("[{}] : RAO request sent: {}", border, raoRequest);
            AbstractRaoResponse abstractRaoResponse = raoRunnerClient.runRao(raoRequest);
            businessLogger.info("[{}] : RAO response received: {}", border, abstractRaoResponse);

            if (abstractRaoResponse.isRaoFailed()) {
                RaoFailureResponse raoFailureResponse = (RaoFailureResponse) abstractRaoResponse;
                businessLogger.error("[{}] : RAO computation failed: {}", border, raoFailureResponse.getErrorMessage());
                throw new RaoRunnerException(raoFailureResponse.getErrorMessage());
            }

            RaoSuccessResponse raoSuccessResponse = (RaoSuccessResponse) abstractRaoResponse;
            if (raoSuccessResponse.isInterrupted()) {
                throw new RaoInterruptionException(String.format("[%s] : RAO computation related to CSA task: [%s], stopped due to interruption request", raoRequest.getId(), border));
            }
            RaoResult raoResult = new RaoResultJsonImporter().importData(new URI(raoSuccessResponse.getRaoResultFileUrl()).toURL().openStream(), crac);
            businessLogger.info("RAO result imported: {}", raoResult);
            logBorderOverload(raoResult, crac, border);
            boolean isSecure = raoResult.isSecure(PhysicalParameter.FLOW);
            if (isSecure && !crac.getAngleCnecs().isEmpty()) {
                businessLogger.info("{} crac contains Angle CNECs. Angle monitoring will be run.", border);
                raoResult = resultHelper.updateRaoResultWithAngleMonitoring(network, crac, scalableZonalDataFilteredForSweCountries, raoResult, raoParameters);
                isSecure = raoResult.isSecure(PhysicalParameter.FLOW, PhysicalParameter.ANGLE);
                if (isSecure) {
                    businessLogger.info("Angle monitoring secure for {} border, Final result will contain Angle monitoring results", border);
                } else {
                    businessLogger.info("Angle monitoring unsecure for {} border", border);
                }
            }

            if (isSecure && !crac.getVoltageCnecs().isEmpty()) {
                businessLogger.info("{} crac contains Voltage CNECs. Voltage monitoring will be run.", border);
                raoResult = resultHelper.updateRaoResultWithVoltageMonitoring(network, crac, raoResult, raoParameters);
                isSecure = raoResult.isSecure(PhysicalParameter.FLOW, PhysicalParameter.VOLTAGE);
                if (isSecure) {
                    businessLogger.info("Voltage monitoring secure for {} border, Final result will contain Voltage monitoring results", border);
                } else {
                    businessLogger.info("Voltage monitoring unsecure for {} border", border);
                }
            }

            return DichotomyStepResult.fromNetworkValidationResult(raoResult, isSecure, raoSuccessResponse, counterTradingValues);
        } catch (Exception e) {
            throw new CsaInternalException(MDC.get("gridcapaTaskId"), "RAO run failed", e);
        }
    }

    private void logBorderOverload(RaoResult raoResult, Crac crac, String borderName) {
        if (raoResult.isSecure(PhysicalParameter.FLOW)) {
            businessLogger.info("There is no overload on '{}' border", borderName);
        } else {
            businessLogger.info("There is overloads on '{}' border, network is not secure", borderName);
            Set<FlowCnec> flowCnecs = getBorderFlowCnecs(crac, borderName);
            Pair<String, Double> flowCnecSmallestMargin = getFlowCnecSmallestMargin(raoResult, flowCnecs);
            businessLogger.info("On the '{}' border, the most limiting CNEC is {}, with a margin of {}", borderName, flowCnecSmallestMargin.getLeft(), flowCnecSmallestMargin.getRight());

        }
    }

    static Set<FlowCnec> getBorderFlowCnecs(Crac crac, String border) {
        return crac.getFlowCnecs().stream()
            .filter(flowCnec -> flowCnec.isOptimized() && flowCnec.getBorder().equals(border))
            .collect(Collectors.toSet());
    }

    Pair<String, Double> getFlowCnecSmallestMargin(RaoResult raoResult, Set<FlowCnec> flowCnecs) {
        String flowCnecId = "";
        double smallestMargin = Double.MAX_VALUE;
        for (FlowCnec flowCnec : flowCnecs) {
            double margin = raoResult.getMargin(flowCnec.getState().getInstant(), flowCnec, Unit.AMPERE);
            if (margin < smallestMargin) {
                flowCnecId = flowCnec.getId();
                smallestMargin = margin;
            }
        }
        return Pair.of(flowCnecId, smallestMargin);
    }

    private RaoRequest buildRaoRequest(String stepFolder, String timestamp, String taskId, Network network, String cracUrl, String raoParametersUrl, String border) {
        String scaledNetworkPath = generateScaledNetworkPath(network, timestamp, stepFolder);
        String scaledNetworkPreSignedUrl = fileExporter.saveNetworkInArtifact(taskId, network, scaledNetworkPath);
        String raoResultDestination = generateBorderRaoResultPath(border, timestamp, stepFolder);
        return new RaoRequest.RaoRequestBuilder()
            .withId(taskId)
            .withRunId(taskId)
            .withNetworkFileUrl(scaledNetworkPreSignedUrl)
            .withCracFileUrl(cracUrl)
            .withRaoParametersFileUrl(raoParametersUrl)
            .withResultsDestination(raoResultDestination)
            .withEventPrefix(border)
            .build();
    }

    private String generateArtifactsFolder(String timestamp, String stepFolder) {
        OffsetDateTime offsetDateTime = OffsetDateTime.parse(timestamp);
        return "artifacts" + "/" + offsetDateTime.getYear() + "/" + offsetDateTime.getMonthValue() + "/" + offsetDateTime.getDayOfMonth() + "/" + offsetDateTime.getHour() + "_" + offsetDateTime.getMinute() + "/" + stepFolder;
    }

    private String generateBorderRaoResultPath(String border, String timestamp, String stepFolder) {
        return generateArtifactsFolder(timestamp, stepFolder) + "/" + border;
    }

    private String generateScaledNetworkPath(Network network, String timestamp, String stepFolder) {
        return generateArtifactsFolder(timestamp, stepFolder) + "/" + "network-" + network.getVariantManager().getWorkingVariantId() + ".xiidm";
    }

}
