package com.farao_community.farao.swe_csa.app.security_evaluator;

import com.farao_community.farao.swe_csa.app.FileImporter;
import com.farao_community.farao.swe_csa.app.dichotomy.CounterTradingValues;
import com.farao_community.farao.swe_csa.app.dichotomy.DichotomyStepResult;
import com.farao_community.farao.swe_csa.app.dichotomy.ParallelDichotomiesResult;
import com.powsybl.glsk.commons.ZonalData;
import com.powsybl.iidm.modification.scalable.Scalable;
import com.powsybl.iidm.network.Network;
import com.powsybl.loadflow.LoadFlowParameters;
import com.powsybl.openrao.data.crac.api.Crac;
import com.powsybl.openrao.data.raoresult.api.RaoResult;
import com.powsybl.openrao.data.raoresult.io.json.RaoResultJsonImporter;
import com.powsybl.openrao.raoapi.parameters.RaoParameters;
import com.powsybl.openrao.raoapi.parameters.extensions.LoadFlowAndSensitivityParameters;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Instant;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class SweCsaRaoResultValidatorTest {
    @Autowired
    FileImporter fileImporter;

    private static final Logger LOGGER = LoggerFactory.getLogger(SweCsaRaoResultValidatorTest.class);

    @Test
    void twoBordersFlowCnecSecurityCheckerTest() {
        Network network = fileImporter.importNetwork("taskId", Objects.requireNonNull(getClass().getResource("/security_evaluator/TestCase_with_swe_countries.xiidm")).toString());

        Crac frEsCrac = fileImporter.importCrac("taskId", Objects.requireNonNull(getClass().getResource("/security_evaluator/crac_fr_es.json")).toString(), network);
        Crac ptEsCrac = fileImporter.importCrac("taskId", Objects.requireNonNull(getClass().getResource("/security_evaluator/crac_pt_es.json")).toString(), network);

        RaoResult frEsRaoResult = new RaoResultJsonImporter().importData(getClass().getResourceAsStream("/security_evaluator/rao_result_fr_es.json"), frEsCrac);
        RaoResult ptEsRaoResult = new RaoResultJsonImporter().importData(getClass().getResourceAsStream("/security_evaluator/rao_result_pt_es.json"), ptEsCrac);

        RaoParameters raoParameters = RaoParameters.load();
        String loadFlowProvider =  LoadFlowAndSensitivityParameters.getLoadFlowProvider(raoParameters);
        LoadFlowParameters loadFlowParameters = LoadFlowAndSensitivityParameters.getSensitivityWithLoadFlowParameters(raoParameters).getLoadFlowParameters();
        int numberOfLoadFlowsInParallel = 1;

        TwoBordersFlowCnecSecurityChecker twoBordersFlowCnecSecurityChecker = new TwoBordersFlowCnecSecurityChecker(network, frEsCrac, ptEsCrac, frEsRaoResult, ptEsRaoResult, numberOfLoadFlowsInParallel, LOGGER, loadFlowProvider, loadFlowParameters);
        Pair<Boolean, Boolean> flowSecurityPair = twoBordersFlowCnecSecurityChecker.check();
        assertEquals(flowSecurityPair, Pair.of(true, false));
    }

    @Test
    void sweCsaRaoResultValidatorTest() {
        Network network = fileImporter.importNetwork("taskId", Objects.requireNonNull(getClass().getResource("/security_evaluator/TestCase_with_swe_countries.xiidm")).toString());

        Crac frEsCrac = fileImporter.importCrac("taskId", Objects.requireNonNull(getClass().getResource("/security_evaluator/crac_fr_es.json")).toString(), network);
        Crac ptEsCrac = fileImporter.importCrac("taskId", Objects.requireNonNull(getClass().getResource("/security_evaluator/crac_pt_es.json")).toString(), network);
        ZonalData<Scalable> zonalScalable = fileImporter.getZonalData("taskId", Instant.parse("2017-04-13T07:00:00Z"), Objects.requireNonNull(getClass().getResource("/security_evaluator/glsk-document-cim.xml")).toString(), network);

        RaoResult frEsRaoResult = new RaoResultJsonImporter().importData(getClass().getResourceAsStream("/security_evaluator/rao_result_fr_es.json"), frEsCrac);
        RaoResult ptEsRaoResult = new RaoResultJsonImporter().importData(getClass().getResourceAsStream("/security_evaluator/rao_result_pt_es.json"), ptEsCrac);
        // Fixme: data for angle cnec and maybe angle RAs
        frEsCrac.removeAngleCnec("Angle-Cnec-Fr-Es-1-preventive");
        frEsCrac.removeAngleCnec("Angle-Cnec-Fr-Es-1-curative 3");

        RaoParameters raoParameters = RaoParameters.load();
        String loadFlowProvider =  LoadFlowAndSensitivityParameters.getLoadFlowProvider(raoParameters);
        LoadFlowParameters loadFlowParameters = LoadFlowAndSensitivityParameters.getSensitivityWithLoadFlowParameters(raoParameters).getLoadFlowParameters();
        SweCsaRaoResultValidator sweCsaRaoResultValidator = new SweCsaRaoResultValidator(loadFlowProvider, loadFlowParameters, LOGGER);
        CounterTradingValues counterTradingValue = new CounterTradingValues(100, -100);
        DichotomyStepResult frEsDichotomyStepResult = DichotomyStepResult.fromNetworkValidationResult(frEsRaoResult, true, null, counterTradingValue);
        DichotomyStepResult ptEsDichotomyStepResult = DichotomyStepResult.fromNetworkValidationResult(ptEsRaoResult, true, null, counterTradingValue);
        // ParallelDichotomiesResult inverse the order of ptEs and frEs
        ParallelDichotomiesResult parallelDichotomiesResult = new ParallelDichotomiesResult(ptEsDichotomyStepResult, frEsDichotomyStepResult, counterTradingValue);
        ParallelDichotomiesResult validatedParallelDichotomiesResult = sweCsaRaoResultValidator.validateNetworkForTwoBorders(network, parallelDichotomiesResult, frEsCrac, ptEsCrac, zonalScalable);
        // Assert
        assertNotNull(validatedParallelDichotomiesResult);
        assertNotNull(validatedParallelDichotomiesResult.getFrEsResult().getRaoResult());
        assertNotNull(validatedParallelDichotomiesResult.getPtEsResult().getRaoResult());
        assertTrue(validatedParallelDichotomiesResult.getFrEsResult().getRaoResult().isSecure());
        assertFalse(validatedParallelDichotomiesResult.getPtEsResult().getRaoResult().isSecure());
    }
}
