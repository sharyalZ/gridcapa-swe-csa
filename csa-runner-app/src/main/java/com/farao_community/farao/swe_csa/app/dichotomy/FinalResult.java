package com.farao_community.farao.swe_csa.app.dichotomy;

import com.farao_community.farao.swe_csa.api.resource.Status;
import com.powsybl.openrao.data.raoresult.api.RaoResult;
import org.apache.commons.lang3.tuple.Pair;

public record FinalResult(Pair<RaoResult, Status> ptEsResult, Pair<RaoResult, Status> frEsResult) {

    public static FinalResult fromDichotomyStepResults(DichotomyStepResult ptEsDichotomyStepResult, DichotomyStepResult frEsDichotomyStepResult) {
        RaoResult raoResultPtEs = ptEsDichotomyStepResult.getRaoResult();
        Status ptEsStatus = ptEsDichotomyStepResult.isSecure() ? Status.FINISHED_SECURE : Status.FINISHED_UNSECURE;
        RaoResult raoResultFrEs = frEsDichotomyStepResult.getRaoResult();
        Status frEsStatus = frEsDichotomyStepResult.isSecure() ? Status.FINISHED_SECURE : Status.FINISHED_UNSECURE;
        return new FinalResult(Pair.of(raoResultPtEs, ptEsStatus), Pair.of(raoResultFrEs, frEsStatus));
    }
}
