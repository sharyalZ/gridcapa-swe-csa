package com.farao_community.farao.swe_csa.app.security_evaluator;

import com.powsybl.openrao.data.crac.api.Crac;
import com.powsybl.openrao.data.raoresult.api.RaoResult;

import java.util.List;

public record BorderContext(Border border, Crac crac, RaoResult raoResult) {
    public static BorderContext find(List<BorderContext> contexts, Border border) {
        return contexts.stream()
                .filter(ctx -> ctx.border().equals(border))
                .findFirst()
                .orElseThrow();
    }
}


