package com.farao_community.farao.swe_csa.app.security_evaluator;

import com.powsybl.openrao.commons.PhysicalParameter;
import com.powsybl.openrao.data.crac.api.State;
import com.powsybl.openrao.data.crac.api.cnec.Cnec;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BorderStateMapper {

    private BorderStateMapper() {
    }

    /**
     * Builds a map of State → set of Borders that contain that state.
     * Preventive states are excluded.
     */
    public static Map<State, EnumSet<Border>> mapContingencyStates(List<BorderContext> borders, PhysicalParameter parameter) {
        Map<State, EnumSet<Border>> merged = new HashMap<>();
        for (BorderContext ctx : borders) {
            ctx.crac().getCnecs(parameter).stream()
                    .map(Cnec::getState)
                    .filter(state -> !state.isPreventive())
                    .forEach(state -> merged.computeIfAbsent(state, k -> EnumSet.noneOf(Border.class)).add(ctx.border()));
        }
        return merged;
    }
}
