package com.farao_community.farao.swe_csa.app.rao_result;

import com.powsybl.openrao.data.crac.api.RemedialAction;
import com.powsybl.openrao.data.crac.api.State;
import com.powsybl.openrao.data.crac.api.rangeaction.CounterTradeRangeAction;
import com.powsybl.openrao.data.crac.api.rangeaction.RangeAction;
import com.powsybl.openrao.data.raoresult.api.RaoResult;
import com.powsybl.openrao.data.raoresult.api.RaoResultClone;
import com.farao_community.farao.swe_csa.api.results.CounterTradingResult;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RaoResultWithCounterTradeRangeActions extends RaoResultClone {

    private final RaoResult raoResult;

    private final CounterTradingResult counterTradingResult;

    public RaoResultWithCounterTradeRangeActions(RaoResult raoResult, CounterTradingResult counterTradingResult) {
        super(raoResult);
        this.raoResult = raoResult;
        this.counterTradingResult = counterTradingResult;
    }

    @Override
    public boolean isActivatedDuringState(State state, RemedialAction<?> remedialAction) {
        return raoResult.isActivatedDuringState(state, remedialAction)
            || counterTradingResult.isActivatedDuringState(state, remedialAction);
    }

    @Override
    public boolean isActivatedDuringState(State state, RangeAction<?> rangeAction) {
        return raoResult.isActivatedDuringState(state, rangeAction)
            || counterTradingResult.isActivatedDuringState(state, rangeAction);
    }

    @Override
    public double getPreOptimizationSetPointOnState(State state, RangeAction<?> rangeAction) {
        if (rangeAction instanceof CounterTradeRangeAction counterTradeRangeAction) {
            return counterTradingResult.getPreOptimizationSetPointOnState(state, counterTradeRangeAction);
        }
        return raoResult.getPreOptimizationSetPointOnState(state, rangeAction);
    }

    @Override
    public double getOptimizedSetPointOnState(State state, RangeAction<?> rangeAction) {
        if (rangeAction instanceof CounterTradeRangeAction counterTradeRangeAction) {
            return counterTradingResult.getOptimizedSetPointOnState(counterTradeRangeAction);
        }
        return raoResult.getOptimizedSetPointOnState(state, rangeAction);
    }

    @Override
    public Set<RangeAction<?>> getActivatedRangeActionsDuringState(State state) {
        Set<RangeAction<?>> overallActivatedRangeActions = new HashSet<>();
        overallActivatedRangeActions.addAll(counterTradingResult.getActivatedRangeActionsDuringState(state));
        overallActivatedRangeActions.addAll(raoResult.getActivatedRangeActionsDuringState(state));
        return overallActivatedRangeActions;
    }

    @Override
    public Map<RangeAction<?>, Double> getOptimizedSetPointsOnState(State state) {
        Map<RangeAction<?>, Double> optimizedSetPointsOnState = new HashMap<>(raoResult.getOptimizedSetPointsOnState(state));
        counterTradingResult.counterTradeRangeActionResults().keySet().forEach(counterTradeRangeAction -> optimizedSetPointsOnState.put(counterTradeRangeAction, counterTradingResult.getOptimizedSetPointOnState(counterTradeRangeAction)));
        return optimizedSetPointsOnState;
    }
}
