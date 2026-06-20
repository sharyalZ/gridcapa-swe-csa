package com.farao_community.farao.swe_csa.app.security_evaluator;

import com.powsybl.iidm.network.Country;
import com.powsybl.openrao.data.crac.api.RemedialAction;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class AppliedNetworkActionsResult {
    private Set<RemedialAction> appliedNetworkActions;
    private Set<String> networkElementsToBeExcluded;
    private Map<Country, Double> powerToBeRedispatched;

    public Set<RemedialAction> getAppliedNetworkActions() {
        return this.appliedNetworkActions;
    }

    public Set<String> getNetworkElementsToBeExcluded() {
        return this.networkElementsToBeExcluded;
    }

    public Map<Country, Double> getPowerToBeRedispatched() {
        return this.powerToBeRedispatched;
    }

    public static final class AppliedNetworkActionsResultBuilder {
        private static final String REQUIRED_ARGUMENT_MESSAGE = "%s is mandatory when building AppliedNetworkActionsResult.";
        private Set<RemedialAction> appliedNetworkActions;
        private Set<String> networkElementsToBeExcluded;
        private Map<Country, Double> powerToBeRedispatched;

        AppliedNetworkActionsResultBuilder() {
        }

        public AppliedNetworkActionsResult.AppliedNetworkActionsResultBuilder withAppliedNetworkActions(Set<RemedialAction> appliedNetworkActions) {
            this.appliedNetworkActions = appliedNetworkActions;
            return this;
        }

        public AppliedNetworkActionsResult.AppliedNetworkActionsResultBuilder withNetworkElementsToBeExcluded(Set<String> networkElementsToBeExcluded) {
            this.networkElementsToBeExcluded = networkElementsToBeExcluded;
            return this;
        }

        public AppliedNetworkActionsResult.AppliedNetworkActionsResultBuilder withPowerToBeRedispatched(Map<Country, Double> powerToBeRedispatched) {
            this.powerToBeRedispatched = powerToBeRedispatched;
            return this;
        }

        public AppliedNetworkActionsResult build() {
            AppliedNetworkActionsResult appliedNetworkActionsResult = new AppliedNetworkActionsResult();
            appliedNetworkActionsResult.appliedNetworkActions = Objects.requireNonNull(this.appliedNetworkActions, String.format("%s is mandatory when building AppliedNetworkActionsResult.", "Applied network actions"));
            appliedNetworkActionsResult.networkElementsToBeExcluded = this.networkElementsToBeExcluded;
            appliedNetworkActionsResult.powerToBeRedispatched = this.powerToBeRedispatched;
            return appliedNetworkActionsResult;
        }
    }
}
