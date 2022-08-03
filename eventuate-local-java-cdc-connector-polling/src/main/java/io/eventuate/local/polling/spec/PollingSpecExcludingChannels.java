package io.eventuate.local.polling.spec;

import java.util.Set;

public class PollingSpecExcludingChannels extends PollingSpec {

    private final Set<String> exclusions;

    @Override
    public String toString() {
        return "PollingSpecExcludingChannels{" +
                "exclusions=" + exclusions +
                '}';
    }

    public PollingSpecExcludingChannels(Set<String> exclusions) {
        this.exclusions = exclusions;
    }

    @Override
    public SqlFragment addToWhere(String destination) {
        return SqlFragment.make("AND %s NOT IN (%s)", destination, "channels", exclusions);
    }
}
