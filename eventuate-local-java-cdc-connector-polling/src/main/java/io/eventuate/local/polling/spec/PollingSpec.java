package io.eventuate.local.polling.spec;

import java.util.Set;

public abstract class PollingSpec {
    public static final PollingSpec ALL = new PollingSpecForAll();

    public static PollingSpec excludingChannels(Set<String> exclusions) {
        return new PollingSpecExcludingChannels(exclusions);
    }

    public static PollingSpec forChannel(String channel) {
        return new PollingSpecForChannel(channel);
    }

    public abstract SqlFragment addToWhere(String destination);

}
