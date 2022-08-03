package io.eventuate.local.polling.spec;

public class PollingSpecForChannel extends PollingSpec {
    private final String channel;

    public PollingSpecForChannel(String channel) {
        this.channel = channel;
    }

    @Override
    public String toString() {
        return "PollingSpecForChannel{" +
                "channel='" + channel + '\'' +
                '}';
    }

    @Override
    public SqlFragment addToWhere(String destination) {
        return SqlFragment.make("AND %s = %s", destination, "channel", channel);
    }
}
