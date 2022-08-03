package io.eventuate.local.polling.spec;

public class PollingSpecForAll extends PollingSpec {

    public PollingSpecForAll() {
    }

    @Override
    public String toString() {
        return "PollingSpecForAll{}";
    }

    @Override
    public SqlFragment addToWhere(String destination) {
        return SqlFragment.EMPTY;
    }
}
