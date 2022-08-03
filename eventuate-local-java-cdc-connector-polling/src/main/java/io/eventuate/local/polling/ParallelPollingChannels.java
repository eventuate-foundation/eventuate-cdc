package io.eventuate.local.polling;

import io.eventuate.local.polling.spec.PollingSpec;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;

public class ParallelPollingChannels {

    private Set<String> channels;

    @Override
    public String toString() {
        return "ParallelPollingChannels{" +
                "channels=" + channels +
                '}';
    }

    public ParallelPollingChannels(Set<String> channels) {
        this.channels = channels;
    }

    public static ParallelPollingChannels make(String[] channels) {
        return new ParallelPollingChannels(new HashSet<>(Arrays.asList(channels)));
    }

    public int size() {
        return channels.size();
    }

    public List<PollingSpec> makePollingSpecs() {
        return channels.isEmpty() ? singletonList(PollingSpec.ALL) : Stream.concat(Stream.of(PollingSpec.excludingChannels(channels)), channels.stream().map(PollingSpec::forChannel)).collect(Collectors.toList());
    }

}
