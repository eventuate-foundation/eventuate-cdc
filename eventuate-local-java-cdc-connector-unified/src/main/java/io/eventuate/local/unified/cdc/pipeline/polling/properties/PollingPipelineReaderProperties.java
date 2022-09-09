package io.eventuate.local.unified.cdc.pipeline.polling.properties;


import io.eventuate.common.jdbc.OutboxPartitioningSpec;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineReaderProperties;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;

public class PollingPipelineReaderProperties extends CdcPipelineReaderProperties {
  private Integer pollingIntervalInMilliseconds = 500;
  private Integer maxEventsPerPolling = 1000;
  private Integer maxAttemptsForPolling = 100;
  private Integer pollingRetryIntervalInMilliseconds = 500;
  private Set<String> pollingParallelChannels;
  private OutboxPartitioningSpec outboxPartitioning = OutboxPartitioningSpec.DEFAULT;

  public Integer getPollingIntervalInMilliseconds() {
    return pollingIntervalInMilliseconds;
  }

  public void setPollingIntervalInMilliseconds(Integer pollingIntervalInMilliseconds) {
    this.pollingIntervalInMilliseconds = pollingIntervalInMilliseconds;
  }

  public Integer getMaxEventsPerPolling() {
    return maxEventsPerPolling;
  }

  public void setMaxEventsPerPolling(Integer maxEventsPerPolling) {
    this.maxEventsPerPolling = maxEventsPerPolling;
  }

  public Integer getMaxAttemptsForPolling() {
    return maxAttemptsForPolling;
  }

  public void setMaxAttemptsForPolling(Integer maxAttemptsForPolling) {
    this.maxAttemptsForPolling = maxAttemptsForPolling;
  }

  public Integer getPollingRetryIntervalInMilliseconds() {
    return pollingRetryIntervalInMilliseconds;
  }

  public void setPollingRetryIntervalInMilliseconds(Integer pollingRetryIntervalInMilliseconds) {
    this.pollingRetryIntervalInMilliseconds = pollingRetryIntervalInMilliseconds;
  }

  public void setPollingParallelChannels(Set<String> pollingParallelChannels) {
    this.pollingParallelChannels = pollingParallelChannels;
  }

  public void setPollingParallelChannelNames(String pollingParallelChannelNames) {
    pollingParallelChannelNames = pollingParallelChannelNames.trim();
    if (pollingParallelChannelNames.isEmpty())
      return;
    this.pollingParallelChannels = Arrays.stream(pollingParallelChannelNames.split(",")).map(String::trim).collect(Collectors.toSet());
  }

  public Set<String> getPollingParallelChannels() {
    return pollingParallelChannels == null ? emptySet() : pollingParallelChannels;
  }

  public void setOutboxPartitioning(OutboxPartitioningSpec outboxPartitioning) {
    this.outboxPartitioning = outboxPartitioning;
  }

  public void setOutboxTables(int outboxTables) {
    this.outboxPartitioning = outboxPartitioning.withOutboxTables(outboxTables);
  }

  public void setOutboxTablePartitions(int tablePartitions) {
    this.outboxPartitioning = outboxPartitioning.withTablePartitions(tablePartitions);
  }

  public OutboxPartitioningSpec getOutboxPartitioning() {
    return outboxPartitioning;
  }
}
