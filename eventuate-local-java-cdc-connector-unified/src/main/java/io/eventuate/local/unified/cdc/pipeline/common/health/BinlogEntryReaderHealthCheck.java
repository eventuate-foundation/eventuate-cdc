package io.eventuate.local.unified.cdc.pipeline.common.health;

import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.local.db.log.common.DbLogClient;
import io.eventuate.local.mysql.binlog.MySqlBinaryLogClient;
import io.eventuate.local.unified.cdc.pipeline.common.BinlogEntryReaderProvider;
import org.springframework.beans.factory.annotation.Value;

public class BinlogEntryReaderHealthCheck extends AbstractHealthCheck {

  @Value("${eventuatelocal.cdc.max.event.interval.to.assume.reader.healthy:#{60000}}")
  private long maxEventIntervalToAssumeReaderHealthy;

  private BinlogEntryReaderProvider binlogEntryReaderProvider;

  public BinlogEntryReaderHealthCheck(BinlogEntryReaderProvider binlogEntryReaderProvider) {
    this.binlogEntryReaderProvider = binlogEntryReaderProvider;
  }

  @Override
  protected void determineHealth(HealthBuilder builder) {

    binlogEntryReaderProvider
            .getAll()
            .forEach(binlogEntryReaderLeadership -> {
              BinlogEntryReader binlogEntryReader = binlogEntryReaderLeadership.getBinlogEntryReader();

              binlogEntryReader.getProcessingError().ifPresent(error -> {
                builder.addError("%s got error during processing: %s".formatted(binlogEntryReader.getReaderName(), error));
              });

              if (binlogEntryReader instanceof MySqlBinaryLogClient client) {
                checkMySqlBinlogReaderHealth(client, builder);
              }

              if (binlogEntryReaderLeadership.isLeader()) {
                checkBinlogEntryReaderHealth(binlogEntryReader, builder);
                if (binlogEntryReader instanceof DbLogClient client) {
                  checkDbLogReaderHealth(client, builder);
                }
              } else
                builder.addDetail("%s is not the leader".formatted(binlogEntryReader.getReaderName()));
            });

  }

  private void checkDbLogReaderHealth(DbLogClient dbLogClient, HealthBuilder builder) {
    if (dbLogClient.isConnected()) {
      builder.addDetail("Reader with id %s is connected".formatted(
              dbLogClient.getReaderName()));
    } else {
      builder.addError("Reader with id %s disconnected".formatted(
              dbLogClient.getReaderName()));
    }

  }

  private void checkBinlogEntryReaderHealth(BinlogEntryReader binlogEntryReader, HealthBuilder builder) {
    long age = System.currentTimeMillis() - binlogEntryReader.getLastEventTime();
    boolean eventNotReceivedInTime =
            age > maxEventIntervalToAssumeReaderHealthy;

    if (eventNotReceivedInTime) {
      builder.addError("Reader with id %s has not received message for %s milliseconds".formatted(
              binlogEntryReader.getReaderName(),
              age));
    } else
      builder.addDetail("Reader with id %s received message %s milliseconds ago".formatted(
              binlogEntryReader.getReaderName(),
              age));
  }

  private void checkMySqlBinlogReaderHealth(MySqlBinaryLogClient mySqlBinaryLogClient, HealthBuilder builder) {
    mySqlBinaryLogClient.getPublishingException().ifPresent(e ->
      builder.addError("Reader with id %s failed to publish event, exception: %s".formatted(
              mySqlBinaryLogClient.getReaderName(),
              e.getMessage())));
  }
}
