package io.eventuate.local.db.log.common;

import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.*;
import io.micrometer.core.instrument.MeterRegistry;

import javax.sql.DataSource;
import java.util.Optional;

public abstract class DbLogClient extends BinlogEntryReader {

  protected String dbUserName;
  protected String dbPassword;
  protected String host;
  protected int port;
  protected String defaultDatabase;
  protected DbLogMetrics dbLogMetrics;
  private boolean checkEntriesForDuplicates;
  protected volatile boolean connected;
  protected CdcMonitoringDao cdcMonitoringDao;

  public DbLogClient(MeterRegistry meterRegistry,
                     String dbUserName,
                     String dbPassword,
                     String dataSourceUrl,
                     DataSource dataSource,
                     String readerName,
                     long replicationLagMeasuringIntervalInMilliseconds,
                     int monitoringRetryIntervalInMilliseconds,
                     int monitoringRetryAttempts,
                     EventuateSchema monitoringSchema,
                     Long readerId) {

    super(meterRegistry,
            dataSourceUrl,
            dataSource,
            readerName,
            readerId);

    cdcMonitoringDao = new CdcMonitoringDao(dataSource,
            monitoringSchema,
            monitoringRetryIntervalInMilliseconds,
            monitoringRetryAttempts);

    dbLogMetrics = new DbLogMetrics(meterRegistry,
            cdcMonitoringDao,
            readerName,
            replicationLagMeasuringIntervalInMilliseconds);

    this.dbUserName = dbUserName;
    this.dbPassword = dbPassword;
    this.dataSourceUrl = dataSourceUrl;

    JdbcUrl jdbcUrl = JdbcUrlParser.parse(dataSourceUrl);
    host = jdbcUrl.getHost();
    port = jdbcUrl.getPort();
    defaultDatabase = jdbcUrl.getDatabase();
  }

  public boolean isConnected() {
    return connected;
  }

  protected boolean shouldSkipEntry(Optional<BinlogFileOffset> startingBinlogFileOffset, BinlogFileOffset offset) {
    if (checkEntriesForDuplicates) {
      if (startingBinlogFileOffset.isPresent()) {
        BinlogFileOffset startingOffset = startingBinlogFileOffset.get();

        if (startingOffset.isSameOrAfter(offset)) {
          return true;
        }
      }

      checkEntriesForDuplicates = false;
    }

    return false;
  }

  @Override
  public void start() {
    checkEntriesForDuplicates = true;
    super.start();
    dbLogMetrics.start();
  }

  @Override
  protected void stopMetrics() {
    super.stopMetrics();
    dbLogMetrics.stop();
  }

  protected void onConnected() {
    dbLogMetrics.onConnected();
    connected = true;
  }

  protected void onDisconnected() {
    dbLogMetrics.onDisconnected();
    connected = false;
  }
}
