package io.eventuate.local.mysql.binlog;

import io.eventuate.local.common.BinlogEntryToPublishedEventConverter;
import io.eventuate.local.db.log.test.common.AbstractDbLogBasedCdcKafkaPublisherEventsTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = {MySqlBinlogCdcIntegrationTestConfiguration.class, KafkaOffsetStoreConfiguration.class})
public class MySQLCdcKafkaPublisherEventsTest extends AbstractDbLogBasedCdcKafkaPublisherEventsTest {
  @Autowired
  private MySqlBinaryLogClient mySqlBinaryLogClient;

  @BeforeEach
  @Override
  public void init() {
    super.init();

    mySqlBinaryLogClient.addBinlogEntryHandler(eventuateSchema,
            sourceTableNameSupplier.getSourceTableName(),
            new BinlogEntryToPublishedEventConverter(idGenerator),
            cdcDataPublisher::sendMessage);

    testHelper.runInSeparateThread(mySqlBinaryLogClient::start);
  }

  @AfterEach
  @Override
  public void clear() {
    mySqlBinaryLogClient.stop();
  }
}
