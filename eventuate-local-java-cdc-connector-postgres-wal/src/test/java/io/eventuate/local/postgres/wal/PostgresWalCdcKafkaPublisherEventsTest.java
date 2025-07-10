package io.eventuate.local.postgres.wal;

import io.eventuate.local.common.BinlogEntryToPublishedEventConverter;
import io.eventuate.local.db.log.test.common.AbstractDbLogBasedCdcKafkaPublisherEventsTest;
import io.eventuate.local.testutil.DefaultAndPostgresWalProfilesResolver;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;


@ActiveProfiles(resolver = DefaultAndPostgresWalProfilesResolver.class)
@SpringBootTest(classes = PostgresWalCdcIntegrationTestConfiguration.class)
public class PostgresWalCdcKafkaPublisherEventsTest extends AbstractDbLogBasedCdcKafkaPublisherEventsTest {

  @Autowired
  private PostgresWalClient postgresWalClient;

  @BeforeEach
  @Override
  public void init() {
    super.init();

    postgresWalClient.addBinlogEntryHandler(eventuateSchema,
            sourceTableNameSupplier.getSourceTableName(),
            new BinlogEntryToPublishedEventConverter(idGenerator),
            cdcDataPublisher::sendMessage);

    testHelper.runInSeparateThread(postgresWalClient::start);
  }

  @Override
  public void clear() {
    postgresWalClient.stop();
  }
}
