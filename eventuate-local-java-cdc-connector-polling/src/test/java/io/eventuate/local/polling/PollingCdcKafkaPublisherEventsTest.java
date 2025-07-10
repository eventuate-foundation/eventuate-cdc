package io.eventuate.local.polling;

import io.eventuate.cdc.producer.wrappers.kafka.EventuateKafkaDataProducerWrapper;
import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.local.common.BinlogEntryToPublishedEventConverter;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.DuplicatePublishingDetector;
import io.eventuate.local.test.util.CdcKafkaPublisherEventsTest;
import io.eventuate.local.testutil.DefaultAndPollingProfilesResolver;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;


@ActiveProfiles(resolver = DefaultAndPollingProfilesResolver.class)
@SpringBootTest(classes = PollingIntegrationTestConfiguration.class)
public class PollingCdcKafkaPublisherEventsTest extends CdcKafkaPublisherEventsTest {

  @Autowired
  private PollingDao pollingDao;

  @Autowired
  private DuplicatePublishingDetector duplicatePublishingDetector;

  @BeforeEach
  public void init() {
    super.init();

    pollingDao.addBinlogEntryHandler(eventuateSchema,
            sourceTableNameSupplier.getSourceTableName(),
            new BinlogEntryToPublishedEventConverter(idGenerator),
            cdcDataPublisher::sendMessage);

    testHelper.runInSeparateThread(pollingDao::start);
  }

  @Override
  protected CdcDataPublisher<PublishedEvent> createCdcKafkaPublisher() {
    return new CdcDataPublisher<>(() ->
            new EventuateKafkaDataProducerWrapper(new EventuateKafkaProducer(eventuateKafkaConfigurationProperties.getBootstrapServers(),
                    EventuateKafkaProducerConfigurationProperties.empty()),
                    eventuateConfigurationProperties.isEnableBatchProcessing(),
                    eventuateConfigurationProperties.getMaxBatchSize(),
                    new LoggingMeterRegistry()),
            duplicatePublishingDetector,
            publishingStrategy,
            meterRegistry);
  }

  @Override
  public void clear() {
    pollingDao.stop();
  }
}
