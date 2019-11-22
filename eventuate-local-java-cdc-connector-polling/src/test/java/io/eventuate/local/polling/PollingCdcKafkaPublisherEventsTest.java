package io.eventuate.local.polling;

import io.eventuate.cdc.producer.wrappers.kafka.EventuateKafkaDataProducerWrapper;
import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.local.common.BinlogEntryToPublishedEventConverter;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.DuplicatePublishingDetector;
import io.eventuate.local.test.util.CdcKafkaPublisherEventsTest;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


@ActiveProfiles("EventuatePolling")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = PollingIntegrationTestConfiguration.class)
public class PollingCdcKafkaPublisherEventsTest extends CdcKafkaPublisherEventsTest {

  @Autowired
  private PollingDao pollingDao;

  @Autowired
  private DuplicatePublishingDetector duplicatePublishingDetector;

  @Before
  public void init() {
    super.init();

    pollingDao.addBinlogEntryHandler(eventuateSchema,
            sourceTableNameSupplier.getSourceTableName(),
            new BinlogEntryToPublishedEventConverter(),
            cdcDataPublisher);

    testHelper.runInSeparateThread(pollingDao::start);
  }

  @Override
  protected CdcDataPublisher<PublishedEvent> createCdcKafkaPublisher() {
    return new CdcDataPublisher<>(() ->
            new EventuateKafkaDataProducerWrapper(new EventuateKafkaProducer(eventuateKafkaConfigurationProperties.getBootstrapServers(),
                    EventuateKafkaProducerConfigurationProperties.empty())),
            duplicatePublishingDetector,
            publishingStrategy,
            meterRegistry);
  }

  @Override
  public void clear() {
    pollingDao.stop();
  }
}
