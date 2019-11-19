package io.eventuate.local.mysql.binlog;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.*;
import io.eventuate.local.test.util.SourceTableNameSupplier;
import io.eventuate.local.test.util.TestHelper;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.eventuate.util.test.async.Eventually;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.sql.DataSource;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {PerformanceTest.Config.class})
@EnableConfigurationProperties({EventuateKafkaProducerConfigurationProperties.class,
        EventuateKafkaConsumerConfigurationProperties.class})
public class PerformanceTest {

  @Configuration
  @EnableAutoConfiguration
  @Import({MySqlBinlogCdcIntegrationTestConfiguration.class, OffsetStoreMockConfiguration.class})
  public static class Config {
    @Bean
    public MySqlBinaryLogClientForPerformanceTesting mySqlBinaryLogClientPerformanceTester(@Value("${spring.datasource.url}") String dataSourceURL,
                                                                                           DataSource dataSource) {

      return new MySqlBinaryLogClientForPerformanceTesting(
              "root", "rootpassword", dataSourceURL, dataSource, System.nanoTime(), 3000);
    }

  }

  @Autowired
  private MySqlBinaryLogClientForPerformanceTesting mySqlBinaryLogClientForPerformanceTesting;

  @Autowired
  private TestHelper testHelper;

  @Autowired
  private SourceTableNameSupplier sourceTableNameSupplier;

  @Autowired
  private CdcDataPublisher<PublishedEvent> cdcDataPublisher;

  private final int nEvents = 1000;

  @Test
  public void testAllEventsSameTopic() throws Exception {
    testPerformance(() -> {
      String entityType = generateId();
      String entityId = generateId();

      for (int i = 0; i < nEvents; i++) {
        testHelper.saveEvent(entityType,
                generateId(),
                generateId(),
                generateId(),
                entityId,
                new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA));
      }
    });
  }

  @Test
  public void testAllEventsDifferentTopics() throws Exception {
    testPerformance(() -> {

      String entityId = generateId();

      for (int i = 0; i < nEvents; i++) {
        testHelper.saveEvent(generateId(),
                generateId(),
                generateId(),
                generateId(),
                entityId,
                new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA));
      }
    });
  }

  @Test
  public void test10Topics() throws Exception {
    testPerformance(() -> {
      String entityType = generateId();
      String entityId = generateId();

      for (int i = 0; i < nEvents; i++) {
        testHelper.saveEvent(entityType + (i % 10),
                generateId(),
                generateId(),
                generateId(),
                entityId,
                new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA));
      }
    });
  }

  @Test
  public void test100Topics() throws Exception {
    testPerformance(() -> {
      String entityType = generateId();
      String entityId = generateId();

      for (int i = 0; i < nEvents; i++) {
        testHelper.saveEvent(entityType + (i % 100),
                generateId(),
                generateId(),
                generateId(),
                entityId,
                new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA));
      }
    });
  }

  private void testPerformance(Runnable eventCreator) throws Exception {
    cdcDataPublisher.start();

    eventCreator.run();

    mySqlBinaryLogClientForPerformanceTesting.addBinlogEntryHandler(new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA),
            sourceTableNameSupplier.getSourceTableName(),
            new BinlogEntryToPublishedEventConverter(),
            cdcDataPublisher);

    testHelper.runInSeparateThread(mySqlBinaryLogClientForPerformanceTesting::start);

    Eventually.eventually(1000, 500, TimeUnit.MILLISECONDS, () -> {

      System.out.println("--------------");
      System.out.println("--------------");
      System.out.println("--------------");

      Assert.assertEquals(nEvents, cdcDataPublisher.getTotallyProcessedEventCount());
      System.out.println(String.format("%s event processing took %s ms",
              nEvents,
              (cdcDataPublisher.getTimeOfLastPrcessedEvent() - mySqlBinaryLogClientForPerformanceTesting.getEventProcessingStartTime()) / 1000000d));


      System.out.println("--------------");
      System.out.println("--------------");
      System.out.println("--------------");
    });
  }

  private String generateId() {
    return UUID.randomUUID().toString();
  }

}
