package io.eventuate.local.mysql.binlog;

import io.eventuate.cdc.producer.wrappers.DataProducer;
import io.eventuate.cdc.producer.wrappers.DataProducerFactory;
import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.*;
import io.eventuate.local.test.util.SourceTableNameSupplier;
import io.eventuate.local.test.util.TestHelper;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
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
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    @Bean
    @Primary
    public PublishingFilter publishingFilter() {
      return (sourceBinlogFileOffset, destinationTopic) -> true;
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

  @Autowired
  private DataProducerFactory dataProducerFactory;

  @Autowired
  private EventuateKafkaProducer eventuateKafkaProducer;

  private final int nEvents = 1000;

  @Test
  public void testAllEventsSameTopicSameId() throws Exception {

    testPerformance(() -> {
      List<EntityTypeAndId> entityTypesAndIds = new ArrayList<>();

      String entityType = generateId();
      String entityId = generateId();

      for (int i = 0; i < nEvents; i++) {
        entityTypesAndIds.add(new EntityTypeAndId(entityType, entityId));
      }

      prepareTopics(entityTypesAndIds);
      generateEvents(entityTypesAndIds);
    });
  }

  @Test
  public void testAllEventsDifferentTopicsSameId() throws Exception {
    testPerformance(() -> {

      List<EntityTypeAndId> entityTypesAndIds = new ArrayList<>();

      String entityId = generateId();

      for (int i = 0; i < nEvents; i++) {
        entityTypesAndIds.add(new EntityTypeAndId(generateId(), entityId));
      }

      prepareTopics(entityTypesAndIds);
      generateEvents(entityTypesAndIds);
    });
  }

  @Test
  public void test2TopicsSameId() throws Exception {
    testPerformance(() -> {

      List<EntityTypeAndId> entityTypesAndIds = new ArrayList<>();

      String entityType = generateId();
      String entityId = generateId();

      for (int i = 0; i < nEvents; i++) {
        entityTypesAndIds.add(new EntityTypeAndId(entityType + (i % 2), entityId));
      }

      prepareTopics(entityTypesAndIds);
      generateEvents(entityTypesAndIds);
    });
  }

  @Test
  public void test4TopicsSameId() throws Exception {
    testPerformance(() -> {

      List<EntityTypeAndId> entityTypesAndIds = new ArrayList<>();

      String entityType = generateId();
      String entityId = generateId();

      for (int i = 0; i < nEvents; i++) {
        entityTypesAndIds.add(new EntityTypeAndId(entityType + (i % 4), entityId));
      }

      prepareTopics(entityTypesAndIds);
      generateEvents(entityTypesAndIds);
    });
  }

  @Test
  public void test10TopicsSameId() throws Exception {
    testPerformance(() -> {

      List<EntityTypeAndId> entityTypesAndIds = new ArrayList<>();

      String entityType = generateId();
      String entityId = generateId();

      for (int i = 0; i < nEvents; i++) {
        entityTypesAndIds.add(new EntityTypeAndId(entityType + (i % 10), entityId));
      }

      prepareTopics(entityTypesAndIds);
      generateEvents(entityTypesAndIds);
    });
  }

  @Test
  public void test100TopicsSameId() throws Exception {
    testPerformance(() -> {
      List<EntityTypeAndId> entityTypesAndIds = new ArrayList<>();

      String entityType = generateId();
      String entityId = generateId();

      for (int i = 0; i < nEvents; i++) {
        entityTypesAndIds.add(new EntityTypeAndId(entityType + (i % 100), entityId));
      }

      prepareTopics(entityTypesAndIds);
      generateEvents(entityTypesAndIds);
    });
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Test
  public void testAllEventsSameTopicDifferentIds() throws Exception {

    testPerformance(() -> {
      List<EntityTypeAndId> entityTypesAndIds = new ArrayList<>();

      String entityType = generateId();

      for (int i = 0; i < nEvents; i++) {
        entityTypesAndIds.add(new EntityTypeAndId(entityType, generateId()));
      }

      prepareTopics(entityTypesAndIds);
      generateEvents(entityTypesAndIds);
    });
  }

  @Test
  public void testAllEventsDifferentTopicsDifferentIds() throws Exception {
    testPerformance(() -> {

      List<EntityTypeAndId> entityTypesAndIds = new ArrayList<>();

      for (int i = 0; i < nEvents; i++) {
        entityTypesAndIds.add(new EntityTypeAndId(generateId(), generateId()));
      }

      prepareTopics(entityTypesAndIds);
      generateEvents(entityTypesAndIds);
    });
  }

  @Test
  public void test2TopicsDifferentIds() throws Exception {
    testPerformance(() -> {

      List<EntityTypeAndId> entityTypesAndIds = new ArrayList<>();

      String entityType = generateId();

      for (int i = 0; i < nEvents; i++) {
        entityTypesAndIds.add(new EntityTypeAndId(entityType + (i % 2), generateId()));
      }

      prepareTopics(entityTypesAndIds);
      generateEvents(entityTypesAndIds);
    });
  }

  @Test
  public void test4TopicsDifferentIds() throws Exception {
    testPerformance(() -> {

      List<EntityTypeAndId> entityTypesAndIds = new ArrayList<>();

      String entityType = generateId();

      for (int i = 0; i < nEvents; i++) {
        entityTypesAndIds.add(new EntityTypeAndId(entityType + (i % 4), generateId()));
      }

      prepareTopics(entityTypesAndIds);
      generateEvents(entityTypesAndIds);
    });
  }

  @Test
  public void test10TopicsDifferentIds() throws Exception {
    testPerformance(() -> {

      List<EntityTypeAndId> entityTypesAndIds = new ArrayList<>();

      String entityType = generateId();

      for (int i = 0; i < nEvents; i++) {
        entityTypesAndIds.add(new EntityTypeAndId(entityType + (i % 10), generateId()));
      }

      prepareTopics(entityTypesAndIds);
      generateEvents(entityTypesAndIds);
    });
  }

  @Test
  public void test100TopicsDifferentIds() throws Exception {
    testPerformance(() -> {
      List<EntityTypeAndId> entityTypesAndIds = new ArrayList<>();

      String entityType = generateId();

      for (int i = 0; i < nEvents; i++) {
        entityTypesAndIds.add(new EntityTypeAndId(entityType + (i % 100), generateId()));
      }

      prepareTopics(entityTypesAndIds);
      generateEvents(entityTypesAndIds);
    });
  }

  private void prepareTopics(List<EntityTypeAndId> entityTypesAndIds) {
    DataProducer dataProducer = dataProducerFactory.create();

    entityTypesAndIds
            .stream()
            .map(EntityTypeAndId::getType)
            .collect(Collectors.toSet())
            .forEach(eventType -> {
              try {
                eventuateKafkaProducer.partitionsFor(eventType);
                eventuateKafkaProducer.send(eventType, generateId(), generateId());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
  }

  private void generateEvents(List<EntityTypeAndId> entityTypesAndIds) {
    entityTypesAndIds.forEach(entityTypeAndId -> {
      testHelper.saveEvent(entityTypeAndId.getType(),
              generateId(),
              generateId(),
              generateId(),
              entityTypeAndId.getId(),
              new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA));
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

    Eventually.eventually(1000, 5000, TimeUnit.MILLISECONDS, () -> {

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

  private static class EntityTypeAndId {
    private String type;
    private String id;

    public EntityTypeAndId(String type, String id) {
      this.type = type;
      this.id = id;
    }

    public String getType() {
      return type;
    }

    public String getId() {
      return id;
    }
  }
}
