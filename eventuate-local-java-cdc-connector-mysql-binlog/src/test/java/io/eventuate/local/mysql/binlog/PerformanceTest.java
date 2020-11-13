package io.eventuate.local.mysql.binlog;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.id.ApplicationIdGenerator;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.BinlogEntryToPublishedEventConverter;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.PublishingFilter;
import io.eventuate.local.test.util.SourceTableNameSupplier;
import io.eventuate.local.test.util.TestHelper;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.util.test.async.Eventually;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {PerformanceTest.Config.class})
public class PerformanceTest {

  @Configuration
  @EnableAutoConfiguration
  @Import({MySqlBinlogCdcIntegrationTestConfiguration.class, OffsetStoreMockConfiguration.class})
  public static class Config {
    @Bean
    @Primary
    public PublishingFilter publishingFilter() {
      return (sourceBinlogFileOffset, destinationTopic) -> true;
    }

  }

  @Autowired
  private MySqlBinaryLogClient mySqlBinaryLogClient;

  @Autowired
  private TestHelper testHelper;

  @Autowired
  private SourceTableNameSupplier sourceTableNameSupplier;

  @Autowired
  private CdcDataPublisher<PublishedEvent> cdcDataPublisher;

  @Autowired
  private EventuateKafkaProducer eventuateKafkaProducer;

  private final int nEvents = 1000;

  @Test
  public void testAllEventsSameTopicSameId() {
    runTest(nEvents, makeFixedEntityTypeGenerator(), makeFixedEntityIdSupplier());
  }

  @Test
  public void testAllEventsDifferentTopicsSameId() {
    runTest(nEvents, makeUniqueEntityTypeGenerator(), makeFixedEntityIdSupplier());
  }

  @Test
  public void test2TopicsSameId() {
    runTest(nEvents, makeNEntityTypeGenerator(2), makeFixedEntityIdSupplier());
  }

  @Test
  public void test4TopicsSameId() {
    runTest(nEvents, makeNEntityTypeGenerator(4), makeFixedEntityIdSupplier());
  }

  @Test
  public void test10TopicsSameId() {
    runTest(nEvents, makeNEntityTypeGenerator(10), makeFixedEntityIdSupplier());
  }

  @Test
  public void test100TopicsSameId() {
    runTest(nEvents, makeNEntityTypeGenerator(100), makeFixedEntityIdSupplier());
  }

  @Test
  public void testAllEventsSameTopicDifferentIds()  {
    runTest(nEvents, makeFixedEntityTypeGenerator(), makeUniqueEntityIdSupplier());
  }

  @Test
  public void testAllEventsDifferentTopicsDifferentIds()  {
    runTest(nEvents, makeUniqueEntityTypeGenerator(), makeUniqueEntityIdSupplier());
  }

  @Test
  public void test2TopicsDifferentIds() {
    runTest(nEvents, makeNEntityTypeGenerator(2), makeUniqueEntityIdSupplier());
  }

  @Test
  public void test4TopicsDifferentIds() {
    runTest(nEvents, makeNEntityTypeGenerator(4), makeUniqueEntityIdSupplier());
  }

  @Test
  public void test10TopicsDifferentIds() {
    runTest(nEvents, makeNEntityTypeGenerator(10), makeUniqueEntityIdSupplier());
  }

  @Test
  public void test100TopicsDifferentIds() {
    runTest(nEvents, makeNEntityTypeGenerator(100), makeUniqueEntityIdSupplier());
  }

  private void runTest(int nEvents, Function<Integer, String> entityTypeGenerator, Supplier<String> entityIdGenerator) {
    testPerformance(() -> {

      List<EntityTypeAndId> entityTypesAndIds = new ArrayList<>();

      for (int i = 0; i < nEvents; i++) {
        entityTypesAndIds.add(new EntityTypeAndId(entityTypeGenerator.apply(i), entityIdGenerator.get()));
      }

      prepareTopics(entityTypesAndIds);
      generateEvents(entityTypesAndIds);
    });
  }

  private Function<Integer, String> makeFixedEntityTypeGenerator() {
    String entityType = generateId();
    return (i) -> entityType;
  }

  private Function<Integer, String> makeUniqueEntityTypeGenerator() {
    return (i) -> generateId();
  }

  private Function<Integer, String> makeNEntityTypeGenerator(int n) {
    String entityType = generateId();
    return (i) -> entityType + (i % n);
  }

  private Supplier<String> makeUniqueEntityIdSupplier() {
    return this::generateId;
  }

  private Supplier<String> makeFixedEntityIdSupplier() {
    String id = generateId();

    return () -> id;
  }

  private void prepareTopics(List<EntityTypeAndId> entityTypesAndIds) {
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
              entityTypeAndId.getId(),
              new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA));
    });
  }

  private void testPerformance(Runnable eventCreator) {
    cdcDataPublisher.start();

    eventCreator.run();

    mySqlBinaryLogClient.addBinlogEntryHandler(new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA),
            sourceTableNameSupplier.getSourceTableName(),
            new BinlogEntryToPublishedEventConverter(new ApplicationIdGenerator()),
            cdcDataPublisher::sendMessage);

    testHelper.runInSeparateThread(mySqlBinaryLogClient::start);

    Eventually.eventually(1000, 500, TimeUnit.MILLISECONDS, () -> {

      System.out.println("--------------");
      System.out.println("--------------");
      System.out.println("--------------");

      Assert.assertEquals(nEvents, cdcDataPublisher.getTotallyProcessedEventCount());
      System.out.println(String.format("%s event processing took %s ms, average send time is %s ms",
              nEvents,
              (cdcDataPublisher.getTimeOfLastProcessedEvent() - mySqlBinaryLogClient.getEventProcessingStartTime()) / 1000000d,
              cdcDataPublisher.getSendTimeAccumulator() / (double) nEvents / 1000000d));


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
