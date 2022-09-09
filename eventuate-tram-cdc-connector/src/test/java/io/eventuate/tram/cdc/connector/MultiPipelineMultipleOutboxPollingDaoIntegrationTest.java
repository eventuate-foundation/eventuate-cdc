package io.eventuate.tram.cdc.connector;

import io.eventuate.common.id.IdGenerator;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.OutboxPartitioningSpec;
import io.eventuate.common.spring.id.IdGeneratorConfiguration;
import io.eventuate.common.testcontainers.EventuateMySqlContainer;
import io.eventuate.common.testcontainers.EventuateZookeeperContainer;
import io.eventuate.common.testcontainers.PropertyProvidingContainer;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.test.util.TestHelper;
import io.eventuate.local.test.util.TestHelperConfiguration;
import io.eventuate.local.testutil.DefaultAndPollingProfilesResolver;
import io.eventuate.messaging.kafka.basic.consumer.*;
import io.eventuate.messaging.kafka.spring.basic.consumer.EventuateKafkaConsumerSpringConfigurationPropertiesConfiguration;
import io.eventuate.messaging.kafka.testcontainers.EventuateKafkaContainer;
import io.eventuate.tram.cdc.connector.pipeline.CdcTramPipelineConfiguration;
import io.eventuate.util.test.async.Eventually;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.DockerHealthcheckWaitStrategy;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@ActiveProfiles(resolver = DefaultAndPollingProfilesResolver.class)
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = MultiPipelineMultipleOutboxPollingDaoIntegrationTest.Config.class)
@EnableAutoConfiguration
@TestPropertySource("/multi-pipeline-multi-outbox.properties")
public class MultiPipelineMultipleOutboxPollingDaoIntegrationTest {
  public static final int OUTBOX_TABLES = 8;
  private static final int NUMBER_OF_EVENTS_TO_PUBLISH = 20;

  @Autowired
  private OutboxPartitioningSpec outboxPartitioningSpec;

  @Autowired
  private TestHelper testHelper;

  @Autowired
  protected IdGenerator idGenerator;

  @Autowired
  protected EventuateSchema eventuateSchema;

  @Autowired
  protected JdbcTemplate jdbcTemplate;

  @Autowired
  private EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties;

  @Value("${eventuatelocal.kafka.bootstrap.servers}")
  private String bootstrapServers;

  @Configuration
  @Import({CdcTramPipelineConfiguration.class, TestHelperConfiguration.class, IdGeneratorConfiguration.class, EventuateKafkaConsumerSpringConfigurationPropertiesConfiguration.class})
  public static class Config {

    @Bean
    public OutboxPartitioningSpec outboxPartitioningSpec() {
      return new OutboxPartitioningSpec(OUTBOX_TABLES, 4);
    }

  }

  private ExecutorService executor = Executors.newCachedThreadPool();

  public static Network network = Network.newNetwork();
  public static EventuateMySqlContainer mysql =
          new EventuateMySqlContainer()
                  .withNetwork(network)
                  .withNetworkAliases("mysql")
                  .withEnv("EVENTUATE_OUTBOX_TABLES", Integer.toString(OUTBOX_TABLES))
                  .withReuse(true);

  static {
    if (System.getenv("EVENTUATE_OUTBOX_ID") != null)
      mysql.withEnv("USE_DB_ID", "true");

  }

  public static EventuateZookeeperContainer zookeeper = new EventuateZookeeperContainer().withReuse(true)
          .withNetwork(network)
          .withNetworkAliases("zookeeper")
          .withReuse(true);


  public static EventuateKafkaContainer kafka =
          new EventuateKafkaContainer("zookeeper:2181")
                  .waitingFor(new DockerHealthcheckWaitStrategy())
                  .withNetwork(network)
                  .withNetworkAliases("kafka")
                  .withReuse(true);

  @DynamicPropertySource
  static void registerMySqlProperties(DynamicPropertyRegistry registry) {
    DynamicPropertyRegistry mine = new DynamicPropertyRegistry() {
      @Override
      public void add(String name, Supplier<Object> valueSupplier) {
        registry.add(name, valueSupplier);
        if ("spring.datasource.url".equals(name))
          registry.add("eventuate.cdc.reader.mysqlreader1.datasourceurl", valueSupplier);
      }
    };
    PropertyProvidingContainer.startAndProvideProperties(mine, mysql, zookeeper, kafka);
  }

  private String sendMessage(String destination) {
    String rawPayload = "\"" + "payload-" + testHelper.generateId() + "\"";
    return testHelper.saveMessage(idGenerator, rawPayload, destination, Collections.singletonMap("PARTITION_ID", UUID.randomUUID().toString()), eventuateSchema);
  }

  @Test
  public void testPolling() throws Exception {
    String destination = testHelper.generateId();
    String subscriberId  = destination;

    List<String> messageIds = IntStream.range(0, NUMBER_OF_EVENTS_TO_PUBLISH).mapToObj(i -> sendMessage(destination)).collect(Collectors.toList());

    BlockingQueue<ConsumerRecord<String, byte[]>> records = new LinkedBlockingQueue<>();

    EventuateKafkaConsumer consumer = new EventuateKafkaConsumer(subscriberId, (record, voidThrowableBiConsumer) -> {
      records.add(record);
      voidThrowableBiConsumer.accept(null, null);
      return () -> 1;
    }, Collections.singletonList(destination),
            bootstrapServers, eventuateKafkaConsumerConfigurationProperties, new DefaultKafkaConsumerFactory());
    consumer.start();
    Eventually.eventually(() -> assertEquals(messageIds.size(), records.size()));
  }


}
