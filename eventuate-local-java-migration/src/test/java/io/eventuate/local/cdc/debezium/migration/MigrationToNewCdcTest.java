package io.eventuate.local.cdc.debezium.migration;

import com.google.common.collect.ImmutableMap;
import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.local.mysql.binlog.DebeziumBinlogOffsetKafkaStore;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.spring.common.EventuateKafkaPropertiesConfiguration;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutionException;

@SpringBootTest(classes = MigrationToNewCdcTest.EventTableChangesToAggregateTopicRelayTestConfiguration.class)
@DirtiesContext
public class MigrationToNewCdcTest {

  @org.springframework.context.annotation.Configuration
  @EnableAutoConfiguration
  @Import(EventuateKafkaPropertiesConfiguration.class)
  public static class EventTableChangesToAggregateTopicRelayTestConfiguration {
  }

  private final String connectorName = "my-sql-connector";
  private final String file = "binlog.000003";
  private final long offset = 10000;

  @Autowired
  private EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties;

  @Test
  public void test() throws NoSuchFieldException, IllegalAccessException, InterruptedException, ExecutionException {
    saveTestOffset();

    BinlogFileOffset debeziumBinlogFileOffset = getDebeziumOffset();

    Assertions.assertEquals(file, debeziumBinlogFileOffset.getBinlogFilename());
    Assertions.assertEquals(offset, debeziumBinlogFileOffset.getOffset());
  }

  private BinlogFileOffset getDebeziumOffset() {
    DebeziumBinlogOffsetKafkaStore debeziumBinlogOffsetKafkaStore = new DebeziumBinlogOffsetKafkaStore(eventuateKafkaConfigurationProperties,
            EventuateKafkaConsumerConfigurationProperties.empty());

    return debeziumBinlogOffsetKafkaStore.getLastBinlogFileOffset().get();
  }

  private void saveTestOffset() throws NoSuchFieldException, IllegalAccessException, InterruptedException, ExecutionException {

    EmbeddedEngine embeddedEngine = createEmbeddedEngine();

    WorkerConfig workerConfig = getWorkerConfig(embeddedEngine);

    KafkaOffsetBackingStore kafkaOffsetBackingStore = createKafkaOffsetBackingStore(workerConfig);

    Converter keyConverter = getKeyConverter(embeddedEngine);
    Converter valueConverter = getValueConverter(embeddedEngine);

    OffsetStorageWriter offsetStorageWriter = new OffsetStorageWriter(kafkaOffsetBackingStore, connectorName, keyConverter, valueConverter);

    offsetStorageWriter.offset(ImmutableMap.of("server", "my-app-connector"), ImmutableMap.of("file", file, "pos", offset));
    offsetStorageWriter.beginFlush();
    offsetStorageWriter.doFlush((error, result) -> {}).get();
  }

  private Converter getKeyConverter(EmbeddedEngine embeddedEngine) throws NoSuchFieldException, IllegalAccessException {
    Field keyConverterField = embeddedEngine.getClass().getDeclaredField("keyConverter");
    keyConverterField.setAccessible(true);
    return (Converter)keyConverterField.get(embeddedEngine);
  }

  private Converter getValueConverter(EmbeddedEngine embeddedEngine) throws NoSuchFieldException, IllegalAccessException {
    Field valueConverterField = embeddedEngine.getClass().getDeclaredField("valueConverter");
    valueConverterField.setAccessible(true);
    return  (Converter)valueConverterField.get(embeddedEngine);
  }

  private KafkaOffsetBackingStore createKafkaOffsetBackingStore(WorkerConfig workerConfig) {
    KafkaOffsetBackingStore kafkaOffsetBackingStore = new KafkaOffsetBackingStore();
    kafkaOffsetBackingStore.configure(workerConfig);
    kafkaOffsetBackingStore.start();
    return kafkaOffsetBackingStore;
  }

  private WorkerConfig getWorkerConfig(EmbeddedEngine embeddedEngine) throws NoSuchFieldException, IllegalAccessException {
    Field workerConfigField = embeddedEngine.getClass().getDeclaredField("workerConfig");
    workerConfigField.setAccessible(true);

    return (WorkerConfig)workerConfigField.get(embeddedEngine);
  }

  private EmbeddedEngine createEmbeddedEngine() {
    Configuration configuration = createConfig();

    return EmbeddedEngine
            .create()
            .using((success, message, throwable) -> {})
            .notifying(sourceRecord -> {})
            .using(configuration)
            .build();
  }

  private Configuration createConfig() {
    return Configuration.create()
            .with("connector.class",
                    "io.debezium.connector.mysql.MySqlConnector")
            .with("offset.storage", KafkaOffsetBackingStore.class.getName())
            .with("bootstrap.servers", eventuateKafkaConfigurationProperties.getBootstrapServers())
            .with("offset.storage.topic", "eventuate.local.cdc." + connectorName + ".offset.storage")
            .with("poll.interval.ms", 50)
            .with("offset.flush.interval.ms", 6000)
            .with("name", connectorName)
            .with("database.server.id", 85744)
            .with("database.server.name", "my-app-connector")
            .with("database.history",
                    io.debezium.relational.history.KafkaDatabaseHistory.class.getName())
            .with("database.history.kafka.topic",
                    "eventuate.local.cdc." + connectorName + ".history.kafka.topic")
            .with("database.history.kafka.bootstrap.servers",
                    eventuateKafkaConfigurationProperties.getBootstrapServers())
            .build();
  }
}
