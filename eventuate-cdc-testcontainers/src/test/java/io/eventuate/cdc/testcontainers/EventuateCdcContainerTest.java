package io.eventuate.cdc.testcontainers;

import io.eventuate.common.testcontainers.DatabaseContainerFactory;
import io.eventuate.common.testcontainers.EventuateDatabaseContainer;
import io.eventuate.common.testcontainers.EventuateZookeeperContainer;
import io.eventuate.common.testcontainers.PropertyProvidingContainer;
import io.eventuate.messaging.kafka.testcontainers.EventuateKafkaCluster;
import io.eventuate.messaging.kafka.testcontainers.EventuateKafkaContainer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.jdbc.DatabaseDriver;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class EventuateCdcContainerTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public static EventuateKafkaCluster eventuateKafkaCluster = new EventuateKafkaCluster("EventuateCdcTest");

    public static EventuateZookeeperContainer zookeeper = eventuateKafkaCluster.zookeeper;

    public static EventuateKafkaContainer kafka = eventuateKafkaCluster.kafka;

    public static EventuateDatabaseContainer<?> database =
            DatabaseContainerFactory.makeVanillaDatabaseContainer()
                    .withNetwork(eventuateKafkaCluster.network)
                    .withNetworkAliases("customer-service-db")
                    .withReuse(false)
            ;

    private EventuateDatabaseContainer<?> createTestDatabase(String alias) {
        return DatabaseContainerFactory.makeVanillaDatabaseContainer()
                .withNetwork(eventuateKafkaCluster.network)
                .withNetworkAliases(alias)
                .withReuse(false);
    }

    @Test
    public void shouldConfigureTramPipelineEnvVars() {
        EventuateDatabaseContainer<?> db = createTestDatabase("test-db");

        EventuateCdcContainer cdc = EventuateCdcContainer.makeFromDockerfile()
                .withTramPipeline(db);

        Map<String, String> envMap = cdc.getEnvMap();
        assertThat(envMap)
                .containsEntry("EVENTUATE_CDC_PIPELINE_PIPELINE1_TYPE", "eventuate-tram")
                .containsEntry("EVENTUATE_CDC_PIPELINE_PIPELINE1_READER", "reader1");
    }

    @Test
    public void shouldConfigureLocalPipelineEnvVars() {
        EventuateDatabaseContainer<?> db = createTestDatabase("test-db");

        EventuateCdcContainer cdc = EventuateCdcContainer.makeFromDockerfile()
                .withLocalPipeline(db);

        Map<String, String> envMap = cdc.getEnvMap();
        assertThat(envMap)
                .containsEntry("EVENTUATE_CDC_PIPELINE_PIPELINE1_TYPE", "eventuate-local")
                .containsEntry("EVENTUATE_CDC_PIPELINE_PIPELINE1_READER", "reader1");
    }

    @Test
    public void shouldConfigureTramAndLocalPipelineEnvVars() {
        EventuateDatabaseContainer<?> db = createTestDatabase("test-db");

        EventuateCdcContainer cdc = EventuateCdcContainer.makeFromDockerfile()
                .withTramAndLocalPipeline(db);

        Map<String, String> envMap = cdc.getEnvMap();

        // Tram pipeline
        assertThat(envMap)
                .containsEntry("EVENTUATE_CDC_PIPELINE_PIPELINE1_TYPE", "eventuate-tram")
                .containsEntry("EVENTUATE_CDC_PIPELINE_PIPELINE1_READER", "reader1");

        // Local pipeline sharing same reader
        assertThat(envMap)
                .containsEntry("EVENTUATE_CDC_PIPELINE_PIPELINE2_TYPE", "eventuate-local")
                .containsEntry("EVENTUATE_CDC_PIPELINE_PIPELINE2_READER", "reader1");

        // No second reader
        assertThat(envMap).doesNotContainKey("EVENTUATE_CDC_READER_READER2_DATASOURCEURL");
    }

    @Test
    public void shouldConfigureMultiplePipelinesWithCorrectIndices() {
        EventuateDatabaseContainer<?> db1 = createTestDatabase("db1");
        EventuateDatabaseContainer<?> db2 = createTestDatabase("db2");
        EventuateDatabaseContainer<?> db3 = createTestDatabase("db3");

        EventuateCdcContainer cdc = EventuateCdcContainer.makeFromDockerfile()
                .withTramPipeline(db1)
                .withTramAndLocalPipeline(db2)
                .withTramPipeline(db3);

        Map<String, String> envMap = cdc.getEnvMap();

        // db1: reader1, pipeline1
        assertThat(envMap)
                .containsEntry("EVENTUATE_CDC_PIPELINE_PIPELINE1_TYPE", "eventuate-tram")
                .containsEntry("EVENTUATE_CDC_PIPELINE_PIPELINE1_READER", "reader1");

        // db2: reader2, pipeline2 (tram) + pipeline3 (local)
        assertThat(envMap)
                .containsEntry("EVENTUATE_CDC_PIPELINE_PIPELINE2_TYPE", "eventuate-tram")
                .containsEntry("EVENTUATE_CDC_PIPELINE_PIPELINE2_READER", "reader2")
                .containsEntry("EVENTUATE_CDC_PIPELINE_PIPELINE3_TYPE", "eventuate-local")
                .containsEntry("EVENTUATE_CDC_PIPELINE_PIPELINE3_READER", "reader2");

        // db3: reader3, pipeline4
        assertThat(envMap)
                .containsEntry("EVENTUATE_CDC_PIPELINE_PIPELINE4_TYPE", "eventuate-tram")
                .containsEntry("EVENTUATE_CDC_PIPELINE_PIPELINE4_READER", "reader3");

        // No reader4
        assertThat(envMap).doesNotContainKey("EVENTUATE_CDC_READER_READER4_DATASOURCEURL");
    }

    @Test
    public void shouldStart() {
        startAndApplyMigrations();
        EventuateCdcContainer cdc = EventuateCdcContainer.makeFromDockerfile()
                .withKafkaCluster(eventuateKafkaCluster)
                .withTramPipeline(database)
                .dependsOn(zookeeper, kafka, database);
        Startables.deepStart(cdc).join();
        cdc.stop();
    }

    @Test
    public void shouldStartWithKafkaCoordination() {
        startCdcWithKafkaCoordination(cdc -> cdc.withTramPipeline(database));
    }

    @Test
    public void shouldStartWithKafkaCoordinationAndTramAndLocalPipeline() {
        startCdcWithKafkaCoordination(cdc -> cdc.withTramAndLocalPipeline(database));
    }

    private void startCdcWithKafkaCoordination(Function<EventuateCdcContainer, EventuateCdcContainer> pipelineConfigurer) {
        database.start();
        startAndApplyMigrations();
        EventuateCdcContainer cdc = pipelineConfigurer.apply(
                EventuateCdcContainer.makeFromDockerfile()
                        .withKafka(eventuateKafkaCluster.network, eventuateKafkaCluster.kafka)
                        .withKafkaLeadership())
                .dependsOn(zookeeper, kafka, database)
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("cdc:"));
        Startables.deepStart(cdc).join();
        cdc.stop();
    }

    private void startAndApplyMigrations() {

        Map<String, String> properties = new HashMap<>();
        DynamicPropertyRegistry registry = (name, valueSupplier) -> properties.put(name, valueSupplier.get().toString());

        PropertyProvidingContainer.startAndProvideProperties(registry, database);


        String jdbcUrl = properties.get("spring.datasource.url");

        // TODO Move/Refactor logic to eventuate-common-flyway to remove duplication

        DatabaseDriver driver = DatabaseDriver.fromJdbcUrl(jdbcUrl);
        String driverId = driver.getId();

        Flyway flyway = Flyway.configure()
            .dataSource(jdbcUrl, properties.get("spring.datasource.username"), properties.get("spring.datasource.password"))
            .locations("classpath:flyway/" + driverId)
            .baselineOnMigrate(true)
            .baselineVersion("0")
            .load();

        flyway.migrate();

        String suffix = "";
        ScriptExecutor scriptExecutor = new ScriptExecutor();

        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties.get("spring.datasource.username"), properties.get("spring.datasource.password"))) {
            SqlExecutor sqlExecutor = statement -> {
                try (PreparedStatement preparedStatement = connection.prepareStatement(statement)) {
                    preparedStatement.execute();
                }
            };

            try {
                connection.createStatement().executeQuery("select * from message");
            } catch (SQLException e) {
                scriptExecutor.executeScript(Collections.singletonMap("EVENTUATE_OUTBOX_SUFFIX", suffix),
                    "flyway-templates/" + driverId + "/3.create-message-table.sql", sqlExecutor);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

}