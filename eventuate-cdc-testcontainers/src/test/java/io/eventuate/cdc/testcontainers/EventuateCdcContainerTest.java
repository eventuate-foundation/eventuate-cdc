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
        database.start();
        startAndApplyMigrations();
        EventuateCdcContainer cdc = EventuateCdcContainer.makeFromDockerfile()
                .withKafka(eventuateKafkaCluster.network, eventuateKafkaCluster.kafka)
                .withKafkaLeadership()
                .withTramPipeline(database)
                .dependsOn(zookeeper, kafka, database)
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("cdc:"))
            ;
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