package io.eventuate.cdc.testcontainers;

import io.eventuate.common.testcontainers.DatabaseContainerFactory;
import io.eventuate.common.testcontainers.EventuateDatabaseContainer;
import io.eventuate.common.testcontainers.EventuateZookeeperContainer;
import io.eventuate.messaging.kafka.testcontainers.EventuateKafkaCluster;
import io.eventuate.messaging.kafka.testcontainers.EventuateKafkaContainer;
import org.junit.Test;
import org.testcontainers.lifecycle.Startables;

public class EventuateCdcContainerTest {

    public static EventuateKafkaCluster eventuateKafkaCluster = new EventuateKafkaCluster("EventuateCdcTest");

    public static EventuateZookeeperContainer zookeeper = eventuateKafkaCluster.zookeeper;

    public static EventuateKafkaContainer kafka = eventuateKafkaCluster.kafka;

    public static EventuateDatabaseContainer<?> customerServiceDatabase =
            DatabaseContainerFactory.makeDatabaseContainer()
                    .withNetwork(eventuateKafkaCluster.network)
                    .withNetworkAliases("customer-service-db")
                    .withReuse(true)
            ;


    @Test
    public void shouldStart() {
        EventuateCdcContainer cdc = EventuateCdcContainer.makeFromDockerfile()
                .withKafkaCluster(eventuateKafkaCluster)
                .withTramPipeline(customerServiceDatabase)
                .dependsOn(zookeeper, kafka, customerServiceDatabase);
        Startables.deepStart(cdc).join();
        cdc.stop();
    }

    @Test
    public void shouldStartWithKafkaCoordination() {
        EventuateCdcContainer cdc = EventuateCdcContainer.makeFromDockerfile()
                .withKafka(eventuateKafkaCluster.network, eventuateKafkaCluster.kafka)
                .withKafkaLeadership()
                .withTramPipeline(customerServiceDatabase)
                .dependsOn(zookeeper, kafka, customerServiceDatabase);
        Startables.deepStart(cdc).join();
        cdc.stop();
    }

}