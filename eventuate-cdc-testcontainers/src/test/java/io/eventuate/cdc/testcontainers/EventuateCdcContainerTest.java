package io.eventuate.cdc.testcontainers;

import io.eventuate.common.testcontainers.DatabaseContainerFactory;
import io.eventuate.common.testcontainers.EventuateDatabaseContainer;
import io.eventuate.common.testcontainers.EventuateZookeeperContainer;
import io.eventuate.messaging.kafka.testcontainers.EventuateKafkaCluster;
import io.eventuate.messaging.kafka.testcontainers.EventuateKafkaContainer;
import org.junit.ClassRule;
import org.junit.Test;

public class EventuateCdcContainerTest {

    public static EventuateKafkaCluster eventuateKafkaCluster = new EventuateKafkaCluster("CustomersAndOrdersE2ETest");

    @ClassRule
    public static EventuateZookeeperContainer zookeeper = eventuateKafkaCluster.zookeeper;

    @ClassRule
    public static EventuateKafkaContainer kafka = eventuateKafkaCluster.kafka;

    @ClassRule
    public static EventuateDatabaseContainer<?> customerServiceDatabase =
            DatabaseContainerFactory.makeDatabaseContainer()
                    .withNetwork(eventuateKafkaCluster.network)
                    .withNetworkAliases("customer-service-db");

    @ClassRule
    public static EventuateCdcContainer cdc = EventuateCdcContainer.makeFromDockerfile()
            .withKafkaCluster(eventuateKafkaCluster)
            .withTramPipeline(customerServiceDatabase);

    @Test
    public void shouldStart() {}

}