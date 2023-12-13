package io.eventuate.tram.cdc.connector.configuration;


import io.eventuate.coordination.leadership.LeaderSelectorFactory;
import io.eventuate.coordination.leadership.zookeeper.KafkaLeaderSelector;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("KafkaLeadership")
public class KafkaLeadershipConfiguration {


    @Bean
    public LeaderSelectorFactory connectorLeaderSelectorFactory(EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties, KafkaConsumerFactory kafkaConsumerFactory, EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties ) {
        return (lockId, leaderId, leaderSelectedCallback, leaderRemovedCallback) ->
                new KafkaLeaderSelector(lockId,
                        leaderId,
                        leaderSelectedCallback,
                        leaderRemovedCallback,
                        eventuateKafkaConfigurationProperties.getBootstrapServers(), eventuateKafkaConsumerConfigurationProperties.getProperties(),
                        kafkaConsumerFactory);
    }

}
