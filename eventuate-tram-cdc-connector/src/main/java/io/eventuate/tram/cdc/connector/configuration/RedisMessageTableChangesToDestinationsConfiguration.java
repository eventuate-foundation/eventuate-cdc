package io.eventuate.tram.cdc.connector.configuration;

import io.eventuate.cdc.producer.wrappers.DataProducerFactory;
import io.eventuate.cdc.producer.wrappers.EventuateRedisDataProducerWrapper;
import io.eventuate.coordination.leadership.LeaderSelectorFactory;
import io.eventuate.local.common.PublishingFilter;
import io.eventuate.messaging.redis.spring.common.CommonRedisConfiguration;
import io.eventuate.messaging.redis.spring.common.RedisConfigurationProperties;
import io.eventuate.messaging.redis.spring.common.RedissonClients;
import io.eventuate.messaging.redis.spring.leadership.RedisLeaderSelector;
import io.eventuate.messaging.redis.spring.producer.EventuateRedisProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;

@Configuration
@Import(CommonRedisConfiguration.class)
@Profile("Redis")
public class RedisMessageTableChangesToDestinationsConfiguration {
  @Bean
  public PublishingFilter redisDuplicatePublishingDetector() {
    return (fileOffset, topic) -> true;
  }

  @Bean
  public DataProducerFactory redisDataProducerFactory(StringRedisTemplate redisTemplate,
                                                      RedisConfigurationProperties redisConfigurationProperties) {
    return () -> new EventuateRedisDataProducerWrapper(new EventuateRedisProducer(redisTemplate, redisConfigurationProperties.getPartitions()));
  }

  @Bean
  public LeaderSelectorFactory connectorLeaderSelectorFactory(RedissonClients redissonClients) {
    return (lockId, leaderId, leaderSelectedCallback, leaderRemovedCallback) ->
            new RedisLeaderSelector(redissonClients, lockId, leaderId, 10000, leaderSelectedCallback, leaderRemovedCallback);
  }
}
