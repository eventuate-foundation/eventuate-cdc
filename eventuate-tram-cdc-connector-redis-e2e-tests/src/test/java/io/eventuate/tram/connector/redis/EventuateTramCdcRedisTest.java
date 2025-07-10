package io.eventuate.tram.connector.redis;

import com.google.common.collect.ImmutableSet;
import io.eventuate.common.spring.id.IdGeneratorConfiguration;
import io.eventuate.common.spring.jdbc.EventuateCommonJdbcOperationsConfiguration;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.messaging.partitionmanagement.CoordinatorFactory;
import io.eventuate.messaging.partitionmanagement.CoordinatorFactoryImpl;
import io.eventuate.messaging.redis.spring.common.CommonRedisConfiguration;
import io.eventuate.messaging.redis.spring.common.RedissonClients;
import io.eventuate.messaging.redis.spring.consumer.*;
import io.eventuate.messaging.redis.spring.leadership.RedisLeaderSelector;
import io.eventuate.tram.connector.AbstractTramCdcTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.function.Consumer;

@SpringBootTest(classes = {EventuateTramCdcRedisTest.Config.class})
public class EventuateTramCdcRedisTest extends AbstractTramCdcTest {

  @Configuration
  @EnableAutoConfiguration
  @Import({CommonRedisConfiguration.class, SqlDialectConfiguration.class, IdGeneratorConfiguration.class, EventuateCommonJdbcOperationsConfiguration.class})
  public static class Config {
  }

  @Autowired
  private RedisTemplate<String, String> redisTemplate;

  @Autowired
  private RedissonClients redissonClients;

  @Override
  protected void createConsumer(String channel, Consumer<String> consumer) throws Exception {
    int partitionCount = 1;

    CoordinatorFactory coordinatorFactory = new CoordinatorFactoryImpl(new RedisAssignmentManager(redisTemplate, 3600000),
            (groupId, memberId, assignmentUpdatedCallback) -> new RedisAssignmentListener(redisTemplate, groupId, memberId, 50, assignmentUpdatedCallback),
            (groupId, memberId, groupMembersUpdatedCallback) -> new RedisMemberGroupManager(redisTemplate, groupId, memberId,50, groupMembersUpdatedCallback),
            (lockId, leaderId, leaderSelectedCallback, leaderRemovedCallback) -> new RedisLeaderSelector(redissonClients, lockId, leaderId,10000, leaderSelectedCallback, leaderRemovedCallback),
            (groupId, memberId) -> new RedisGroupMember(redisTemplate, groupId, memberId, 1000),
            partitionCount);

    MessageConsumerRedisImpl messageConsumerRedis = new MessageConsumerRedisImpl(
            redisTemplate,
            coordinatorFactory,
            100,
            100);

    messageConsumerRedis.subscribe(subscriberId,
            ImmutableSet.of(channel),
            redisMessage -> consumer.accept(redisMessage.getPayload()));
  }
}
