package io.eventuate.tram.connector;

import com.google.common.collect.ImmutableSet;
import io.eventuate.common.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.messaging.partitionmanagement.CoordinatorFactory;
import io.eventuate.messaging.partitionmanagement.CoordinatorFactoryImpl;
import io.eventuate.messaging.redis.common.CommonRedisConfiguration;
import io.eventuate.messaging.redis.common.RedissonClients;
import io.eventuate.messaging.redis.consumer.*;
import io.eventuate.messaging.redis.leadership.RedisLeaderSelector;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.function.Consumer;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {EventuateTramCdcRedisTest.Config.class})
public class EventuateTramCdcRedisTest extends AbstractTramCdcTest {

  @Configuration
  @EnableAutoConfiguration
  @Import({CommonRedisConfiguration.class, SqlDialectConfiguration.class})
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
