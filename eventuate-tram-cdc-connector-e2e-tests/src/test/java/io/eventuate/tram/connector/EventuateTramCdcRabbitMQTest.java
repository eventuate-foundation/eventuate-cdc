package io.eventuate.tram.connector;

import com.google.common.collect.ImmutableSet;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.coordination.leadership.zookeeper.ZkLeaderSelector;
import io.eventuate.messaging.partitionmanagement.CoordinatorFactory;
import io.eventuate.messaging.partitionmanagement.CoordinatorFactoryImpl;
import io.eventuate.messaging.rabbitmq.consumer.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.function.Consumer;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {EventuateTramCdcRabbitMQTest.Config.class})
public class EventuateTramCdcRabbitMQTest extends AbstractTramCdcTest {

  @Configuration
  @EnableAutoConfiguration
  @Import(SqlDialectConfiguration.class)
  public static class Config {
  }

  @Value("${rabbitmq.url}")
  private String rabbitmqURL;

  @Value("${eventuatelocal.zookeeper.connection.string}")
  private String zkUrl;

  @Override
  protected void createConsumer(String channelName, Consumer<String> consumer) throws Exception {
    int partitionCount = 1;

    CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(zkUrl, new ExponentialBackoffRetry(1000, 5));
    curatorFramework.start();

    CoordinatorFactory coordinatorFactory = new CoordinatorFactoryImpl(new ZkAssignmentManager(curatorFramework),
            (groupId, memberId, assignmentUpdatedCallback) -> new ZkAssignmentListener(curatorFramework, groupId, memberId, assignmentUpdatedCallback),
            (groupId, memberId, groupMembersUpdatedCallback) -> new ZkMemberGroupManager(curatorFramework, groupId, memberId, groupMembersUpdatedCallback),
            (lockId, leaderId, leaderSelectedCallback, leaderRemovedCallback) -> new ZkLeaderSelector(curatorFramework, lockId, leaderId, leaderSelectedCallback, leaderRemovedCallback),
            (groupId, memberId) -> new ZkGroupMember(curatorFramework, groupId, memberId),
            partitionCount);

    MessageConsumerRabbitMQImpl messageConsumerRabbitMQ = new MessageConsumerRabbitMQImpl(coordinatorFactory,
            rabbitmqURL,
            partitionCount);

    messageConsumerRabbitMQ.subscribe(subscriberId,
            ImmutableSet.of(channelName),
            rabbitMQMessage -> consumer.accept(rabbitMQMessage.getPayload()));
  }
}
