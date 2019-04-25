package io.eventuate.tram.connector;

import io.eventuate.javaclient.spring.jdbc.EventuateSchema;
import io.eventuate.testutil.Eventually;
import io.eventuate.tram.redis.common.CommonRedisConfiguration;
import io.lettuce.core.RedisCommandExecutionException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {EventuateTramCdcRedisTest.Config.class})
public class EventuateTramCdcRedisTest {

  @Import(CommonRedisConfiguration.class)
  @Configuration
  @EnableAutoConfiguration
  public static class Config {
  }

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Autowired
  private RedisTemplate<String, String> redisTemplate;

  private String subscriberId = UUID.randomUUID().toString();

  @Test
  public void insertToMessageTableAndWaitMessageInRedis() throws Exception {
    String data = UUID.randomUUID().toString();
    String topic = UUID.randomUUID().toString();

    BlockingQueue<String> blockingQueue = new LinkedBlockingDeque<>();

    saveEvent(data, topic, new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA));

    makeSureConsumerGroupExists(channelToRedisStream(topic, 0));
    makeSureConsumerGroupExists(channelToRedisStream(topic, 1));

    createConsumer(channelToRedisStream(topic, 0), blockingQueue::add);
    createConsumer(channelToRedisStream(topic, 1), blockingQueue::add);


    Eventually.eventually(120, 500, TimeUnit.MILLISECONDS, () -> {
      try {
        Assert.assertTrue(blockingQueue.poll(100, TimeUnit.MILLISECONDS).contains(data));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private void makeSureConsumerGroupExists(String channel) throws Exception{
    for (int i = 0; i < 10; i++) {
      try {
        redisTemplate.opsForStream().createGroup(channel, ReadOffset.from("0"), subscriberId);
        return;
      } catch (RedisSystemException e) {
        if (isKeyDoesNotExist(e)) {
          Thread.sleep(100);
          continue;
        } else if (isGroupExistsAlready(e)) {
          return;
        }
        throw e;
      }
    }
  }

  private boolean isKeyDoesNotExist(RedisSystemException e) {
    return isRedisCommandExceptionContainingMessage(e, "ERR The XGROUP subcommand requires the key to exist");
  }

  private boolean isGroupExistsAlready(RedisSystemException e) {
    return isRedisCommandExceptionContainingMessage(e, "Consumer Group name already exists");
  }

  private boolean isRedisCommandExceptionContainingMessage(RedisSystemException e, String expectedMessage) {
    String message = e.getCause().getMessage();

    return e.getCause() instanceof RedisCommandExecutionException &&
            message != null &&
            message.contains(expectedMessage);
  }

  private void createConsumer(String channel, Consumer<String> consumer) {
    new Thread(() -> {
      while(true) {
        getUnprocessedRecords(channel)
                .forEach(entries ->
                        entries.getValue().values().forEach(o -> consumer.accept(o.toString())));
      }
    }).start();
  }

  private void saveEvent(String eventData, String eventType, EventuateSchema eventuateSchema) {
    String table = eventuateSchema.qualifyTable("message");
    String id = UUID.randomUUID().toString();

    jdbcTemplate.update(String.format("insert into %s(id, destination, headers, payload, creation_time) values(?, ?, ?, ?, ?)",
            table),
            id,
            eventType,
            String.format("{\"ID\" : \"%s\"}", id),
            eventData,
            System.currentTimeMillis());
  }

  private List<MapRecord<String, Object, Object>> getUnprocessedRecords(String channel) {
    return getRecords(ReadOffset.from(">"), channel, StreamReadOptions.empty().block(Duration.ofMillis(100)));
  }

  private List<MapRecord<String, Object, Object>> getRecords(ReadOffset readOffset, String channel, StreamReadOptions options) {
    List<MapRecord<String, Object, Object>> records = redisTemplate
            .opsForStream()
            .read(org.springframework.data.redis.connection.stream.Consumer.from(subscriberId, subscriberId),
                    options, StreamOffset.create(channel, readOffset));

    return records;
  }

  private String channelToRedisStream(String topic, int partition) {
    return String.format("eventuate-tram:channel:%s-%s", topic, partition);
  }
}
