package io.eventuate.tram.connector;

import io.eventuate.sql.dialect.SqlDialectConfiguration;
import io.eventuate.tram.redis.common.CommonRedisConfiguration;
import io.lettuce.core.RedisCommandExecutionException;
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
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.time.Duration;
import java.util.List;
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

  private void makeSureConsumerGroupExists(String channel) throws Exception{
    for (int i = 0; i < 100; i++) {
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

  @Override
  protected void createConsumer(String channel, Consumer<String> consumer) throws Exception{
    String redisStream = channelToRedisStream(channel, 0);

    makeSureConsumerGroupExists(redisStream);

    new Thread(() -> {
      while(true) {
        getUnprocessedRecords(redisStream)
                .forEach(entries ->
                        entries.getValue().values().forEach(o -> consumer.accept(o.toString())));
      }
    }).start();
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
