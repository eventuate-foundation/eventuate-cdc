package io.eventuate.local.cdc.debezium.migration;

import io.eventuate.testutil.Eventually;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = MigrationE2ETestConfiguration.class)
@DirtiesContext
public class MigrationOldCdcPhaseE2ETest extends AbstractE2EMigrationTest {

  @Test
  public void send2EventsAndReceive1() throws InterruptedException, ExecutionException {
    for (int i = 0; i < 2; i++) {
      sendEvent();
    }

    Handler handler = new Handler() {
      private boolean received;
      private boolean secondEventFailed;

      @Override
      public void accept(ConsumerRecord<String, String> consumerRecord, BiConsumer<Void, Throwable> callback) {
        if (!received) {
          received = true;
          super.accept(consumerRecord, callback);
        } else {
          secondEventFailed = true;
          callback.accept(null, new IllegalStateException());
        }
      }

      @Override
      public void assertContainsEvent() throws InterruptedException {
        Eventually.eventually(() -> Assert.assertTrue(secondEventFailed));
        super.assertContainsEvent();
      }
    };

    subscribe(handler);
    handler.assertContainsEvent();
    Thread.sleep(61000);
  }
}
