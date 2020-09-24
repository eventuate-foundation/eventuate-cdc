package io.eventuate.local.test.util;

import com.google.common.collect.ImmutableMap;
import io.eventuate.common.id.IdGenerator;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.exception.EventuateLocalPublishingException;
import io.eventuate.tram.cdc.connector.BinlogEntryToMessageConverter;
import io.eventuate.tram.cdc.connector.MessageWithDestination;
import io.eventuate.util.test.async.Eventually;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class AbstractBinlogEntryReaderMessageTableTest {

  @Autowired
  private TestHelper testHelper;

  @Autowired
  private EventuateSchema eventuateSchema;

  @Autowired
  private BinlogEntryReader binlogEntryReader;

  @Autowired
  private IdGenerator idGenerator;

  @Test
  public void testMessageHandled() {

    ConcurrentLinkedQueue<MessageWithDestination> messages = new ConcurrentLinkedQueue<>();

    binlogEntryReader.addBinlogEntryHandler(eventuateSchema,
            "message",
            new BinlogEntryToMessageConverter(idGenerator), new CdcDataPublisher<MessageWithDestination>(null, null, null, null) {
              @Override
              public CompletableFuture<?> sendMessage(MessageWithDestination messageWithDestination)
                      throws EventuateLocalPublishingException {
                messages.add(messageWithDestination);
                return CompletableFuture.completedFuture(null);
              }
            });

    String payload = "payload-" + testHelper.generateId();
    String rawPayload = "\"" + payload + "\"";
    String destination = "destination-" + testHelper.generateId();
    Map<String, String> headers = ImmutableMap.of("key", "value");

    String messageId = testHelper.saveMessage(idGenerator, rawPayload, destination, "0", headers, eventuateSchema);

    testHelper.runInSeparateThread(binlogEntryReader::start);

    Eventually.eventually(() -> {
      MessageWithDestination message = messages.poll();

      Assert.assertNotNull(message);
      Assert.assertTrue(message.getPayload().contains(payload));
      Assert.assertEquals(messageId, message.getHeader("ID").get());
      message.removeHeader("ID");
      Assert.assertEquals(headers, message.getHeaders());
    });

    binlogEntryReader.stop();
  }
}
