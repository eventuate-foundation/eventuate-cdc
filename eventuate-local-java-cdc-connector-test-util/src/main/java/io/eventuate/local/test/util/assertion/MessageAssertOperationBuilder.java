package io.eventuate.local.test.util.assertion;

import io.eventuate.tram.cdc.connector.MessageWithDestination;
import org.junit.jupiter.api.Assertions;

import java.util.HashMap;
import java.util.Map;

public class MessageAssertOperationBuilder implements BinlogAssertOperationBuilder<MessageWithDestination> {
  private String id;
  private String destination;
  private String payload;
  private Map<String, String> headers;

  public static MessageAssertOperationBuilder assertion() {
    return new MessageAssertOperationBuilder();
  }

  @Override
  public BinlogAssertOperation<MessageWithDestination> build() {
    return message -> {
      if (id != null) {
        Assertions.assertEquals(id, message.getId());
      }

      if (destination != null) {
        Assertions.assertEquals(destination, message.getDestination());
      }

      if (payload != null) {
        Assertions.assertEquals(payload, message.getPayload());
      }

      if (headers != null) {
        headers.put("ID", message.getId());

        Assertions.assertEquals(headers, message.getHeaders());
      }
    };
  }

  public MessageAssertOperationBuilder withId(String id) {
    this.id = id;

    return this;
  }

  public MessageAssertOperationBuilder withDestination(String destination) {
    this.destination = destination;

    return this;
  }

  public MessageAssertOperationBuilder withPayload(String payload) {
    this.payload = payload;

    return this;
  }

  public MessageAssertOperationBuilder withHeaders(Map<String, String> headers) {
    this.headers = new HashMap<>(headers);

    return this;
  }
}
