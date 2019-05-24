package io.eventuate.tram.cdc.connector;

import com.google.common.collect.ImmutableMap;
import io.eventuate.common.eventuate.local.BinLogEvent;
import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.javaclient.commonimpl.JSonMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MessageWithDestination implements BinLogEvent {
  private final String destination;
  private String payload;
  private Map<String, String> headers;
  private BinlogFileOffset binlogFileOffset;

  public MessageWithDestination(String destination,
                                String payload,
                                Map<String, String> headers,
                                BinlogFileOffset binlogFileOffset) {

    this.destination = destination;
    this.payload = payload;
    this.headers = headers;
    this.binlogFileOffset = binlogFileOffset;
  }

  public String getDestination() {
    return destination;
  }

  public Optional<BinlogFileOffset> getBinlogFileOffset() {
    return Optional.ofNullable(binlogFileOffset);
  }

  public String getPayload() {
    return payload;
  }

  public Optional<String> getPartitionId() {
    return getHeader("PARTITION_ID");
  }

  public Optional<String> getHeader(String name) {
    return Optional.ofNullable(headers.get(name));
  }

  public String getRequiredHeader(String name) {
    String s = headers.get(name);
    if (s == null)
      throw new RuntimeException("No such header: " + name + " in this message " + this);
    else
      return s;
  }

  public boolean hasHeader(String name) {
    return headers.containsKey(name);
  }

  public String getId() {
    return getRequiredHeader("ID");
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public void setPayload(String payload) {
    this.payload = payload;
  }

  public void setHeaders(Map<String, String> headers) {
    this.headers = headers;
  }

  public void setHeader(String name, String value) {
    if (headers == null)
      headers = new HashMap<>();
    headers.put(name, value);
  }

  public void removeHeader(String key) {
    headers.remove(key);
  }

  public String toJson() {
    return JSonMapper.toJson(ImmutableMap.of("payload", getPayload(), "headers", getHeaders()));
  }
}
