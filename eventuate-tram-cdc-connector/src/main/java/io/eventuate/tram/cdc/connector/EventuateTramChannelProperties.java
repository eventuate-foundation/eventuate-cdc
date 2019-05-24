package io.eventuate.tram.cdc.connector;

import io.eventuate.messaging.activemq.common.ChannelType;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@ConfigurationProperties("eventuate.tram")
public class EventuateTramChannelProperties {
  Map<String, ChannelType> channelTypes;

  public Map<String, ChannelType> getChannelTypes() {
    return channelTypes;
  }

  public void setChannelTypes(Map<String, ChannelType> channelTypes) {
    this.channelTypes = channelTypes;
  }
}