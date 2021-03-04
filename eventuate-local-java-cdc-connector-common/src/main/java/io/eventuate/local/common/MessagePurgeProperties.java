package io.eventuate.local.common;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

import java.util.concurrent.TimeUnit;

public class MessagePurgeProperties {
  private boolean messagesEnabled = false;
  private int messagesMaxAgeInSeconds = (int) TimeUnit.DAYS.toSeconds(2);
  private boolean receivedMessagesEnabled = false;
  private int receivedMessagesMaxAgeInSeconds = (int)TimeUnit.DAYS.toSeconds(2);

  private int intervalInSeconds = (int)TimeUnit.MINUTES.toSeconds(1);

  public boolean isMessagesEnabled() {
    return messagesEnabled;
  }

  public void setMessagesEnabled(boolean messagesEnabled) {
    this.messagesEnabled = messagesEnabled;
  }

  public int getMessagesMaxAgeInSeconds() {
    return messagesMaxAgeInSeconds;
  }

  public void setMessagesMaxAgeInSeconds(int messagesMaxAgeInSeconds) {
    this.messagesMaxAgeInSeconds = messagesMaxAgeInSeconds;
  }

  public boolean isReceivedMessagesEnabled() {
    return receivedMessagesEnabled;
  }

  public void setReceivedMessagesEnabled(boolean receivedMessagesEnabled) {
    this.receivedMessagesEnabled = receivedMessagesEnabled;
  }

  public int getReceivedMessagesMaxAgeInSeconds() {
    return receivedMessagesMaxAgeInSeconds;
  }

  public void setReceivedMessagesMaxAgeInSeconds(int receivedMessagesMaxAgeInSeconds) {
    this.receivedMessagesMaxAgeInSeconds = receivedMessagesMaxAgeInSeconds;
  }

  public int getIntervalInSeconds() {
    return intervalInSeconds;
  }

  public void setIntervalInSeconds(int intervalInSeconds) {
    this.intervalInSeconds = intervalInSeconds;
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }
}
