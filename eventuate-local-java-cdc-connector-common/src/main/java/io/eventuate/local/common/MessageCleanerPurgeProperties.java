package io.eventuate.local.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.springframework.util.Assert;

import java.util.concurrent.TimeUnit;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MessageCleanerPurgeProperties{
  private boolean purgeMessagesEnabled = false;
  private int purgeMessagesMaxAgeInSeconds = (int) TimeUnit.DAYS.toSeconds(2);
  private boolean purgeReceivedMessagesEnabled = false;
  private int purgeReceivedMessagesMaxAgeInSeconds = (int)TimeUnit.DAYS.toSeconds(2);

  private int purgeIntervalInSeconds = (int)TimeUnit.MINUTES.toSeconds(1);

  public boolean isPurgeMessagesEnabled() {
    return purgeMessagesEnabled;
  }

  public void setPurgeMessagesEnabled(boolean purgeMessagesEnabled) {
    this.purgeMessagesEnabled = purgeMessagesEnabled;
  }

  public int getPurgeMessagesMaxAgeInSeconds() {
    return purgeMessagesMaxAgeInSeconds;
  }

  public void setPurgeMessagesMaxAgeInSeconds(int purgeMessagesMaxAgeInSeconds) {
    this.purgeMessagesMaxAgeInSeconds = purgeMessagesMaxAgeInSeconds;
  }

  public boolean isPurgeReceivedMessagesEnabled() {
    return purgeReceivedMessagesEnabled;
  }

  public void setPurgeReceivedMessagesEnabled(boolean purgeReceivedMessagesEnabled) {
    this.purgeReceivedMessagesEnabled = purgeReceivedMessagesEnabled;
  }

  public int getPurgeReceivedMessagesMaxAgeInSeconds() {
    return purgeReceivedMessagesMaxAgeInSeconds;
  }

  public void setPurgeReceivedMessagesMaxAgeInSeconds(int purgeReceivedMessagesMaxAgeInSeconds) {
    this.purgeReceivedMessagesMaxAgeInSeconds = purgeReceivedMessagesMaxAgeInSeconds;
  }

  public int getPurgeIntervalInSeconds() {
    return purgeIntervalInSeconds;
  }

  public void setPurgeIntervalInSeconds(int purgeIntervalInSeconds) {
    this.purgeIntervalInSeconds = purgeIntervalInSeconds;
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }
}
