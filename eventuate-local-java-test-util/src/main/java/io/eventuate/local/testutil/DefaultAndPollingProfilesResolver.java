package io.eventuate.local.testutil;

public class DefaultAndPollingProfilesResolver extends DefaultProfilesResolver {

  public DefaultAndPollingProfilesResolver() {
    super("EventuatePolling");
  }
}
