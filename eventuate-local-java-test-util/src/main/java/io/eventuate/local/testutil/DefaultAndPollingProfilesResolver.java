package io.eventuate.local.testutil;

import java.util.Set;

public class DefaultAndPollingProfilesResolver extends DefaultProfilesResolver {
  @Override
  public String[] resolve(Class<?> testClass) {
    Set<String> activeProfiles = getDefaultProfiles();

    activeProfiles.add("EventuatePolling");

    return convertProfilesToArray(activeProfiles);
  }
}
