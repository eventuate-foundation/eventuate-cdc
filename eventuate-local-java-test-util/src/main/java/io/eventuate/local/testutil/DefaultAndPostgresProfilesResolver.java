package io.eventuate.local.testutil;

import java.util.Set;

public class DefaultAndPostgresProfilesResolver extends DefaultProfilesResolver {
  @Override
  public String[] resolve(Class<?> testClass) {
    Set<String> activeProfiles = getDefaultProfiles();

    activeProfiles.add("postgres");

    return convertProfilesToArray(activeProfiles);
  }
}
