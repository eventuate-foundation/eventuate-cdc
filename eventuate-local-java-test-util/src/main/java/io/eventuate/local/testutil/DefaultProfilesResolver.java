package io.eventuate.local.testutil;

import org.springframework.test.context.ActiveProfilesResolver;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class DefaultProfilesResolver implements ActiveProfilesResolver {
  @Override
  public String[] resolve(Class<?> testClass) {

    Set<String> activeProfiles = getDefaultProfiles();

    activeProfiles.add("postgres");
    activeProfiles.add("PostgresWal");

    return convertProfilesToArray(activeProfiles);
  }

  protected Set<String> getDefaultProfiles() {
    String springProfilesActive = System.getenv("SPRING_PROFILES_ACTIVE");

    if (springProfilesActive == null) return new HashSet<>();

    return new HashSet<>(Arrays.asList(springProfilesActive.split(",")));
  }

  protected String[] convertProfilesToArray(Set<String> profiles) {
    return profiles.toArray(new String[] {});
  }
}
