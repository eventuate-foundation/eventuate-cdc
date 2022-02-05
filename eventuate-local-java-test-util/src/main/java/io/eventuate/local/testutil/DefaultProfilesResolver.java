package io.eventuate.local.testutil;

import org.springframework.test.context.ActiveProfilesResolver;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;

public class DefaultProfilesResolver implements ActiveProfilesResolver {
  private List<String> additionalProfiles;

  public DefaultProfilesResolver(String... additionalProfiles) {
    this.additionalProfiles = asList(additionalProfiles);
  }

  @Override
  public String[] resolve(Class<?> testClass) {
    Set<String> activeProfiles = getDefaultProfiles();

    activeProfiles.addAll(this.additionalProfiles);

    return convertProfilesToArray(activeProfiles);
  }

  protected Set<String> getDefaultProfiles() {
    String springProfilesActive = System.getenv("SPRING_PROFILES_ACTIVE");

    if (springProfilesActive == null) return new HashSet<>();

    return new HashSet<>(asList(springProfilesActive.split(",")));
  }

  protected String[] convertProfilesToArray(Set<String> profiles) {
    return profiles.toArray(new String[] {});
  }
}
