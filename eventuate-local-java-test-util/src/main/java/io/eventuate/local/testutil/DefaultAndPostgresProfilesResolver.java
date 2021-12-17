package io.eventuate.local.testutil;

public class DefaultAndPostgresProfilesResolver extends DefaultProfilesResolver {
  public DefaultAndPostgresProfilesResolver() {
    super("postgres");
  }
}
