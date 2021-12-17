package io.eventuate.local.testutil;

public class DefaultAndPostgresWalProfilesResolver extends DefaultProfilesResolver {
  public DefaultAndPostgresWalProfilesResolver() {
    super("PostgresWal", "postgres");
  }
}
