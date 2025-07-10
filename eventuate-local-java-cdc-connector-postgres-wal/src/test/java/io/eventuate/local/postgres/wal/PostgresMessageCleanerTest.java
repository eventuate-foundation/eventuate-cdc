package io.eventuate.local.postgres.wal;

import io.eventuate.local.test.util.AbstractMessageCleanerTest;
import io.eventuate.local.testutil.DefaultAndPostgresProfilesResolver;
import io.eventuate.local.testutil.DefaultAndPostgresWalProfilesResolver;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(classes = AbstractMessageCleanerTest.Config.class)
@ActiveProfiles(resolver = DefaultAndPostgresProfilesResolver.class)
public class PostgresMessageCleanerTest extends AbstractMessageCleanerTest {

}
