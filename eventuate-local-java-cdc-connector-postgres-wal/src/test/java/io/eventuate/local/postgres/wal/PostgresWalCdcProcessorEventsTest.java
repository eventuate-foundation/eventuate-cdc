package io.eventuate.local.postgres.wal;

import io.eventuate.local.test.util.CdcProcessorEventsTest;
import io.eventuate.local.testutil.DefaultAndPostgresWalProfilesResolver;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(resolver = DefaultAndPostgresWalProfilesResolver.class)
@SpringBootTest(classes = PostgresWalCdcIntegrationTestConfiguration.class)
public class PostgresWalCdcProcessorEventsTest extends CdcProcessorEventsTest {
}
