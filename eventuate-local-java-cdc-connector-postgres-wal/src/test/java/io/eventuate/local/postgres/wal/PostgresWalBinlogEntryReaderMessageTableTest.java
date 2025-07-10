package io.eventuate.local.postgres.wal;

import io.eventuate.common.spring.id.IdGeneratorConfiguration;
import io.eventuate.local.test.util.AbstractBinlogEntryReaderMessageTableTest;
import io.eventuate.local.testutil.DefaultAndPostgresWalProfilesResolver;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(resolver = DefaultAndPostgresWalProfilesResolver.class)
@SpringBootTest(classes = PostgresWalBinlogEntryReaderMessageTableTestConfiguration.class)
public class PostgresWalBinlogEntryReaderMessageTableTest extends AbstractBinlogEntryReaderMessageTableTest {
}
