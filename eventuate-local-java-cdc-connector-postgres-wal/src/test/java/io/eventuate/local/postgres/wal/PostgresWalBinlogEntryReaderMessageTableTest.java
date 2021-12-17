package io.eventuate.local.postgres.wal;

import io.eventuate.common.spring.id.IdGeneratorConfiguration;
import io.eventuate.local.test.util.AbstractBinlogEntryReaderMessageTableTest;
import io.eventuate.local.testutil.DefaultAndPostgresWalProfilesResolver;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@ActiveProfiles(resolver = DefaultAndPostgresWalProfilesResolver.class)
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = PostgresWalBinlogEntryReaderMessageTableTestConfiguration.class)
public class PostgresWalBinlogEntryReaderMessageTableTest extends AbstractBinlogEntryReaderMessageTableTest {
}
