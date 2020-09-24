package io.eventuate.local.postgres.wal;

import io.eventuate.common.spring.id.IdGeneratorConfiguration;
import io.eventuate.local.test.util.AbstractBinlogEntryReaderMessageTableTest;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@ActiveProfiles("${SPRING_PROFILES_ACTIVE:PostgresWal,postgres}")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {PostgresWalBinlogEntryReaderMessageTableTestConfiguration.class,
        IdGeneratorConfiguration.class})
public class PostgresWalBinlogEntryReaderMessageTableTest extends AbstractBinlogEntryReaderMessageTableTest {
}
