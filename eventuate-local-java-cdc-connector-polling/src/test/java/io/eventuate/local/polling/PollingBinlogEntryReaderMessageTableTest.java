package io.eventuate.local.polling;

import io.eventuate.common.spring.id.IdGeneratorConfiguration;
import io.eventuate.local.test.util.AbstractBinlogEntryReaderMessageTableTest;
import io.eventuate.local.testutil.DefaultAndPollingProfilesResolver;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(resolver = DefaultAndPollingProfilesResolver.class)
@SpringBootTest(classes = PollingBinlogEntryReaderMessageTableTestConfiguration.class)
public class PollingBinlogEntryReaderMessageTableTest extends AbstractBinlogEntryReaderMessageTableTest {
}
