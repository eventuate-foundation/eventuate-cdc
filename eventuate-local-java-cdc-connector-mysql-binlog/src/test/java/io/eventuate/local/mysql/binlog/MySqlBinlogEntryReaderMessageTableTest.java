package io.eventuate.local.mysql.binlog;

import io.eventuate.common.spring.id.IdGeneratorConfiguration;
import io.eventuate.local.test.util.AbstractBinlogEntryReaderMessageTableTest;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {MySqlBinlogEntryReaderMessageTableTestConfiguration.class, IdGeneratorConfiguration.class})
public class MySqlBinlogEntryReaderMessageTableTest extends AbstractBinlogEntryReaderMessageTableTest {
}
