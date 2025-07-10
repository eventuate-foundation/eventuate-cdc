package io.eventuate.local.mysql.binlog;

import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = {MySqlBinlogCdcIntegrationTestConfiguration.class, OffsetStoreMockConfiguration.class})
public class MySQLCdcProcessorEventsTest extends AbstractMySQLCdcProcessorEventsTest {
}
