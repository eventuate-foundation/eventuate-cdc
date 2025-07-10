package io.eventuate.local.mysql.binlog;

import io.eventuate.local.test.util.AbstractMessageCleanerTest;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = AbstractMessageCleanerTest.Config.class)
public class MySqlMessageCleanerTest extends AbstractMessageCleanerTest {

}
