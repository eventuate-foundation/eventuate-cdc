package io.eventuate.local.mysql.binlog;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.EventuateConfigurationProperties;
import io.eventuate.local.test.util.AbstractCdcIntegrationEventsTest;
import io.eventuate.local.test.util.EventInfo;
import io.eventuate.local.test.util.assertion.BinlogAssertion;
import io.eventuate.local.testutil.CustomDBCreator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static io.eventuate.local.test.util.assertion.EventAssertOperationBuilder.fromEventInfo;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {MySqlBinlogCdcIntegrationTestConfiguration.class, OffsetStoreMockConfiguration.class})
public abstract class AbstractMySqlBinlogCdcIntegrationEventsTest extends AbstractCdcIntegrationEventsTest {

  @Value("${spring.datasource.url}")
  private String dataSourceUrl;

  @Autowired
  private EventuateConfigurationProperties eventuateConfigurationProperties;

  private String dataFile = "../scripts/initialize-mysql.sql";

  @Value("${spring.datasource.driver.class.name}")
  private String driverClassName;

  @Test
  public void shouldGetEventsFromOnlyEventuateSchema() {

    String customSchemaName = "custom" + System.currentTimeMillis();

    createCustomSchema(customSchemaName);

    EventInfo eventInfoFromCustomSchema = insertEventIntoCustomSchema(customSchemaName);

    BinlogAssertion<PublishedEvent> eventAssertion = testHelper.prepareBinlogEntryHandlerEventAssertion(binlogEntryReader);
    testHelper.runInSeparateThread(binlogEntryReader::start);

    EventInfo eventInfo = testHelper.saveRandomEvent();

    eventAssertion.assertEventReceived(fromEventInfo(eventInfo).excludeId(eventInfoFromCustomSchema.getEventId()).build());

    binlogEntryReader.stop();
  }

  private EventInfo insertEventIntoCustomSchema(String otherSchemaName) {
    return testHelper.saveEvent("Other", testHelper.getTestCreatedEventType(), testHelper.generateId(), new EventuateSchema(otherSchemaName));
  }

  private void createCustomSchema(String otherSchemaName) {
    CustomDBCreator dbCreator = new CustomDBCreator(dataFile, dataSourceUrl, driverClassName, eventuateConfigurationProperties.getDbUserName(), eventuateConfigurationProperties.getDbPassword());
    dbCreator.create(sqlList -> {
      sqlList.set(0, sqlList.get(0).replace("create database", "create database if not exists"));
      for (int i = 0; i < 3; i++) sqlList.set(i, sqlList.get(i).replace("eventuate", otherSchemaName));
      return sqlList;
    });
  }
}
