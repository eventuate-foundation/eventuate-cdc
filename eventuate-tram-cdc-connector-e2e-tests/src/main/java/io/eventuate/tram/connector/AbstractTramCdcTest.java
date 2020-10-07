package io.eventuate.tram.connector;

import io.eventuate.cdc.e2e.common.AbstractEventuateCdcTest;
import io.eventuate.common.id.IdGenerator;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.sqldialect.SqlDialectSelector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.Collections;
import java.util.Map;

public abstract class AbstractTramCdcTest extends AbstractEventuateCdcTest {

  @Autowired
  private SqlDialectSelector sqlDialectSelector;

  @Value("spring.datasource.driver.class.name")
  private String driver;

  @Autowired
  private IdGenerator idGenerator;

  @Override
  protected String saveEvent(String eventData, String entityType, EventuateSchema eventuateSchema, boolean published) {
    return eventuateCommonJdbcOperations.insertIntoMessageTable(idGenerator,
            eventData,
            entityType,
            sqlDialectSelector.getDialect(driver).getCurrentTimeInMillisecondsExpression(),
            Collections.emptyMap(),
            eventuateSchema,
            published);
  }

  @Override
  protected String extractEventId(Map<String, Object> eventAsMap) {
    Map<String, Object> headers = (Map<String, Object>) eventAsMap.get("headers");

    return (String) headers.get("ID");
  }

  @Override
  protected String extractEventPayload(Map<String, Object> eventAsMap) {
    return (String) eventAsMap.get("payload");
  }
}
