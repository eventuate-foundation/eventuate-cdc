package io.eventuate.tram.connector;

import io.eventuate.cdc.e2e.common.AbstractEventuateCdcTest;
import io.eventuate.common.id.IdGenerator;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.sqldialect.SqlDialectSelector;
import io.eventuate.common.json.mapper.JSonMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

public abstract class AbstractTramCdcTest extends AbstractEventuateCdcTest {

  @Autowired
  private SqlDialectSelector sqlDialectSelector;

  @Value("spring.datasource.driver.class.name")
  private String driver;

  @Autowired
  private IdGenerator idGenerator;

  @Override
  protected String saveEvent(String eventData, String entityType, EventuateSchema eventuateSchema) {
    return eventuateCommonJdbcOperations.insertIntoMessageTable(idGenerator,
            eventData,
            entityType,
            sqlDialectSelector.getDialect(driver).getCurrentTimeInMillisecondsExpression(),
            Collections.emptyMap(),
            eventuateSchema);
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
