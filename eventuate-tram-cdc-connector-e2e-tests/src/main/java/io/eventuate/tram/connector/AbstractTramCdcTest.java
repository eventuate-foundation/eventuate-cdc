package io.eventuate.tram.connector;

import io.eventuate.cdc.e2e.common.AbstractEventuateCdcTest;
import io.eventuate.common.id.IdGenerator;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.sqldialect.SqlDialectSelector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.Collections;

public abstract class AbstractTramCdcTest extends AbstractEventuateCdcTest {

  @Autowired
  private SqlDialectSelector sqlDialectSelector;

  @Value("spring.datasource.driver.class.name")
  private String driver;

  @Autowired
  private IdGenerator idGenerator;

  @Override
  protected void saveEvent(String eventData, String entityType, EventuateSchema eventuateSchema) {
    eventuateCommonJdbcOperations.insertIntoMessageTable(idGenerator,
            eventData,
            entityType,
            sqlDialectSelector.getDialect(driver).getCurrentTimeInMillisecondsExpression(),
            Collections.emptyMap(),
            eventuateSchema);
  }
}
