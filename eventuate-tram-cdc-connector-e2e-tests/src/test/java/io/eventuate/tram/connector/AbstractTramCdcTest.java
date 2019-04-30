package io.eventuate.tram.connector;

import io.eventuate.cdc.e2e.common.AbstractEventuateCdcTest;
import io.eventuate.javaclient.spring.jdbc.EventuateSchema;
import io.eventuate.sql.dialect.SqlDialectSelector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

public abstract class AbstractTramCdcTest extends AbstractEventuateCdcTest {

  @Autowired
  private SqlDialectSelector sqlDialectSelector;

  @Value("spring.datasource.driver.class.name")
  private String driver;

  @Override
  protected void saveEvent(String eventData, String eventType, EventuateSchema eventuateSchema) {
    eventuateCommonJdbcOperations.insertIntoMessageTable(generateId(),
            eventData,
            eventType,
            sqlDialectSelector.getDialect(driver).getCurrentTimeInMillisecondsExpression(),
            eventuateSchema);
  }
}
