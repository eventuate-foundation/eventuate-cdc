package io.eventuate.tram.connector;

import com.google.common.collect.ImmutableMap;
import io.eventuate.cdc.e2e.common.AbstractEventuateCdcTest;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.sqldialect.SqlDialectSelector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

public abstract class AbstractTramCdcTest extends AbstractEventuateCdcTest {

  @Autowired
  private SqlDialectSelector sqlDialectSelector;

  @Value("${spring.datasource.driver.class.name}")
  private String driver;

  @Override
  protected void saveEvent(String eventData, String entityType, EventuateSchema eventuateSchema) {
    String id = generateId();

    eventuateCommonJdbcOperations.insertIntoMessageTable(id,
            eventData,
            entityType,
            sqlDialectSelector.getDialect(driver).getCurrentTimeInMillisecondsExpression(),
            ImmutableMap.of("ID", id),
            eventuateSchema);
  }
}
