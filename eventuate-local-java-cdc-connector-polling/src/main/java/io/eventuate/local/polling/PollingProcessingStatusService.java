package io.eventuate.local.polling;

import io.eventuate.common.jdbc.sqldialect.EventuateSqlDialect;
import io.eventuate.local.common.CdcProcessingStatus;
import io.eventuate.local.common.CdcProcessingStatusService;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class PollingProcessingStatusService implements CdcProcessingStatusService {
  private final JdbcTemplate jdbcTemplate;
  private final String publishedField;
  private final Set<String> tables = new CopyOnWriteArraySet<>();
  private final EventuateSqlDialect eventuateSqlDialect;

  public PollingProcessingStatusService(DataSource dataSource, String publishedField, EventuateSqlDialect eventuateSqlDialect) {
    jdbcTemplate = new JdbcTemplate(dataSource);
    this.publishedField = publishedField;
    this.eventuateSqlDialect = eventuateSqlDialect;
  }

  public void addTable(String table) {
    tables.add(table);
  }

  @Override
  public CdcProcessingStatus getCurrentStatus() {
    return new CdcProcessingStatus(-1, -1, isProcessingFinished());
  }

  @Override
  public void saveEndingOffsetOfLastProcessedEvent(long endingOffsetOfLastProcessedEvent) {
    throw new UnsupportedOperationException();
  }

  private boolean isProcessingFinished() {
    return tables
            .stream()
            .allMatch(table ->
                    jdbcTemplate.queryForObject(eventuateSqlDialect.addLimitToSql(String.format("select count(*) from %s where %s = 0",
                            table, publishedField), "1"), Long.class) == 0);
  }
}