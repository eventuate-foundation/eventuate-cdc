package io.eventuate.local.mysql.binlog;

import io.eventuate.local.testutil.CustomDBCreator;
import io.eventuate.local.testutil.CustomDBTestConfiguration;
import io.eventuate.local.testutil.SqlScriptEditor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

@SpringBootTest(classes = CustomDBTestConfiguration.class)
public class ColumnOrderExtractorCustomDBTest extends AbstractColumnOrderExtractorTest {
  @Autowired
  private CustomDBCreator customDBCreator;

  @Autowired
  private SqlScriptEditor eventuateLocalCustomDBSqlEditor;

  private JdbcTemplate jdbcTemplate;

  private String monitoringSchemaSql;

  @BeforeEach
  public void createCustomDBAndDropDefaultMonitoringTable() {
    customDBCreator.create(eventuateLocalCustomDBSqlEditor);

    jdbcTemplate = new JdbcTemplate(dataSource);
    List<Map<String, Object>> sql = jdbcTemplate.queryForList("SHOW CREATE TABLE cdc_monitoring");
    monitoringSchemaSql = sql.get(0).get("Create Table").toString();
    jdbcTemplate.execute("drop table cdc_monitoring");
  }

  @AfterEach
  public void restoreDefaultMonitoringTable() {
    jdbcTemplate.execute(monitoringSchemaSql);
  }

}
