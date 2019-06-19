package io.eventuate.local.mysql.binlog;

import io.eventuate.local.testutil.CustomDBCreator;
import io.eventuate.local.testutil.CustomDBTestConfiguration;
import io.eventuate.local.testutil.SqlScriptEditor;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = CustomDBTestConfiguration.class)
public class ColumnOrderExtractorCustomDBTest extends AbstractColumnOrderExtractorTest {
  @Autowired
  private CustomDBCreator customDBCreator;

  @Autowired
  private SqlScriptEditor eventuateLocalCustomDBSqlEditor;

  private JdbcTemplate jdbcTemplate;

  private String monitoringSchemaSql;

  @Before
  public void createCustomDBAndDropDefaultMonitoringTable() {
    customDBCreator.create(eventuateLocalCustomDBSqlEditor);

    jdbcTemplate = new JdbcTemplate(dataSource);
    List<Map<String, Object>> sql = jdbcTemplate.queryForList("SHOW CREATE TABLE cdc_monitoring");
    monitoringSchemaSql = sql.get(0).get("Create Table").toString();
    jdbcTemplate.execute("drop table cdc_monitoring");
  }

  @After
  public void restoreDefaultMonitoringTable() {
    jdbcTemplate.execute(monitoringSchemaSql);
  }

}
