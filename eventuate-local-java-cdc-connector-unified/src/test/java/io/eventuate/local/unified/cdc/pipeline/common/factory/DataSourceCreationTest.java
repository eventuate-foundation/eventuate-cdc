package io.eventuate.local.unified.cdc.pipeline.common.factory;

import com.zaxxer.hikari.HikariDataSource;
import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.local.common.ConnectionPoolConfigurationProperties;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineReaderProperties;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = DataSourceCreationTest.Config.class)
@TestPropertySource(properties = {"connection.pool.maximumPoolSize = 100", "connection.pool.poolName = testPool"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class DataSourceCreationTest {

  @EnableConfigurationProperties(ConnectionPoolConfigurationProperties.class)
  public static class Config {
  }

  @Autowired
  private ConnectionPoolConfigurationProperties connectionPoolConfigurationProperties;

  @Test
  public void testThatPropertiesApplied() {
    CommonCdcPipelineReaderFactory commonCdcPipelineReaderFactory = createCdcPipelineReaderFactory();
    HikariDataSource dataSource = (HikariDataSource) commonCdcPipelineReaderFactory.createDataSource(cdcPipelineReaderProperties());

    Assert.assertEquals(100, dataSource.getMaximumPoolSize());
    Assert.assertEquals("testPool", dataSource.getPoolName());
  }

  @Test
  public void testUnknownProperty() {
    Exception exception = null;
    try {
      connectionPoolConfigurationProperties.getPool().put("someUnknownProperty", "kg943=-5yjhgri[e");

      CommonCdcPipelineReaderFactory commonCdcPipelineReaderFactory = createCdcPipelineReaderFactory();
      commonCdcPipelineReaderFactory.createDataSource(cdcPipelineReaderProperties());

    } catch (Exception e) {
      exception = e;
    }

    Assert.assertNotNull(exception);
    Assert.assertEquals("Property someUnknownProperty does not exist on target class com.zaxxer.hikari.HikariConfig", exception.getMessage());
  }

  private CommonCdcPipelineReaderFactory createCdcPipelineReaderFactory() {
    return new CommonCdcPipelineReaderFactory(null, null, null, connectionPoolConfigurationProperties) {
      @Override
      public BinlogEntryReader create(CdcPipelineReaderProperties cdcPipelineReaderProperties) {
        return null;
      }

      @Override
      public boolean supports(String type) {
        return false;
      }

      @Override
      public Class propertyClass() {
        return null;
      }
    };
  }

  private CdcPipelineReaderProperties cdcPipelineReaderProperties() {
    return new CdcPipelineReaderProperties() {{
      setDataSourceUrl("jdbc:h2:mem:connectionPoolPropertiesTestDB");
      setDataSourceDriverClassName("org.h2.Driver");
      setDataSourceUserName("sa");
      setDataSourcePassword("");
    }};
  }
}