package io.eventuate.local.mysql.binlog;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.*;
import io.eventuate.local.test.util.SourceTableNameSupplier;
import io.eventuate.local.test.util.TestHelper;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.eventuate.util.test.async.Eventually;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.sql.DataSource;
import java.io.*;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {PerformanceTest.Config.class})
@EnableConfigurationProperties({EventuateKafkaProducerConfigurationProperties.class,
        EventuateKafkaConsumerConfigurationProperties.class})
public class PerformanceTest {

  @Configuration
  @EnableAutoConfiguration
  @Import({MySqlBinlogCdcIntegrationTestConfiguration.class, OffsetStoreMockConfiguration.class})
  public static class Config {
    @Bean
    public MySqlBinaryLogClientForPerformanceTesting mySqlBinaryLogClientPerformanceTester(@Value("${spring.datasource.url}") String dataSourceURL,
                                                                                           DataSource dataSource) {

      return new MySqlBinaryLogClientForPerformanceTesting(
              "root", "rootpassword", dataSourceURL, dataSource, System.nanoTime(), 3000);
    }

  }

  @Autowired
  private MySqlBinaryLogClientForPerformanceTesting mySqlBinaryLogClientForPerformanceTesting;

  @Autowired
  private TestHelper testHelper;

  @Autowired
  private SourceTableNameSupplier sourceTableNameSupplier;

  @Autowired
  private CdcDataPublisher<PublishedEvent> cdcDataPublisher;

  @Test
  public void test() throws Exception {
    cdcDataPublisher.start();

    final int nEvents = 1000;

    for (int i = 0; i < nEvents; i++) testHelper.saveEvent(testHelper.generateTestCreatedEvent());

    mySqlBinaryLogClientForPerformanceTesting.addBinlogEntryHandler(new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA),
            sourceTableNameSupplier.getSourceTableName(),
            new BinlogEntryToPublishedEventConverter(),
            cdcDataPublisher);

    testHelper.runInSeparateThread(mySqlBinaryLogClientForPerformanceTesting::start);

    Eventually.eventually(() -> {
      Assert.assertEquals(nEvents, mySqlBinaryLogClientForPerformanceTesting.measurements.size());

      System.out.println("--------------");
      System.out.println("--------------");
      System.out.println("--------------");
      mySqlBinaryLogClientForPerformanceTesting.measurements.stream().max(Double::compareTo).ifPresent(m -> System.out.println("max: " + m));
      mySqlBinaryLogClientForPerformanceTesting.measurements.stream().min(Double::compareTo).ifPresent(m -> System.out.println("min: " + m));
      mySqlBinaryLogClientForPerformanceTesting.measurements.stream().reduce((a, b) -> a + b).ifPresent(s -> {
        System.out.println("sum: " + s);
        System.out.println("average: " + s/((double)nEvents));
      });

      try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("performance.csv")));) {
        for (int i = 0; i < mySqlBinaryLogClientForPerformanceTesting.measurements.size(); i++) {
          bw.append(String.format("%s", mySqlBinaryLogClientForPerformanceTesting.measurements.get(i)));
          bw.newLine();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      System.out.println("--------------");
      System.out.println("--------------");
      System.out.println("--------------");
    });
  }

}
