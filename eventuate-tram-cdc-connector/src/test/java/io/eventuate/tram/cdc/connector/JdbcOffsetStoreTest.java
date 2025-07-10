package io.eventuate.tram.cdc.connector;

import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.common.jdbc.EventuateSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Optional;
import java.util.UUID;

@SpringBootTest(classes = JdbcOffsetStoreTest.Config.class)
public class JdbcOffsetStoreTest {
  @Configuration
  @EnableAutoConfiguration
  public static class Config {
    @Bean
    public JdbcOffsetStore jdbcOffsetStore(JdbcTemplate jdbcTemplate, EventuateSchema eventuateSchema) {
      return new JdbcOffsetStore(UUID.randomUUID().toString(), jdbcTemplate, eventuateSchema);
    }

    @Bean
    public EventuateSchema eventuateSchema(@Value("${eventuate.database.schema:#{null}}") String eventuateDatabaseSchema) {
      return new EventuateSchema(eventuateDatabaseSchema);
    }
  }

  @Autowired
  private JdbcOffsetStore jdbcOffsetStore;

  @Test
  public void testOffsetSaving() {
    Assertions.assertEquals(Optional.empty(), jdbcOffsetStore.getLastBinlogFileOffset());

    BinlogFileOffset offset = new BinlogFileOffset("test_file", 1000);

    jdbcOffsetStore.save(offset);

    Assertions.assertEquals(Optional.of(offset), jdbcOffsetStore.getLastBinlogFileOffset());
  }
}
