package io.eventuate.tram.cdc.connector;

import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.common.json.mapper.JSonMapper;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.db.log.common.OffsetStore;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Optional;

public class JdbcOffsetStore implements OffsetStore {
  private JdbcTemplate jdbcTemplate;
  private EventuateSchema eventuateSchema;
  private String clientName;
  private String tableName;

  public JdbcOffsetStore(String clientName, JdbcTemplate jdbcTemplate, EventuateSchema eventuateSchema) {
    this.clientName = clientName;
    this.jdbcTemplate = jdbcTemplate;
    this.eventuateSchema = eventuateSchema;

    init();
  }

  private void init() {
    tableName = eventuateSchema.qualifyTable("offset_store");

    String selectAllByClientNameQuery = "select * from %s where client_name = ?".formatted(tableName);

    if (jdbcTemplate.queryForList(selectAllByClientNameQuery, clientName).isEmpty()) {

      String insertNullOffsetForClientNameQuery =
              "insert into %s (client_name, serialized_offset) VALUES (?, NULL)".formatted(
                      tableName);

      jdbcTemplate.update(insertNullOffsetForClientNameQuery, clientName);
    }
  }

  @Override
  public Optional<BinlogFileOffset> getLastBinlogFileOffset() {
    String selectOffsetByClientNameQuery = "select serialized_offset from %s where client_name = ?".formatted(tableName);

    String offset = jdbcTemplate.queryForObject(selectOffsetByClientNameQuery, String.class, clientName);
    return Optional.ofNullable(offset).map(o -> JSonMapper.fromJson(o, BinlogFileOffset.class));
  }

  @Override
  public void save(BinlogFileOffset binlogFileOffset) {
    String updateOffsetByClientNameQuery = "update %s set serialized_offset = ?".formatted(tableName);
    jdbcTemplate.update(updateOffsetByClientNameQuery, JSonMapper.toJson(binlogFileOffset));
  }
}
