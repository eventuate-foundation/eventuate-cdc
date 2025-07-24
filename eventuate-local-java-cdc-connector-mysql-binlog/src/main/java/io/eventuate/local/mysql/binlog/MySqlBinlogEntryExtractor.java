package io.eventuate.local.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;
import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.common.jdbc.SchemaAndTable;
import io.eventuate.local.common.BinlogEntry;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MySqlBinlogEntryExtractor extends AbstractMySqlBinlogExtractor {


  public MySqlBinlogEntryExtractor(DataSource dataSource) {
    super(dataSource);
  }

  public BinlogEntry extract(SchemaAndTable schemaAndTable, WriteRowsEventData eventData, String binlogFilename, long position) {
    updateColumnOrders(schemaAndTable);

    return new BinlogEntry() {
      @Override
      public Object getColumn(String name) {
        return getValue(schemaAndTable, eventData, name);
      }

      @Override
      public BinlogFileOffset getBinlogFileOffset() {
        return new BinlogFileOffset(binlogFilename, position);
      }

      @Override
      public String getJsonColumn(String name) {
        byte[] bytes = getBytes(name);

        if (bytes == null) {
          return null;
        }

        try {
          return JsonBinary.parseAsString(bytes);
        } catch (IOException e) {
          //not a json, should be plain string from some old version of database schema
        }

        return new String(bytes, StandardCharsets.UTF_8);
      }

      @Override
      public String getStringColumn(String name) {
        byte[] bytes = getBytes(name);

        if (bytes == null) {
          return null;
        }

        return new String(bytes, StandardCharsets.UTF_8);
      }

      private byte[] getBytes(String name) {
        Object value = getColumn(name);

        if (value == null) {
          return null;
        }

        if (!(value instanceof byte[]))
          throw new IllegalArgumentException("Unexpected type %s of column %s, should be byte[]".formatted(
                  value.getClass(), name));

        return (byte[]) value;
      }

      private String binaryToString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
      }
    };
  }
}
