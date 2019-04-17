package io.eventuate.local.postgres.wal;

import io.eventuate.common.BinlogFileOffset;
import io.eventuate.local.common.BinlogEntry;

import java.util.Arrays;
import java.util.List;

public class PostgresWalBinlogEntryExtractor {

  public BinlogEntry extract(PostgresWalChange postgresWalChange) {
    List<String> columns = Arrays.asList(postgresWalChange.getColumnnames());
    List<String> values = Arrays.asList(postgresWalChange.getColumnvalues());

    return new BinlogEntry() {
      @Override
      public Object getColumn(String name) {
        return values.get(columns.indexOf(name));
      }

      @Override
      public BinlogFileOffset getBinlogFileOffset() {
        return null;
      }
    };
  }
}
