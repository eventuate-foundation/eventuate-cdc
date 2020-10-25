package io.eventuate.local.mysql.binlog;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.id.IdGenerator;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.BinlogEntryReaderLeadership;
import io.eventuate.local.common.BinlogEntryToPublishedEventConverter;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.exception.EventuateLocalPublishingException;
import io.eventuate.local.test.util.SourceTableNameSupplier;
import io.eventuate.local.test.util.TestHelper;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class AbstractMySqlBinaryLogClientTest {

  @Autowired
  protected MySqlBinaryLogClient mySqlBinaryLogClient;

  @Autowired
  protected SourceTableNameSupplier sourceTableNameSupplier;

  @Autowired
  protected EventuateSchema eventuateSchema;

  @Autowired
  protected TestHelper testHelper;

  @Autowired
  protected BinlogEntryReaderLeadership binlogEntryReaderLeadership;

  @Autowired
  protected IdGenerator idGenerator;

  protected void prepareBinlogEntryHandler(Consumer<PublishedEvent> consumer) {
    mySqlBinaryLogClient.addBinlogEntryHandler(eventuateSchema,
            sourceTableNameSupplier.getSourceTableName(),
            new BinlogEntryToPublishedEventConverter(idGenerator),
            new CdcDataPublisher<PublishedEvent>(null, null, null, null) {
              @Override
              public CompletableFuture<?> sendMessage(PublishedEvent publishedEvent) throws EventuateLocalPublishingException {
                consumer.accept(publishedEvent);
                return CompletableFuture.completedFuture(null);
              }
            });
  }
}
