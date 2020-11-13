package io.eventuate.local.mysql.binlog;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.local.db.log.common.OffsetStore;
import io.eventuate.local.test.util.CdcProcessorEventsTest;
import org.springframework.beans.factory.annotation.Autowired;


public abstract class AbstractMySQLCdcProcessorEventsTest extends CdcProcessorEventsTest {

  @Autowired
  private OffsetStore offsetStore;

  @Override
  public void onEventSent(PublishedEvent publishedEvent) {
    offsetStore.save(publishedEvent.getBinlogFileOffset().get());
  }
}
