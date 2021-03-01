package io.eventuate.local.unified.cdc.pipeline.common;

import io.eventuate.common.eventuate.local.BinLogEvent;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.MessageCleaner;

public class CdcPipeline<EVENT extends BinLogEvent> {
  private CdcDataPublisher<EVENT> cdcDataPublisher;
  private MessageCleaner messageCleaner;

  public CdcPipeline(CdcDataPublisher<EVENT> cdcDataPublisher, MessageCleaner messageCleaner) {
    this.cdcDataPublisher = cdcDataPublisher;
    this.messageCleaner = messageCleaner;
  }

  public void start() {
    cdcDataPublisher.start();
    messageCleaner.start();
  }

  public void stop() {
    cdcDataPublisher.stop();
    messageCleaner.stop();
  }
}
