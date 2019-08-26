package io.eventuate.tram.cdc.connector;

import io.eventuate.local.common.CdcProcessingStatus;
import io.eventuate.local.unified.cdc.pipeline.common.BinlogEntryReaderLeadershipProvider;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CdcProcessingStatusController {
  private BinlogEntryReaderLeadershipProvider binlogEntryReaderLeadershipProvider;

  public CdcProcessingStatusController(BinlogEntryReaderLeadershipProvider binlogEntryReaderLeadershipProvider) {
    this.binlogEntryReaderLeadershipProvider = binlogEntryReaderLeadershipProvider;
  }

  @RequestMapping(value = "/cdc-event-processing-status", method = RequestMethod.GET)
  public CdcProcessingStatus allCdcEventsProcessed(@RequestParam("readerName") String readerName) {
    return binlogEntryReaderLeadershipProvider
            .getBinlogEntryReaderLeadership(readerName)
            .getBinlogEntryReader()
            .getCdcProcessingStatusService()
            .getCurrentStatus();
  }
}