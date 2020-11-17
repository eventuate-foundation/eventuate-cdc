package io.eventuate.tram.cdc.connector;

import io.eventuate.local.common.CdcProcessingStatus;
import io.eventuate.local.unified.cdc.pipeline.common.BinlogEntryReaderProvider;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

@RestController
public class CdcReaderController {
  private BinlogEntryReaderProvider binlogEntryReaderProvider;

  public CdcReaderController(BinlogEntryReaderProvider binlogEntryReaderProvider) {
    this.binlogEntryReaderProvider = binlogEntryReaderProvider;
  }

  @RequestMapping(value = "/readers/{reader}/finished", method = RequestMethod.GET)
  public ResponseEntity isCdcReaderFinished(@PathVariable("reader") String reader) {

    return getStatus(reader)
            .map(s -> new ResponseEntity<>(s.isCdcProcessingFinished() ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE))
            .orElse(ResponseEntity.notFound().build());
  }

  @RequestMapping(value = "/readers/{reader}/status", method = RequestMethod.GET)
  public ResponseEntity<CdcProcessingStatus> getCdcReaderStatus(@PathVariable("reader") String reader) {

    return getStatus(reader).map(s -> new ResponseEntity<>(s, HttpStatus.OK)).orElse(ResponseEntity.notFound().build());
  }

  @RequestMapping(value = "/cdc-event-processing-status", method = RequestMethod.GET)
  public ResponseEntity<CdcProcessingStatus> getCdcReaderStatusCompatibilityMethod(@RequestParam("readerName") String readerName) {
    return getCdcReaderStatus(readerName);
  }

  private Optional<CdcProcessingStatus> getStatus(String readerName) {
    return Optional
            .ofNullable(binlogEntryReaderProvider.get(readerName))
            .map(readerLeadership -> readerLeadership.getBinlogEntryReader().getCdcProcessingStatusService().getCurrentStatus());
  }
}