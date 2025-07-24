package io.eventuate.tram.cdc.connector;

import io.eventuate.local.common.CdcProcessingStatus;
import io.eventuate.local.unified.cdc.pipeline.common.BinlogEntryReaderProvider;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@RestController
public class CdcReaderController {
  private BinlogEntryReaderProvider binlogEntryReaderProvider;

  public CdcReaderController(BinlogEntryReaderProvider binlogEntryReaderProvider) {
    this.binlogEntryReaderProvider = binlogEntryReaderProvider;
  }

  @GetMapping("/readers/{reader}/finished")
  public ResponseEntity isCdcReaderFinished(@PathVariable("reader") String reader) {

    return getStatus(reader)
            .map(s -> new ResponseEntity<>(s.isCdcProcessingFinished() ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE))
            .orElse(ResponseEntity.notFound().build());
  }

  @GetMapping("/readers/{reader}/status")
  public ResponseEntity<CdcProcessingStatus> getCdcReaderStatus(@PathVariable("reader") String reader) {

    return getStatus(reader).map(s -> new ResponseEntity<>(s, HttpStatus.OK)).orElse(ResponseEntity.notFound().build());
  }

  @GetMapping("/cdc-event-processing-status")
  public ResponseEntity<CdcProcessingStatus> getCdcReaderStatusCompatibilityMethod(@RequestParam("readerName") String readerName) {
    return getCdcReaderStatus(readerName);
  }

  private Optional<CdcProcessingStatus> getStatus(String readerName) {
    return Optional
            .ofNullable(binlogEntryReaderProvider.get(readerName))
            .map(readerLeadership -> readerLeadership.getBinlogEntryReader().getCdcProcessingStatusService().getCurrentStatus());
  }
}