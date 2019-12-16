package io.eventuate.local.polling;

import io.eventuate.local.common.GenericOffsetStore;
import io.eventuate.local.common.OffsetProcessor;

import java.util.ArrayList;
import java.util.List;

public class PollingOffsetProcessor extends OffsetProcessor<Object> {

  public PollingOffsetProcessor(GenericOffsetStore<Object> offsetStore) {
    super(offsetStore);
  }

  @Override
  protected void collectAndSaveOffsets() {
    List<Object> offsetsToSave = new ArrayList<>();

    while (true) {
      if (isDone(offsets.peek())) {
        offsetsToSave.add(getOffset(offsets.poll()));
      }
      else {
        break;
      }
    }

    offsetStore.save(offsetsToSave);
  }
}
