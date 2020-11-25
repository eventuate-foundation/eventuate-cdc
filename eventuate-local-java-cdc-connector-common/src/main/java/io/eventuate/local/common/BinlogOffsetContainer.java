package io.eventuate.local.common;

public class BinlogOffsetContainer<OFFSET> {
  private OFFSET offset;
  private boolean forSave;

  public BinlogOffsetContainer(OFFSET offset, boolean forSave) {
    this.offset = offset;
    this.forSave = forSave;
  }

  public OFFSET getOffset() {
    return offset;
  }

  public boolean isForSave() {
    return forSave;
  }
}
