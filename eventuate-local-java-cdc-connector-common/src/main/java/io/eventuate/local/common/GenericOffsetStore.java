package io.eventuate.local.common;

public interface GenericOffsetStore<OFFSET> {
  void save(OFFSET offset);
}
