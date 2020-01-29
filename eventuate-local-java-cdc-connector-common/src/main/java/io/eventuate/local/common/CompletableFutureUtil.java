package io.eventuate.local.common;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class CompletableFutureUtil {
  public static <T> T get(CompletableFuture<T> completableFuture) {
    try {
      return completableFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  };
}
