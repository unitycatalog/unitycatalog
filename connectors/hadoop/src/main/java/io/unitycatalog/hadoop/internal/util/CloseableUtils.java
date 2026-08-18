package io.unitycatalog.hadoop.internal.util;

import java.io.Closeable;
import java.io.IOException;

public final class CloseableUtils {
  private CloseableUtils() {}

  public static void closeQuietly(Closeable closeable) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (IOException e) {
      // Ignore close failures during best-effort cleanup.
    }
  }
}
