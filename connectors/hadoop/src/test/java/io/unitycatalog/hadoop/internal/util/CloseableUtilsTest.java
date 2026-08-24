package io.unitycatalog.hadoop.internal.util;

import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.Closeable;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class CloseableUtilsTest {

  @Test
  void closeQuietlyIgnoresCloseFailuresAndNulls() {
    Closeable brokenCloseable =
        () -> {
          throw new IOException("close failed");
        };

    assertThatCode(() -> CloseableUtils.closeQuietly(null)).doesNotThrowAnyException();
    assertThatCode(() -> CloseableUtils.closeQuietly(brokenCloseable)).doesNotThrowAnyException();
  }
}
