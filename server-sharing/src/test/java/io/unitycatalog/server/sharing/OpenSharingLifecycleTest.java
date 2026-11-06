package io.unitycatalog.server.sharing;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.utils.ServerProperties;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class OpenSharingLifecycleTest {

  @Test
  public void refusesToStartWithoutAuthorization() {
    assertThatThrownBy(() -> start(new Properties()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("server.authorization=enable");
  }

  private static void start(Properties properties) {
    OpenSharingLifecycle.start(new ServerProperties(properties), null, null, null, null);
  }
}
