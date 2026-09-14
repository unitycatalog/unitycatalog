package io.unitycatalog.server.sharing;

import static org.junit.jupiter.api.Assertions.assertNull;

import io.unitycatalog.server.utils.ServerProperties;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class OpenSharingLifecycleTest {

  @Test
  void leavesEmbeddedRuntimeStoppedWhenTheFeatureIsDisabled() {
    Properties values = new Properties();
    values.setProperty("server.opensharing.enabled", "false");

    assertNull(
        OpenSharingLifecycle.start(
            new ServerProperties(values), null, null, null, null, 8080));
  }
}
