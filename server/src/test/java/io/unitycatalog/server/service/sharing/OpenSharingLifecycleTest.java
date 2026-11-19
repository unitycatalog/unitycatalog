package io.unitycatalog.server.service.sharing;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class OpenSharingLifecycleTest {

  @Test
  public void refusesToStartWithoutAuthorization() {
    assertThatThrownBy(() -> start(new Properties()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("server.authorization=enable");
  }

  @Test
  public void refusesToStartWithoutPublicUrl() {
    Properties withoutPublicUrl = new Properties();
    withoutPublicUrl.setProperty(Property.AUTHORIZATION_ENABLED.getKey(), "enable");
    assertThatThrownBy(() -> start(withoutPublicUrl))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(Property.OPENSHARING_PUBLIC_URL.getKey());
  }

  private static void start(Properties properties) {
    OpenSharingLifecycle.start(new ServerProperties(properties), null, null);
  }
}
