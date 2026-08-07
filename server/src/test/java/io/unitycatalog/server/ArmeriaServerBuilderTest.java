package io.unitycatalog.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.server.Server;
import org.junit.jupiter.api.Test;

public class ArmeriaServerBuilderTest {

  @Test
  public void bindsToLoopbackInterfaces() {
    try (Server server = new ArmeriaServerBuilder(0, "/api/", "/control/").build()) {
      assertThat(server.config().ports())
          .isNotEmpty()
          .allSatisfy(
              port -> assertThat(port.localAddress().getAddress().isLoopbackAddress()).isTrue());
    }
  }
}
