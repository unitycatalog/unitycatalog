package io.unitycatalog.server;

import static io.unitycatalog.server.utils.TestUtils.sendRawGet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.base.BaseServerTest;
import java.net.ServerSocket;
import java.net.URI;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class UnityCatalogServerTest extends BaseServerTest {

  @Test
  public void derivesObservabilityPortFromClientPortWhenAutomatic() {
    assertThat(UnityCatalogServer.resolveObservabilityPort(9000, 0)).isEqualTo(9002);
  }

  @Test
  public void explicitObservabilityPortOverridesDerivedPort() {
    assertThat(UnityCatalogServer.resolveObservabilityPort(9000, 9464)).isEqualTo(9464);
  }

  @ParameterizedTest
  @ValueSource(ints = {9000, 9001})
  public void rejectsObservabilityPortEqualToEitherApiPort(int observabilityPort) {
    assertThatThrownBy(() -> UnityCatalogServer.resolveObservabilityPort(9000, observabilityPort))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Observability port");
  }

  @Test
  public void leavesObservabilityPortUnboundWhenDisabled() throws Exception {
    int observabilityPort = URI.create(observabilityServerConfig.getServerUrl()).getPort();
    try (ServerSocket unusedObservabilityPort = new ServerSocket(observabilityPort)) {
      assertThat(sendRawGet(serverConfig, "/").statusCode()).isEqualTo(200);
      for (String path : List.of("/livez", "/readyz", "/metrics")) {
        assertThat(sendRawGet(serverConfig, path).statusCode()).as(path).isEqualTo(404);
      }
    }
  }
}
