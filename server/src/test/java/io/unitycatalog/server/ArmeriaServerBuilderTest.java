package io.unitycatalog.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linecorp.armeria.server.Server;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ArmeriaServerBuilderTest {

  @Test
  public void apiPortBindsLoopbackObservabilityPortBindsAllInterfaces() {
    // Asserts the configured bind addresses, which is what determines off-box reachability. It does
    // not (and in-process cannot) prove the API port refuses a non-loopback connection: the server
    // is only build()'d, not start()'ed, and any in-process client would connect over 127.0.0.1,
    // which a loopback bind accepts anyway. Off-box exposure of the observability port is guarded
    // by
    // network policy (see the source comment on the port binding).
    try (Server server =
        new ArmeriaServerBuilder(8080, "/api/", "/control/", new ServerProperties(new Properties()))
            .observabilityPort(8090)
            .build()) {
      // The API port is reached only through the in-process URL transcoder, so it binds the
      // loopback interfaces.
      assertThat(server.config().ports())
          .filteredOn(port -> port.localAddress().getPort() == 8080)
          .isNotEmpty()
          .allSatisfy(
              port -> assertThat(port.localAddress().getAddress().isLoopbackAddress()).isTrue());

      // The observability port must be reachable by kubelet/Prometheus at the pod IP, so it binds
      // all interfaces (guarded by network policy), not just loopback.
      assertThat(server.config().ports())
          .filteredOn(port -> port.localAddress().getPort() == 8090)
          .isNotEmpty()
          .allSatisfy(
              port -> assertThat(port.localAddress().getAddress().isAnyLocalAddress()).isTrue());
    }
  }

  @Test
  public void rejectsObservabilityPortEqualToApiPort() {
    // The two ports must be distinct: sharing one would collapse the API and the observability
    // endpoints onto a single listener, defeating the isolation. Fails fast at construction.
    assertThatThrownBy(
            () ->
                new ArmeriaServerBuilder(
                        8080, "/api/", "/control/", new ServerProperties(new Properties()))
                    .observabilityPort(8080))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Observability port");
  }

  @Test
  public void defaultConfigurationBindsOnlyTheApiPort() {
    try (Server server =
        new ArmeriaServerBuilder(8080, "/api/", "/control/", new ServerProperties(new Properties()))
            .build()) {
      assertThat(server.config().ports())
          .isNotEmpty()
          .allSatisfy(
              port -> {
                assertThat(port.localAddress().getPort()).isEqualTo(8080);
                assertThat(port.localAddress().getAddress().isLoopbackAddress()).isTrue();
              });
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 9464, 65535})
  public void bindsExplicitObservabilityPort(int observabilityPort) {
    try (Server server =
        new ArmeriaServerBuilder(9001, "/api/", "/control/", new ServerProperties(new Properties()))
            .observabilityPort(observabilityPort)
            .build()) {
      assertThat(server.config().ports())
          .extracting(port -> port.localAddress().getPort())
          .contains(9001, observabilityPort);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1, 65536, Integer.MAX_VALUE})
  public void rejectsOutOfRangeObservabilityPort(int configuredPort) {
    assertThatThrownBy(
            () ->
                new ArmeriaServerBuilder(
                        9001, "/api/", "/control/", new ServerProperties(new Properties()))
                    .observabilityPort(configuredPort))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("between 1 and 65535");
  }

  @Test
  public void standaloneServerDoesNotReserveAnAbsentClientPort() {
    try (Server server =
        new ArmeriaServerBuilder(9001, "/api/", "/control/", new ServerProperties(new Properties()))
            .observabilityPort(9000)
            .build()) {
      assertThat(server.config().ports())
          .extracting(port -> port.localAddress().getPort())
          .contains(9001, 9000);
    }
  }
}
