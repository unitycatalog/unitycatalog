package io.unitycatalog.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linecorp.armeria.common.util.BlockingTaskExecutor;
import com.linecorp.armeria.server.Server;
import io.unitycatalog.server.utils.ServerProperties;
import java.net.URI;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ArmeriaServerBuilderTest {

  private static BlockingTaskExecutor blockingTaskExecutor;

  @BeforeAll
  public static void createBlockingTaskExecutor() {
    blockingTaskExecutor = BlockingTaskExecutor.builder().numThreads(1).build();
  }

  @AfterAll
  public static void shutdownBlockingTaskExecutor() {
    blockingTaskExecutor.shutdown();
  }

  private static ArmeriaServerBuilder newBuilder(int port) {
    return new ArmeriaServerBuilder(
        port, "/api/", "/control/", new ServerProperties(new Properties()), blockingTaskExecutor);
  }

  @Test
  public void apiAndObservabilityPortsBindAllInterfaces() {
    // Clients connect to the API port directly, and kubelet/Prometheus reach the observability port
    // at the pod IP (guarded by network policy), so neither is limited to loopback.
    try (Server server = newBuilder(8080).observabilityPort(8090).build()) {
      for (int boundPort : new int[] {8080, 8090}) {
        assertThat(server.config().ports())
            .as("port %d", boundPort)
            .filteredOn(port -> port.localAddress().getPort() == boundPort)
            .isNotEmpty()
            .allSatisfy(
                port -> assertThat(port.localAddress().getAddress().isAnyLocalAddress()).isTrue());
      }
    }
  }

  @Test
  public void rejectsObservabilityPortEqualToApiPort() {
    // The two ports must be distinct: sharing one would collapse the API and the observability
    // endpoints onto a single listener, defeating the isolation. Fails fast at construction.
    assertThatThrownBy(() -> newBuilder(8080).observabilityPort(8080))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Observability port");
  }

  @Test
  public void defaultConfigurationBindsOnlyTheApiPort() {
    try (Server server = newBuilder(8080).build()) {
      assertThat(server.config().ports())
          .isNotEmpty()
          .allSatisfy(port -> assertThat(port.localAddress().getPort()).isEqualTo(8080));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 9464, 65535})
  public void bindsExplicitObservabilityPort(int observabilityPort) {
    try (Server server = newBuilder(9001).observabilityPort(observabilityPort).build()) {
      assertThat(server.config().ports())
          .extracting(port -> port.localAddress().getPort())
          .contains(9001, observabilityPort);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1, 65536, Integer.MAX_VALUE})
  public void rejectsOutOfRangeObservabilityPort(int configuredPort) {
    assertThatThrownBy(() -> newBuilder(9001).observabilityPort(configuredPort))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("between 1 and 65535");
  }

  @Test
  public void rejectsAForwardedPrefixInsideTheUnityCatalogApi() {
    assertThatThrownBy(() -> newBuilder(8080).forward("/api/shares/", URI.create("http://x")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("overlaps");
  }
}
