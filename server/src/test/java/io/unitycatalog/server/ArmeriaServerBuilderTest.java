package io.unitycatalog.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.common.util.BlockingTaskExecutor;
import com.linecorp.armeria.server.Server;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class ArmeriaServerBuilderTest {

  @Test
  public void bindsToLoopbackInterfaces() {
    BlockingTaskExecutor blockingTaskExecutor =
        BlockingTaskExecutor.builder().numThreads(1).build();
    try {
      try (Server server =
          new ArmeriaServerBuilder(
                  0,
                  "/api/",
                  "/control/",
                  new ServerProperties(new Properties()),
                  blockingTaskExecutor)
              .build()) {
        assertThat(server.config().ports())
            .isNotEmpty()
            .allSatisfy(
                port -> assertThat(port.localAddress().getAddress().isLoopbackAddress()).isTrue());
      }
    } finally {
      blockingTaskExecutor.shutdown();
    }
  }
}
