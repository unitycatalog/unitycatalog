package io.unitycatalog.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class UnityCatalogServerTest {

  @Test
  public void derivesObservabilityPortFromClientPortWhenNotConfigured() {
    ServerProperties serverProperties = new ServerProperties(new Properties());

    assertThat(UnityCatalogServer.resolveObservabilityPort(9000, serverProperties)).isEqualTo(9002);
  }

  @Test
  public void explicitObservabilityPortOverridesDerivedPort() {
    Properties configured = new Properties();
    configured.setProperty(Property.OBSERVABILITY_PORT.getKey(), "9464");
    ServerProperties serverProperties = new ServerProperties(configured);

    assertThat(UnityCatalogServer.resolveObservabilityPort(9000, serverProperties)).isEqualTo(9464);
  }

  @Test
  public void rejectsObservabilityPortEqualToClientPort() {
    Properties configured = new Properties();
    configured.setProperty(Property.OBSERVABILITY_PORT.getKey(), "9000");
    ServerProperties serverProperties = new ServerProperties(configured);

    assertThatThrownBy(() -> UnityCatalogServer.resolveObservabilityPort(9000, serverProperties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("server.observability.port")
        .hasMessageContaining("client-facing port");
  }
}
