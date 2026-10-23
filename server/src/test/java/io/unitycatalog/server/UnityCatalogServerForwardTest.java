package io.unitycatalog.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.Server;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs a component on its own loopback listener next to a full {@link UnityCatalogServer}, the way
 * an embedded sharing server would, and reaches both through UC's one public port.
 */
class UnityCatalogServerForwardTest {

  private static final String PREFIX = "/api/2.1/opensharing/";

  @TempDir Path tempDir;

  @Test
  void servesUnityCatalogAndTheForwardedComponentOnOnePort() throws Exception {
    // Both ports are chosen while both sockets are open. A wildcard ServerSocket(0) can otherwise
    // return a port the component already holds on loopback, which UC's all-interfaces bind
    // then refuses.
    int port;
    int componentPort;
    try (ServerSocket api = new ServerSocket(0);
        ServerSocket other = new ServerSocket(0)) {
      port = api.getLocalPort();
      componentPort = other.getLocalPort();
    }
    Server component =
        Server.builder()
            .localPort(componentPort, SessionProtocol.HTTP)
            .serviceUnder(PREFIX, (ctx, req) -> HttpResponse.of("component: " + ctx.path()))
            .build();
    component.start().join();

    Properties properties = new Properties();
    properties.setProperty(Property.SERVER_ENV.getKey(), "test");
    properties.setProperty(Property.TABLE_STORAGE_ROOT.getKey(), tempDir.toUri().toString());
    properties.setProperty(Property.AUTHORIZATION_ENABLED.getKey(), "enable");
    ServerProperties serverProperties = new ServerProperties(properties);
    HibernateConfigurator hibernateConfigurator =
        new HibernateConfigurator(HibernateConfigurator.setupHibernateProperties(serverProperties));
    UnityCatalogServer server =
        UnityCatalogServer.builder()
            .port(port)
            .serverProperties(serverProperties)
            .hibernateConfigurator(hibernateConfigurator)
            .forward(PREFIX, URI.create("http://127.0.0.1:" + componentPort))
            .build();
    try {
      server.start();
      WebClient client = WebClient.of("http://127.0.0.1:" + port);

      AggregatedHttpResponse forwarded = client.get(PREFIX + "shares").aggregate().join();
      assertThat(forwarded.status()).isEqualTo(HttpStatus.OK);
      assertThat(forwarded.contentUtf8()).isEqualTo("component: " + PREFIX + "shares");

      // The same port still serves UC itself, behind UC's authentication.
      assertThat(client.get("/api/2.1/unity-catalog/catalogs").aggregate().join().status())
          .isEqualTo(HttpStatus.UNAUTHORIZED);
    } finally {
      server.close();
      component.stop().join();
      hibernateConfigurator.close();
    }
  }
}
