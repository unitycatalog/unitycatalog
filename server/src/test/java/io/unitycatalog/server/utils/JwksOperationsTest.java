package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.OAuthInvalidRequestException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JwksOperationsTest {

  private HttpServer issuerServer;
  private String issuer;
  private int discoveryStatus;
  private String discoveryContentType;
  private String discoveryBody;

  private final JwksOperations jwksOperations = new JwksOperations(null);

  @BeforeEach
  void startIssuerServer() throws Exception {
    issuerServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    // Daemon threads, so the forked test JVM can exit even if stop() leaves threads behind.
    issuerServer.setExecutor(
        Executors.newCachedThreadPool(
            r -> {
              Thread t = new Thread(r);
              t.setDaemon(true);
              return t;
            }));
    issuerServer.createContext(
        "/.well-known/openid-configuration",
        exchange -> {
          byte[] body = discoveryBody.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", discoveryContentType);
          exchange.sendResponseHeaders(discoveryStatus, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    issuerServer.createContext(
        "/jwks",
        exchange -> {
          byte[] body = "RBAC: access denied".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "text/plain");
          exchange.sendResponseHeaders(403, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    issuerServer.start();
    issuer = "http://127.0.0.1:" + issuerServer.getAddress().getPort();
  }

  @AfterEach
  void stopIssuerServer() {
    issuerServer.stop(0);
  }

  @Test
  void loadJwkProvider_reportsStatusAndUrlWhenDiscoveryRequestIsRejected() {
    // What Istio returns when an AuthorizationPolicy denies the request.
    respondWith(403, "text/plain", "RBAC: access denied");

    assertThatThrownBy(() -> jwksOperations.loadJwkProvider(issuer))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasMessageContaining(issuer + "/.well-known/openid-configuration")
        .hasMessageContaining("HTTP 403")
        .hasMessageNotContaining("RBAC")
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INTERNAL);
  }

  @Test
  void verifierForIssuerAndKey_reportsKeyAndIssuerWhenSigningKeyFetchIsRejected() {
    respondWith(
        200,
        "application/json",
        String.format("{\"issuer\":\"%s\",\"jwks_uri\":\"%s/jwks\"}", issuer, issuer));

    assertThatThrownBy(() -> jwksOperations.verifierForIssuerAndKey(issuer, "key-1", "RS256"))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasMessage("Could not get signing key 'key-1' for issuer " + issuer)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INTERNAL);
  }

  @Test
  void loadJwkProvider_reportsUrlWhenDiscoveryDocumentIsNotJson() {
    respondWith(200, "text/html", "<html>login</html>");

    assertThatThrownBy(() -> jwksOperations.loadJwkProvider(issuer))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasMessageContaining(issuer + "/.well-known/openid-configuration")
        .hasMessageContaining("not valid JSON");
  }

  @Test
  void loadJwkProvider_reportsBothIssuersWhenDiscoveryIssuerDiffers() {
    respondWith(200, "application/json", "{\"issuer\":\"https://other.example\"}");

    assertThatThrownBy(() -> jwksOperations.loadJwkProvider(issuer))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasMessageContaining("'" + issuer + "'")
        .hasMessageContaining("'https://other.example'")
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.ABORTED);
  }

  @Test
  void loadJwkProvider_reportsMismatchWhenDiscoveryIssuerIsMissing() {
    respondWith(200, "application/json", "{\"jwks_uri\":\"https://other.example/jwks\"}");

    assertThatThrownBy(() -> jwksOperations.loadJwkProvider(issuer))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasMessageContaining("doesn't match configuration issuer 'null'");
  }

  private void respondWith(int status, String contentType, String body) {
    discoveryStatus = status;
    discoveryContentType = contentType;
    discoveryBody = body;
  }
}
