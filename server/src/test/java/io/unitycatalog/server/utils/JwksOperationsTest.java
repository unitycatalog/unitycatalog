package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import io.unitycatalog.server.exception.OAuthInvalidRequestException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JwksOperationsTest {
  private HttpServer server;

  @BeforeEach
  void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    server.start();
  }

  @AfterEach
  void tearDown() {
    server.stop(0);
  }

  @Test
  void acceptsValidMetadata() {
    assertThat(
            JwksOperations.validateIssuerConfiguration(
                Map.of(
                    "issuer", "https://issuer.example", "jwks_uri", "https://issuer.example/keys"),
                "https://issuer.example"))
        .isEqualTo("https://issuer.example/keys");
  }

  @Test
  void rejectsMissingWrongTypeAndMismatchedMetadata() {
    assertThatThrownBy(
            () -> JwksOperations.validateIssuerConfiguration(Map.of("issuer", "issuer"), "issuer"))
        .isInstanceOf(OAuthInvalidRequestException.class);
    assertThatThrownBy(
            () ->
                JwksOperations.validateIssuerConfiguration(
                    Map.of("issuer", 42, "jwks_uri", "https://keys"), "issuer"))
        .isInstanceOf(OAuthInvalidRequestException.class);
    assertThatThrownBy(
            () ->
                JwksOperations.validateIssuerConfiguration(
                    Map.of("issuer", "https://other", "jwks_uri", "https://keys"),
                    "https://issuer.example"))
        .isInstanceOf(OAuthInvalidRequestException.class);
  }

  @Test
  void rejectsBlankMetadata() {
    assertThatThrownBy(
            () ->
                JwksOperations.validateIssuerConfiguration(
                    Map.of("issuer", " ", "jwks_uri", "https://keys"), "issuer"))
        .isInstanceOf(OAuthInvalidRequestException.class);
    assertThatThrownBy(
            () ->
                JwksOperations.validateIssuerConfiguration(
                    Map.of("issuer", "issuer", "jwks_uri", "  "), "issuer"))
        .isInstanceOf(OAuthInvalidRequestException.class);
  }

  @Test
  void rejectsNonSuccessDiscoveryResponse() {
    server.createContext(
        "/.well-known/openid-configuration",
        exchange -> {
          exchange.sendResponseHeaders(500, -1);
          exchange.close();
        });

    assertThatThrownBy(
            () ->
                new JwksOperations(null)
                    .loadJwkProvider("http://localhost:" + server.getAddress().getPort()))
        .isInstanceOf(OAuthInvalidRequestException.class);
  }

  @Test
  void rejectsInvalidDiscoveryJson() throws IOException {
    server.createContext(
        "/.well-known/openid-configuration",
        exchange -> {
          byte[] body = "not-json".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });

    assertThatThrownBy(
            () ->
                new JwksOperations(null)
                    .loadJwkProvider("http://localhost:" + server.getAddress().getPort()))
        .isInstanceOf(OAuthInvalidRequestException.class);
  }
}
