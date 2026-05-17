package io.unitycatalog.server.base.auth;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;

import com.auth0.jwt.algorithms.Algorithm;
import com.sun.net.httpserver.HttpServer;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.security.SecurityConfiguration;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class BaseAuthCRUDTest extends BaseServerTest {

  protected static final String TEST_AUDIENCE = "unity-catalog";

  protected SecurityConfiguration securityConfiguration;
  protected SecurityContext securityContext;

  // Test identity provider keypair and mock OIDC server
  private HttpServer mockOidcServer;
  protected String testIssuer;
  protected Algorithm testIssuerAlgorithm;
  protected String testIssuerKeyId;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.AUTHORIZATION_ENABLED.getKey(), "enable");

    try {
      startMockOidcServer();
    } catch (Exception e) {
      throw new RuntimeException("Failed to start mock OIDC server", e);
    }

    serverProperties.setProperty("server.allowed-issuers", testIssuer);
    serverProperties.setProperty("server.audiences", TEST_AUDIENCE);
  }

  /**
   * Starts a lightweight server that acts as a mock OIDC identity provider. It serves:
   *
   * <ul>
   *   <li>{@code /.well-known/openid-configuration} — OIDC discovery document
   *   <li>{@code /jwks} — JWKS endpoint with the test keypair's public key
   * </ul>
   */
  private void startMockOidcServer() throws Exception {
    // Generate a dedicated RSA keypair for the test identity provider
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048);
    var keyPair = keyPairGenerator.generateKeyPair();
    RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
    RSAPrivateKey privateKey = (RSAPrivateKey) keyPair.getPrivate();

    testIssuerKeyId = UUID.randomUUID().toString();
    testIssuerAlgorithm = Algorithm.RSA512(publicKey, privateKey);

    // Build JWKS JSON from the public key
    String jwksJson = buildJwksJson(publicKey, testIssuerKeyId);

    // Use the JDK built-in HttpServer instead of Armeria to avoid shared
    // event-loop threads that prevent the forked test JVM from exiting.
    mockOidcServer = HttpServer.create(new InetSocketAddress(0), 0);

    mockOidcServer.createContext(
        "/.well-known/openid-configuration",
        exchange -> {
          String discoveryDoc =
              String.format("{\"issuer\":\"%s\",\"jwks_uri\":\"%s/jwks\"}", testIssuer, testIssuer);
          byte[] body = discoveryDoc.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });

    mockOidcServer.createContext(
        "/jwks",
        exchange -> {
          byte[] body = jwksJson.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });

    mockOidcServer.start();

    int port = mockOidcServer.getAddress().getPort();
    testIssuer = "http://localhost:" + port;
  }

  /** Builds a JWKS JSON string containing a single RSA public key. */
  private static String buildJwksJson(RSAPublicKey publicKey, String keyId) {
    Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
    String n = encoder.encodeToString(toUnsignedBytes(publicKey.getModulus()));
    String e = encoder.encodeToString(toUnsignedBytes(publicKey.getPublicExponent()));
    return String.format(
        "{\"keys\":[{\"kty\":\"RSA\",\"use\":\"sig\",\"alg\":\"RS512\",\"kid\":\"%s\",\"n\":\"%s\",\"e\":\"%s\"}]}",
        keyId, n, e);
  }

  /** Converts a BigInteger to unsigned big-endian bytes (no leading zero padding). */
  private static byte[] toUnsignedBytes(BigInteger value) {
    byte[] bytes = value.toByteArray();
    if (bytes[0] == 0) {
      byte[] trimmed = new byte[bytes.length - 1];
      System.arraycopy(bytes, 1, trimmed, 0, trimmed.length);
      return trimmed;
    }
    return bytes;
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();

    Path configurationFolder = Path.of("etc", "conf");

    securityConfiguration = new SecurityConfiguration(configurationFolder);
    securityContext =
        new SecurityContext(configurationFolder, securityConfiguration, "server", INTERNAL);
  }

  @AfterEach
  @Override
  public void tearDown() {
    // Stop the UC server first (parent), then the mock OIDC server.
    // This ordering prevents the UC server from trying to reach the
    // mock OIDC server after it has been shut down.
    super.tearDown();

    System.clearProperty("server.authorization");

    if (mockOidcServer != null) {
      mockOidcServer.stop(0);
      mockOidcServer = null;
    }
  }
}
