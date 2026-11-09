package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.SignatureVerificationException;
import com.linecorp.armeria.client.ResponseTimeoutException;
import com.linecorp.armeria.common.ContentTooLargeException;
import com.sun.net.httpserver.HttpServer;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.OAuthInvalidRequestException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class JwksOperationsTest {

  private static KeyPair signingKey;
  private static KeyPair rotatedSigningKey;
  private HttpServer issuerServer;
  private String issuer;
  private int discoveryStatus;
  private String discoveryContentType;
  private String discoveryBody;
  private String discoveryRedirect;
  private boolean isDiscoveryStalled;
  private String jwksBody = "RBAC: access denied";
  private int jwksStatus = 403;
  private String jwksRedirect;
  private boolean isJwksStalled;
  private final AtomicInteger discoveryRequests = new AtomicInteger();
  private final AtomicInteger jwksRequests = new AtomicInteger();
  private final AtomicInteger redirectRequests = new AtomicInteger();
  private final AtomicLong elapsedNanos = new AtomicLong();

  private final JwksOperations jwksOperations =
      new JwksOperations(null, List.of(), elapsedNanos::get, Duration.ofSeconds(1));

  @BeforeAll
  static void createSigningKeys() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    signingKey = generator.generateKeyPair();
    rotatedSigningKey = generator.generateKeyPair();
  }

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
          discoveryRequests.incrementAndGet();
          if (isDiscoveryStalled) {
            return;
          }
          byte[] body = discoveryBody.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", discoveryContentType);
          if (discoveryRedirect != null) {
            exchange.getResponseHeaders().set("Location", discoveryRedirect);
          }
          exchange.sendResponseHeaders(discoveryStatus, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    issuerServer.createContext(
        "/jwks",
        exchange -> {
          jwksRequests.incrementAndGet();
          if (isJwksStalled) {
            return;
          }
          byte[] body = jwksBody.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          if (jwksRedirect != null) {
            exchange.getResponseHeaders().set("Location", jwksRedirect);
          }
          exchange.sendResponseHeaders(jwksStatus, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    issuerServer.createContext(
        "/redirect-target",
        exchange -> {
          redirectRequests.incrementAndGet();
          exchange.sendResponseHeaders(200, -1);
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
    assertThatThrownBy(() -> jwksOperations.loadJwkProvider(issuer))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasMessageContaining("HTTP 403");
    assertThat(discoveryRequests).hasValue(1);
  }

  @Test
  void verifierForIssuerAndKey_reportsFetchFailureAndDelaysRetries() {
    respondWith(
        200,
        "application/json",
        String.format("{\"issuer\":\"%s\",\"jwks_uri\":\"%s/jwks\"}", issuer, issuer));

    assertThatThrownBy(() -> jwksOperations.verifierForIssuerAndKey(issuer, "key-1", "RS256"))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasMessage("Could not get signing key 'key-1' for issuer " + issuer)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INTERNAL);

    respondWithKey("key-1", signingKey);
    assertThatThrownBy(() -> jwksOperations.verifierForIssuerAndKey(issuer, "key-1", "RS256"))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasRootCauseMessage("Signing-key refresh is rate limited");
    assertThat(jwksRequests).hasValue(1);

    elapsedNanos.set(Duration.ofSeconds(6).toNanos());
    jwksOperations
        .verifierForIssuerAndKey(issuer, "key-1", "RS256")
        .verify(tokenForKey("key-1", signingKey));
    assertThat(jwksRequests).hasValue(2);
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

  @Test
  void verifierForIssuerAndKey_cachesKeysUntilRotationAndProviderExpiry() {
    respondWithDiscovery(issuer + "/jwks");
    respondWithKey("key-1", signingKey);
    String token = tokenForKey("key-1", signingKey);
    for (int i = 0; i < 20; i++) {
      jwksOperations.verifierForIssuerAndKey(issuer, "key-1", "RS256").verify(token);
    }
    assertThat(discoveryRequests).hasValue(1);
    assertThat(jwksRequests).hasValue(1);

    respondWithKey("key-2", rotatedSigningKey);
    String rotatedToken = tokenForKey("key-2", rotatedSigningKey);
    elapsedNanos.set(Duration.ofSeconds(6).minusNanos(1).toNanos());
    assertThatThrownBy(() -> jwksOperations.verifierForIssuerAndKey(issuer, "key-2", "RS256"))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasRootCauseMessage("Signing-key refresh is rate limited");
    assertThat(jwksRequests).hasValue(1);

    elapsedNanos.incrementAndGet();
    jwksOperations.verifierForIssuerAndKey(issuer, "key-2", "RS256").verify(rotatedToken);
    assertThat(discoveryRequests).hasValue(1);
    assertThat(jwksRequests).hasValue(2);

    elapsedNanos.set(Duration.ofHours(1).minusNanos(1).toNanos());
    jwksOperations.verifierForIssuerAndKey(issuer, "key-2", "RS256").verify(rotatedToken);
    assertThat(discoveryRequests).hasValue(1);
    assertThat(jwksRequests).hasValue(2);

    elapsedNanos.incrementAndGet();
    // Reusing the key ID proves that expiry also replaces cached key material.
    respondWithKey("key-2", signingKey);
    jwksOperations
        .verifierForIssuerAndKey(issuer, "key-2", "RS256")
        .verify(tokenForKey("key-2", signingKey));
    assertThatThrownBy(
            () ->
                jwksOperations
                    .verifierForIssuerAndKey(issuer, "key-2", "RS256")
                    .verify(rotatedToken))
        .isInstanceOf(SignatureVerificationException.class);
    assertThat(discoveryRequests).hasValue(2);
    assertThat(jwksRequests).hasValue(3);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void verifierForIssuerAndKey_limitsRefreshesWithoutBlockingPublishedKeys(
      boolean shouldExpireProvider) {
    respondWithDiscovery(issuer + "/jwks");
    jwksStatus = 200;
    jwksBody =
        String.format(
            "{\"keys\":[%s,%s]}",
            jwkJson("key-1", signingKey), jwkJson("key-2", rotatedSigningKey));
    String token = tokenForKey("key-1", signingKey);
    if (shouldExpireProvider) {
      jwksOperations.verifierForIssuerAndKey(issuer, "key-1", "RS256").verify(token);
      elapsedNanos.set(Duration.ofHours(1).toNanos());
    }
    for (int i = 0; i < 10; i++) {
      String missingKey = "missing-" + i;
      assertThatThrownBy(() -> jwksOperations.verifierForIssuerAndKey(issuer, missingKey, "RS256"))
          .isInstanceOf(OAuthInvalidRequestException.class)
          .hasMessage("Could not get signing key '" + missingKey + "' for issuer " + issuer)
          .extracting(e -> ((BaseException) e).getErrorCode())
          .isEqualTo(ErrorCode.INTERNAL);
    }
    jwksOperations.verifierForIssuerAndKey(issuer, "key-1", "RS256").verify(token);
    jwksOperations
        .verifierForIssuerAndKey(issuer, "key-2", "RS256")
        .verify(tokenForKey("key-2", rotatedSigningKey));
    int expectedRequests = shouldExpireProvider ? 2 : 1;
    assertThat(discoveryRequests).hasValue(expectedRequests);
    assertThat(jwksRequests).hasValue(expectedRequests);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void verifierForIssuerAndKey_sharesConcurrentLoads(boolean shouldRequestUnknownKeys)
      throws Exception {
    respondWithDiscovery(issuer + "/jwks");
    respondWithKey("key-1", signingKey);
    String token = tokenForKey("key-1", signingKey);
    CountDownLatch start = new CountDownLatch(1);
    var executor = Executors.newFixedThreadPool(8);
    try {
      var requests = new ArrayList<Future<?>>();
      for (int i = 0; i < 8; i++) {
        String keyId = shouldRequestUnknownKeys ? "missing-" + i : "key-1";
        requests.add(
            executor.submit(
                () -> {
                  start.await();
                  if (shouldRequestUnknownKeys) {
                    assertThatThrownBy(
                            () -> jwksOperations.verifierForIssuerAndKey(issuer, keyId, "RS256"))
                        .isInstanceOf(OAuthInvalidRequestException.class)
                        .hasMessage(
                            "Could not get signing key '" + keyId + "' for issuer " + issuer);
                    return null;
                  }
                  return jwksOperations
                      .verifierForIssuerAndKey(issuer, keyId, "RS256")
                      .verify(token);
                }));
      }
      start.countDown();
      for (var request : requests) {
        request.get(10, TimeUnit.SECONDS);
      }
      jwksOperations.verifierForIssuerAndKey(issuer, "key-1", "RS256").verify(token);
      assertThat(discoveryRequests).hasValue(1);
      assertThat(jwksRequests).hasValue(1);
    } finally {
      executor.shutdownNow();
    }
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "not JSON|INTERNAL|not valid JSON",
        "{\"issuer\":123}|ABORTED|doesn't match configuration issuer",
        "{\"issuer\":\"%s\",\"jwks_uri\":123}|ABORTED|JWKS configuration missing",
        "{}|ABORTED|Could not get issuer configuration"
      },
      delimiter = '|')
  void loadJwkProvider_delaysRetriesAfterDiscoveryFailure(
      String body, ErrorCode errorCode, String message) {
    respondWith(200, "application/json", body.formatted(issuer));
    OAuthInvalidRequestException firstFailure =
        catchThrowableOfType(
            () -> jwksOperations.loadJwkProvider(issuer), OAuthInvalidRequestException.class);
    assertThat(firstFailure)
        .hasMessageContaining(message)
        .extracting(BaseException::getErrorCode)
        .isEqualTo(errorCode);
    respondWithDiscovery(issuer + "/jwks");
    elapsedNanos.set(Duration.ofSeconds(30).minusNanos(1).toNanos());
    assertThatThrownBy(() -> jwksOperations.loadJwkProvider(issuer))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .isNotSameAs(firstFailure)
        .hasMessageContaining(message)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(errorCode);
    assertThat(discoveryRequests).hasValue(1);

    elapsedNanos.incrementAndGet();
    jwksOperations.loadJwkProvider(issuer);
    assertThat(discoveryRequests).hasValue(2);
  }

  @Test
  void jwksUriForIssuer_checksOriginAndExactAllowlistWithoutHttpsDowngrade() {
    var operations =
        new JwksOperations(
            null,
            List.of("https://www.googleapis.com/oauth2/v3/certs", "http://keys.example.com/jwks"));
    URI google = URI.create("https://accounts.google.com");
    for (String allowed :
        List.of(
            "https://accounts.google.com/jwks",
            "https://ACCOUNTS.GOOGLE.COM:443/jwks?version=1",
            "https://www.googleapis.com/oauth2/v3/certs")) {
      assertThat(operations.jwksUriForIssuer(google, allowed)).isEqualTo(URI.create(allowed));
    }
    for (String rejected :
        List.of(
            "https://accounts.google.com:8443/jwks",
            "https://other.example.com/jwks",
            "http://accounts.google.com/jwks",
            "http://keys.example.com/jwks",
            "https://www.googleapis.com:443/oauth2/v3/certs",
            "https://www.googleapis.com/other")) {
      assertThatThrownBy(() -> operations.jwksUriForIssuer(google, rejected))
          .isInstanceOf(OAuthInvalidRequestException.class)
          .hasMessage("JWKS URI is not allowed")
          .extracting(e -> ((BaseException) e).getErrorCode())
          .isEqualTo(ErrorCode.ABORTED);
    }
    assertThat(
            operations.jwksUriForIssuer(
                URI.create("http://idp.example.com"), "http://idp.example.com:80/jwks"))
        .isEqualTo(URI.create("http://idp.example.com:80/jwks"));
  }

  @Test
  void jwksUriForIssuer_rejectsInvalidUrls() {
    for (String keysUrl :
        List.of(
            "/jwks",
            "file:///keys.json",
            "https://user@idp.example.com/jwks",
            "https://idp.example.com/jwks#keys",
            "https://idp.example.com:0/jwks",
            "https://idp.example.com:65536/jwks",
            "https:///jwks",
            "https://",
            "not a URL")) {
      assertThatThrownBy(
              () -> jwksOperations.jwksUriForIssuer(URI.create("https://idp.example.com"), keysUrl))
          .isInstanceOf(OAuthInvalidRequestException.class)
          .hasMessage("Invalid identity provider URL")
          .extracting(e -> ((BaseException) e).getErrorCode())
          .isEqualTo(ErrorCode.ABORTED);
      assertThatThrownBy(() -> new JwksOperations(null, List.of(keysUrl)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Invalid server.allowed-jwks-uris entry: " + keysUrl);
    }
  }

  @Test
  void loadJwkProvider_rejectsInvalidIssuerUrlsBeforeDiscovery() {
    for (String invalid :
        List.of(
            issuer + "?query",
            issuer + "#fragment",
            issuer.substring("http://".length()) + "#fragment",
            issuer.replace("127.0.0.1", "user@127.0.0.1"),
            issuer.replace("http://", "ftp://"),
            issuer.replace("http://", "file://"))) {
      assertThatThrownBy(() -> jwksOperations.loadJwkProvider(invalid))
          .isInstanceOf(OAuthInvalidRequestException.class)
          .hasMessageStartingWith("Invalid ")
          .extracting(e -> ((BaseException) e).getErrorCode())
          .isEqualTo(ErrorCode.ABORTED);
    }
    assertThat(discoveryRequests).hasValue(0);
  }

  @Test
  void loadJwkProvider_preservesUppercaseHttpScheme() {
    issuer = issuer.replace("http://", "HTTP://");
    respondWithDiscovery(issuer + "/jwks");
    respondWithKey("key-1", signingKey);

    jwksOperations
        .verifierForIssuerAndKey(issuer, "key-1", "RS256")
        .verify(tokenForKey("key-1", signingKey));
    assertThat(discoveryRequests).hasValue(1);
    assertThat(jwksRequests).hasValue(1);
  }

  @Test
  void loadJwkProvider_rejectsDiscoveryRedirects() {
    respondWith(302, "application/json", "redirect");
    discoveryRedirect = issuer + "/redirect-target";
    assertThatThrownBy(() -> jwksOperations.loadJwkProvider(issuer))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasMessageContaining("HTTP 302");
    assertThat(redirectRequests).hasValue(0);
  }

  @Test
  void verifierForIssuerAndKey_rejectsJwksRedirects() {
    respondWithDiscovery(issuer + "/jwks");
    jwksStatus = 302;
    jwksRedirect = issuer + "/redirect-target";
    assertThatThrownBy(() -> jwksOperations.verifierForIssuerAndKey(issuer, "key-1", "RS256"))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasMessage("Could not get signing key 'key-1' for issuer " + issuer);
    assertThat(jwksRequests).hasValue(1);
    assertThat(redirectRequests).hasValue(0);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void requests_rejectOversizedResponses(boolean isDiscovery) {
    respondWithDiscovery(issuer + "/jwks");
    String oversized = " ".repeat(1024 * 1024 + 1);
    if (isDiscovery) {
      discoveryBody = oversized;
    } else {
      jwksStatus = 200;
      jwksBody = oversized;
    }
    assertThatThrownBy(() -> jwksOperations.verifierForIssuerAndKey(issuer, "key-1", "RS256"))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasRootCauseInstanceOf(ContentTooLargeException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INTERNAL);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void requests_timeOutWhenProviderDoesNotRespond(boolean isDiscovery) {
    respondWithDiscovery(issuer + "/jwks");
    isDiscoveryStalled = isDiscovery;
    isJwksStalled = !isDiscovery;
    assertThatThrownBy(() -> jwksOperations.verifierForIssuerAndKey(issuer, "key-1", "RS256"))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasRootCauseInstanceOf(ResponseTimeoutException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INTERNAL);
    if (isDiscovery) {
      assertThatThrownBy(() -> jwksOperations.loadJwkProvider(issuer))
          .isInstanceOf(OAuthInvalidRequestException.class)
          .hasMessage(
              "Could not get issuer configuration from "
                  + issuer
                  + "/.well-known/openid-configuration")
          .hasNoCause()
          .extracting(e -> ((BaseException) e).getErrorCode())
          .isEqualTo(ErrorCode.INTERNAL);
      assertThat(discoveryRequests).hasValue(1);
    }
  }

  @Test
  void loadJwkProvider_rejectsJwksOnAnotherOrigin() {
    respondWithDiscovery(issuer.replace("127.0.0.1", "localhost") + "/jwks");

    assertThatThrownBy(() -> jwksOperations.loadJwkProvider(issuer))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasMessageContaining("JWKS URI is not allowed")
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.ABORTED);
    assertThat(jwksRequests).hasValue(0);
  }

  private void respondWith(int status, String contentType, String body) {
    discoveryStatus = status;
    discoveryContentType = contentType;
    discoveryBody = body;
  }

  private void respondWithDiscovery(String keysUrl) {
    respondWith(
        200,
        "application/json",
        String.format("{\"issuer\":\"%s\",\"jwks_uri\":\"%s\"}", issuer, keysUrl));
  }

  private void respondWithKey(String keyId, KeyPair keyPair) {
    jwksStatus = 200;
    jwksBody = String.format("{\"keys\":[%s]}", jwkJson(keyId, keyPair));
  }

  private String jwkJson(String keyId, KeyPair keyPair) {
    var key = (RSAPublicKey) keyPair.getPublic();
    byte[] modulus = key.getModulus().toByteArray();
    // RSA generation sets the high bit; BigInteger adds a leading sign byte.
    modulus = Arrays.copyOfRange(modulus, 1, modulus.length);
    Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
    return String.format(
        "{\"kty\":\"RSA\",\"kid\":\"%s\",\"n\":\"%s\",\"e\":\"%s\"}",
        keyId,
        encoder.encodeToString(modulus),
        encoder.encodeToString(key.getPublicExponent().toByteArray()));
  }

  private String tokenForKey(String keyId, KeyPair keyPair) {
    Algorithm algorithm =
        Algorithm.RSA256((RSAPublicKey) keyPair.getPublic(), (RSAPrivateKey) keyPair.getPrivate());
    return JWT.create().withIssuer(issuer).withKeyId(keyId).sign(algorithm);
  }
}
