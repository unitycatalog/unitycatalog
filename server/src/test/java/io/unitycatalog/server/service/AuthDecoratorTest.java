package io.unitycatalog.server.service;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.AlgorithmMismatchException;
import com.auth0.jwt.exceptions.SignatureVerificationException;
import com.auth0.jwt.exceptions.TokenExpiredException;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.exception.AuthorizationException;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.TokenRevocationRepository;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.security.JwtTokenType;
import io.unitycatalog.server.security.SecurityConfiguration;
import io.unitycatalog.server.security.SecurityContext;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import lombok.SneakyThrows;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * How {@link AuthDecorator} answers a valid internal token whose revocation check or user lookup
 * fails, or finds no enabled user, and an internal token signed with another key. The token is
 * real, signed with keys generated in a temp folder; only the database is mocked.
 */
class AuthDecoratorTest {

  private static final String EMAIL = "user@example.com";

  private final UserRepository userRepository = mock();
  private final TokenRevocationRepository tokenRevocationRepository = mock();
  private final HttpService delegate = mock();
  private AuthDecorator decorator;
  private SecurityContext securityContext;
  private String token;
  // Shared by all tests, so the signing keys are generated once.
  @TempDir static Path configurationFolder;

  @BeforeEach
  void setUp() {
    securityContext =
        new SecurityContext(
            configurationFolder,
            new SecurityConfiguration(configurationFolder),
            "server",
            INTERNAL);
    SessionFactory sessionFactory = mock();
    Session session = mock();
    when(sessionFactory.openSession()).thenReturn(session);
    when(session.beginTransaction()).thenReturn(mock(Transaction.class));
    Repositories repositories = mock();
    when(repositories.getUserRepository()).thenReturn(userRepository);
    when(repositories.getTokenRevocationRepository()).thenReturn(tokenRevocationRepository);
    when(repositories.getSessionFactory()).thenReturn(sessionFactory);
    decorator = new AuthDecorator(securityContext, repositories);
    token = securityContext.createAccessToken(EMAIL, Duration.ofMinutes(5));
  }

  @Test
  void missingUserIsDenied() {
    assertDenied(ErrorCode.PERMISSION_DENIED, "User not allowed: " + EMAIL);
  }

  @Test
  void revokedTokenIsUnauthenticated() {
    when(tokenRevocationRepository.isRevoked(any(), any())).thenReturn(true);

    assertDenied(ErrorCode.UNAUTHENTICATED, "Access token has been revoked.");
    verify(userRepository, never()).getUserByEmail(any(), any());
  }

  @Test
  void databaseFailureKeepsItsMessageOutOfTheResponse() {
    // A database failure is not the caller's fault: it is a 500, not a 403, and its message stays
    // in the server log (the cause) rather than in the response.
    RuntimeException failure = new RuntimeException("database detail");
    when(userRepository.getUserByEmail(any(), eq(EMAIL))).thenThrow(failure);

    assertThatThrownBy(this::serve)
        .isExactlyInstanceOf(BaseException.class)
        .hasMessage("Failed to authorize request")
        .hasRootCause(failure)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INTERNAL);
    verifyNotDelegated();
  }

  @Test
  void tokenExpiringDuringTheLookupIsRejected() {
    // The cleanup may purge an expired token's revocation row while the lookup runs, so a token
    // that expires by then must be rejected even though no row was found.
    token = securityContext.createAccessToken(EMAIL, Duration.ofSeconds(2));
    Instant expiresAt = JWT.decode(token).getExpiresAtAsInstant();
    when(userRepository.getUserByEmail(any(), eq(EMAIL)))
        .thenAnswer(
            invocation -> {
              Thread.sleep(Duration.between(Instant.now(), expiresAt).plusMillis(100).toMillis());
              return user(User.StateEnum.ENABLED);
            });

    assertThatThrownBy(this::serve).isInstanceOf(TokenExpiredException.class);
    // The lookup ran, so the check after it rejected the token, not the first verify.
    verify(userRepository).getUserByEmail(any(), eq(EMAIL));
    verifyNotDelegated();
  }

  @Test
  void disabledUserIsDenied() {
    when(userRepository.getUserByEmail(any(), eq(EMAIL))).thenReturn(user(User.StateEnum.DISABLED));

    assertDenied(ErrorCode.PERMISSION_DENIED, "User not allowed: " + EMAIL);
  }

  @Test
  void enabledUserIsServed() throws Exception {
    when(userRepository.getUserByEmail(any(), eq(EMAIL))).thenReturn(user(User.StateEnum.ENABLED));
    HttpResponse response = HttpResponse.of(200);
    when(delegate.serve(any(), any())).thenReturn(response);

    assertThat(serve()).isSameAs(response);
  }

  @Test
  void tokenSignedWithAnotherKeyIsRejected() throws Exception {
    when(userRepository.getUserByEmail(any(), eq(EMAIL))).thenReturn(user(User.StateEnum.ENABLED));
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    KeyPair otherKey = generator.generateKeyPair();
    token =
        JWT.create()
            .withSubject("server")
            .withIssuer(INTERNAL)
            // The key ID is ignored: the token is checked against the server's own key.
            .withKeyId("another-key-id")
            .withClaim(JwtClaim.TOKEN_TYPE.key(), JwtTokenType.ACCESS.name())
            .withClaim(JwtClaim.SUBJECT.key(), EMAIL)
            .sign(
                Algorithm.RSA512(
                    (RSAPublicKey) otherKey.getPublic(), (RSAPrivateKey) otherKey.getPrivate()));

    assertThatThrownBy(this::serve).isInstanceOf(SignatureVerificationException.class);
    verifyNotDelegated();
  }

  // The server's own key, but another algorithm: only RS512 is accepted.
  @ParameterizedTest
  @ValueSource(strings = {"RS256", "RS384", "HS512", "none"})
  void tokenSignedWithAnotherAlgorithmIsRejected(String algorithm) throws Exception {
    when(userRepository.getUserByEmail(any(), eq(EMAIL))).thenReturn(user(User.StateEnum.ENABLED));
    SecurityConfiguration keys = new SecurityConfiguration(configurationFolder);
    Algorithm signer =
        switch (algorithm) {
          case "RS256" -> Algorithm.RSA256(keys.rsaPublicKey(), keys.rsaPrivateKey());
          case "RS384" -> Algorithm.RSA384(keys.rsaPublicKey(), keys.rsaPrivateKey());
          case "HS512" -> Algorithm.HMAC512(keys.rsaPublicKey().getEncoded());
          default -> Algorithm.none();
        };
    token =
        JWT.create()
            .withSubject("server")
            .withIssuer(INTERNAL)
            .withClaim(JwtClaim.TOKEN_TYPE.key(), JwtTokenType.ACCESS.name())
            .withClaim(JwtClaim.SUBJECT.key(), EMAIL)
            .sign(signer);

    assertThatThrownBy(this::serve).isInstanceOf(AlgorithmMismatchException.class);
    verifyNotDelegated();
  }

  private static UserDAO user(User.StateEnum state) {
    return UserDAO.builder()
        .id(UUID.randomUUID())
        .email(EMAIL)
        .state(state.name())
        .createdAt(new Date())
        .build();
  }

  private void assertDenied(ErrorCode errorCode, String message) {
    assertThatThrownBy(this::serve)
        .isInstanceOf(AuthorizationException.class)
        .hasMessage(message)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(errorCode);
    verifyNotDelegated();
  }

  private HttpResponse serve() throws Exception {
    HttpRequest request =
        HttpRequest.of(
            RequestHeaders.builder(HttpMethod.GET, "/api/2.1/unity-catalog/catalogs")
                .add(HttpHeaderNames.AUTHORIZATION, "Bearer " + token)
                .build());
    return decorator.serve(delegate, ServiceRequestContext.of(request), request);
  }

  @SneakyThrows
  private void verifyNotDelegated() {
    verify(delegate, never()).serve(any(), any());
  }

  @ParameterizedTest
  @EnumSource(
      value = HttpMethod.class,
      names = {"GET", "HEAD", "OPTIONS"})
  void safeMethodsDoNotRequireCsrfHeader(HttpMethod method) {
    assertThatCode(() -> AuthDecorator.validateCsrfHeader(RequestHeaders.of(method, "/")))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @EnumSource(
      value = HttpMethod.class,
      names = {"GET", "HEAD", "OPTIONS", "UNKNOWN"},
      mode = EnumSource.Mode.EXCLUDE)
  void unsafeMethodsRequireCsrfHeader(HttpMethod method) {
    RequestHeaders headers = RequestHeaders.of(method, "/");
    assertThatThrownBy(() -> AuthDecorator.validateCsrfHeader(headers))
        .isInstanceOf(AuthorizationException.class)
        .hasMessage("Cookie requests require X-Requested-With: XMLHttpRequest")
        .extracting(e -> ((AuthorizationException) e).getErrorCode())
        .isEqualTo(ErrorCode.PERMISSION_DENIED);

    assertThatCode(
            () ->
                AuthDecorator.validateCsrfHeader(
                    headers.toBuilder()
                        .set(HttpHeaderNames.X_REQUESTED_WITH, "XMLHttpRequest")
                        .build()))
        .doesNotThrowAnyException();
  }
}
