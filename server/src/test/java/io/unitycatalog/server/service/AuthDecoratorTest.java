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
import io.unitycatalog.server.security.SecurityConfiguration;
import io.unitycatalog.server.security.SecurityContext;
import java.nio.file.Path;
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

/**
 * How {@link AuthDecorator} answers a valid internal token whose revocation check or user lookup
 * fails, or finds no enabled user. The token is real, signed with keys generated in a temp folder;
 * only the database is mocked.
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
