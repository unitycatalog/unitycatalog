package io.unitycatalog.server.service;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.SignatureVerificationException;
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
import io.unitycatalog.server.persist.UserRepository;
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
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * How {@link AuthDecorator} answers a valid internal token whose user lookup fails or finds no
 * enabled user, and an internal token signed with another key. The token is real, signed with keys
 * generated in a temp folder; only the user repository is mocked.
 */
class AuthDecoratorTest {

  private static final String EMAIL = "user@example.com";

  private final UserRepository userRepository = mock();
  private final HttpService delegate = mock();
  private SecurityContext securityContext;
  private AuthDecorator decorator;
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
    Repositories repositories = mock();
    when(repositories.getUserRepository()).thenReturn(userRepository);
    decorator = new AuthDecorator(securityContext, repositories);
    token = securityContext.createAccessToken(EMAIL, Duration.ofMinutes(5));
  }

  static Stream<Arguments> lookupFailures() {
    BaseException notFound = new BaseException(ErrorCode.NOT_FOUND, "User not found: " + EMAIL);
    BaseException internal = new BaseException(ErrorCode.INTERNAL, "Failed to get user by email");
    IllegalStateException unexpected = new IllegalStateException("unexpected");
    return Stream.of(
        // A missing user is a denial.
        Arguments.of(notFound, AuthorizationException.class, ErrorCode.PERMISSION_DENIED),
        // A database failure is not the caller's fault: it keeps its error code, not a 403.
        Arguments.of(internal, BaseException.class, ErrorCode.INTERNAL),
        // Anything else is not a lookup result at all and propagates as is.
        Arguments.of(unexpected, IllegalStateException.class, null));
  }

  @ParameterizedTest
  @MethodSource("lookupFailures")
  void lookupFailureIsDeniedOnlyWhenTheUserIsMissing(
      RuntimeException failure, Class<? extends Throwable> expectedType, ErrorCode expectedCode) {
    when(userRepository.getUserByEmail(EMAIL)).thenThrow(failure);

    var thrown = assertThatThrownBy(this::serve).isInstanceOf(expectedType);
    if (expectedCode == null) {
      thrown.isSameAs(failure);
    } else {
      thrown.extracting(e -> ((BaseException) e).getErrorCode()).isEqualTo(expectedCode);
    }
    verifyNotDelegated();
  }

  @Test
  void disabledUserIsDenied() {
    when(userRepository.getUserByEmail(EMAIL))
        .thenReturn(new User().email(EMAIL).state(User.StateEnum.DISABLED));

    assertThatThrownBy(this::serve)
        .isInstanceOf(AuthorizationException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.PERMISSION_DENIED);
    verifyNotDelegated();
  }

  static Stream<Arguments> tokensWithoutKnownTypeOrExpiry() {
    String typeMessage =
        "Access token has a missing or unknown type. Get a new token from this server.";
    UnaryOperator<JWTCreator.Builder> expiring =
        b -> b.withExpiresAt(Instant.now().plus(Duration.ofMinutes(5)));
    return Stream.of(
        Arguments.of("no type", expiring, typeMessage),
        Arguments.of(
            "unknown type",
            (UnaryOperator<JWTCreator.Builder>)
                b -> expiring.apply(b).withClaim(JwtClaim.TOKEN_TYPE.key(), "BOGUS"),
            typeMessage),
        Arguments.of(
            "lower-case type",
            (UnaryOperator<JWTCreator.Builder>)
                b -> expiring.apply(b).withClaim(JwtClaim.TOKEN_TYPE.key(), "access"),
            typeMessage),
        Arguments.of(
            "ACCESS without exp",
            (UnaryOperator<JWTCreator.Builder>)
                b -> b.withClaim(JwtClaim.TOKEN_TYPE.key(), JwtTokenType.ACCESS.name()),
            "Access token has no expiry. Get a new token from this server."));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("tokensWithoutKnownTypeOrExpiry")
  void tokenWithoutKnownTypeOrExpiryIsUnauthenticated(
      String name, UnaryOperator<JWTCreator.Builder> claims, String message) {
    token = signedToken(claims);

    assertThatThrownBy(this::serve)
        .isInstanceOf(AuthorizationException.class)
        .hasMessage(message)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.UNAUTHENTICATED);
    verify(userRepository, never()).getUserByEmail(any());
    verifyNotDelegated();
  }

  @Test
  void serviceTokenWithoutExpiryIsServed() throws Exception {
    // The bootstrap admin token has no expiry; it is valid while the signing key is.
    token = securityContext.createServiceToken();
    when(userRepository.getUserByEmail("admin"))
        .thenReturn(new User().email("admin").state(User.StateEnum.ENABLED));
    HttpResponse response = HttpResponse.of(200);
    when(delegate.serve(any(), any())).thenReturn(response);

    assertThat(serve()).isSameAs(response);
  }

  /** A token signed with the server's key, with the given type and expiry claims. */
  private String signedToken(UnaryOperator<JWTCreator.Builder> claims) {
    JWTCreator.Builder builder =
        JWT.create().withIssuer(INTERNAL).withClaim(JwtClaim.SUBJECT.key(), EMAIL);
    return claims.apply(builder).sign(securityContext.getAlgorithm());
  }

  @Test
  void enabledUserIsServed() throws Exception {
    when(userRepository.getUserByEmail(EMAIL))
        .thenReturn(new User().email(EMAIL).state(User.StateEnum.ENABLED));
    HttpResponse response = HttpResponse.of(200);
    when(delegate.serve(any(), any())).thenReturn(response);

    assertThat(serve()).isSameAs(response);
  }

  @Test
  void tokenSignedWithAnotherKeyIsRejected() throws Exception {
    when(userRepository.getUserByEmail(EMAIL))
        .thenReturn(new User().email(EMAIL).state(User.StateEnum.ENABLED));
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    KeyPair otherKey = generator.generateKeyPair();
    token =
        JWT.create()
            .withSubject("server")
            .withIssuer(INTERNAL)
            // A key ID the server never had: earlier versions failed the lookup with a 500.
            .withKeyId("another-key-id")
            .withClaim(JwtClaim.TOKEN_TYPE.key(), JwtTokenType.ACCESS.name())
            .withClaim(JwtClaim.SUBJECT.key(), EMAIL)
            .sign(
                Algorithm.RSA512(
                    (RSAPublicKey) otherKey.getPublic(), (RSAPrivateKey) otherKey.getPrivate()));

    assertThatThrownBy(this::serve).isInstanceOf(SignatureVerificationException.class);
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
