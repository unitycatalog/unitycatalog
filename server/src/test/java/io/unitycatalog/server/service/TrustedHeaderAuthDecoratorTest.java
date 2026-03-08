package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.netty.util.AsciiString;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.exception.AuthorizationException;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TrustedHeaderAuthDecoratorTest {

  private static final String EMAIL_HEADER = "x-auth-user-email";
  private static final String TEST_EMAIL = "user@example.com";

  @Mock private Repositories repositories;
  @Mock private UserRepository userRepository;
  @Mock private HttpService delegate;
  @Mock private ServiceRequestContext ctx;

  private TrustedHeaderAuthDecorator decorator;

  @BeforeEach
  void setUp() {
    when(repositories.getUserRepository()).thenReturn(userRepository);
    decorator = new TrustedHeaderAuthDecorator(EMAIL_HEADER, repositories);
  }

  @Test
  void missingHeaderThrowsUnauthenticated() {
    HttpRequest req =
        HttpRequest.of(
            RequestHeaders.builder(HttpMethod.GET, "/api/2.1/unity-catalog/catalogs").build());

    assertThatThrownBy(() -> decorator.serve(delegate, ctx, req))
        .isInstanceOf(AuthorizationException.class)
        .hasMessageContaining("No identity header");
  }

  @Test
  void blankHeaderThrowsUnauthenticated() {
    HttpRequest req =
        HttpRequest.of(
            RequestHeaders.builder(HttpMethod.GET, "/api/2.1/unity-catalog/catalogs")
                .add(AsciiString.of(EMAIL_HEADER), "   ")
                .build());

    assertThatThrownBy(() -> decorator.serve(delegate, ctx, req))
        .isInstanceOf(AuthorizationException.class)
        .hasMessageContaining("No identity header");
  }

  @Test
  void unknownUserThrowsPermissionDenied() {
    HttpRequest req =
        HttpRequest.of(
            RequestHeaders.builder(HttpMethod.GET, "/api/2.1/unity-catalog/catalogs")
                .add(AsciiString.of(EMAIL_HEADER), TEST_EMAIL)
                .build());

    when(userRepository.getUserByEmail(TEST_EMAIL)).thenReturn(null);

    assertThatThrownBy(() -> decorator.serve(delegate, ctx, req))
        .isInstanceOf(AuthorizationException.class)
        .hasMessageContaining("User not allowed");
  }

  @Test
  void disabledUserThrowsPermissionDenied() {
    HttpRequest req =
        HttpRequest.of(
            RequestHeaders.builder(HttpMethod.GET, "/api/2.1/unity-catalog/catalogs")
                .add(AsciiString.of(EMAIL_HEADER), TEST_EMAIL)
                .build());

    User disabledUser = new User().email(TEST_EMAIL).state(User.StateEnum.DISABLED);
    when(userRepository.getUserByEmail(TEST_EMAIL)).thenReturn(disabledUser);

    assertThatThrownBy(() -> decorator.serve(delegate, ctx, req))
        .isInstanceOf(AuthorizationException.class)
        .hasMessageContaining("User not allowed");
  }

  @Test
  void validUserSetsPrincipalEmailAndDelegates() throws Exception {
    HttpRequest req =
        HttpRequest.of(
            RequestHeaders.builder(HttpMethod.GET, "/api/2.1/unity-catalog/catalogs")
                .add(AsciiString.of(EMAIL_HEADER), TEST_EMAIL)
                .build());

    User enabledUser = new User().email(TEST_EMAIL).state(User.StateEnum.ENABLED);
    when(userRepository.getUserByEmail(TEST_EMAIL)).thenReturn(enabledUser);

    HttpResponse expectedResponse = HttpResponse.of(200);
    when(delegate.serve(ctx, req)).thenReturn(expectedResponse);

    HttpResponse response = decorator.serve(delegate, ctx, req);

    assertThat(response).isSameAs(expectedResponse);
    org.mockito.Mockito.verify(ctx).setAttr(AuthDecorator.PRINCIPAL_EMAIL_ATTR, TEST_EMAIL);
  }
}
