package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.model.CreateUser;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests for {@link TrustedHeaderAuthDecorator}: the server is configured with {@code
 * server.authentication.mode=trusted-header} and requests carry identity in a trusted header rather
 * than a JWT. Each test drives a real HTTP request through the full decorator chain.
 */
public class TrustedHeaderAuthDecoratorTest extends BaseServerTest {

  private static final String TRUSTED_HEADER = "X-Forwarded-Email";
  private static final String KNOWN_USER_EMAIL = "trusted-user@example.com";
  // listCatalogs is authenticated and response-filtered, so any authenticated user gets 200 with a
  // (possibly empty) list. This exercises identity resolution without needing privilege grants.
  private static final String CATALOGS_ENDPOINT = "/api/2.1/unity-catalog/catalogs";

  private WebClient client;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.AUTHORIZATION_ENABLED.getKey(), "enable");
    serverProperties.setProperty(Property.AUTHENTICATION_MODE.getKey(), "trusted-header");
    serverProperties.setProperty(Property.TRUSTED_HEADER_AUTH_NAME.getKey(), TRUSTED_HEADER);
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    client = WebClient.builder(serverConfig.getServerUrl()).build();

    // Seed an ENABLED user directly against the same database the server uses.
    UserRepository userRepository =
        new Repositories(
                hibernateConfigurator.getSessionFactory(), new ServerProperties(serverProperties))
            .getUserRepository();
    userRepository.createUser(
        CreateUser.builder().name("Trusted User").email(KNOWN_USER_EMAIL).build());
  }

  private AggregatedHttpResponse listCatalogs(String headerEmail) {
    RequestHeadersBuilder builder =
        RequestHeaders.builder().method(HttpMethod.GET).path(CATALOGS_ENDPOINT);
    if (headerEmail != null) {
      builder.add(TRUSTED_HEADER, headerEmail);
    }
    return client.execute(builder.build()).aggregate().join();
  }

  @Test
  public void testKnownUserIsAllowed() {
    AggregatedHttpResponse response = listCatalogs(KNOWN_USER_EMAIL);
    assertThat(response.status()).isEqualTo(HttpStatus.OK);
  }

  @Test
  public void testUnknownUserIsForbidden() {
    AggregatedHttpResponse response = listCatalogs("nobody@example.com");
    assertThat(response.status()).isEqualTo(HttpStatus.FORBIDDEN);
  }

  @Test
  public void testMissingHeaderIsUnauthorized() {
    AggregatedHttpResponse response = listCatalogs(null);
    assertThat(response.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
  }

  @Test
  public void testDisabledUserIsForbidden() {
    // Create then disable a user (deleteUser sets state to DISABLED), and verify the header for
    // that user is rejected.
    String disabledEmail = "disabled-user@example.com";
    UserRepository userRepository =
        new Repositories(
                hibernateConfigurator.getSessionFactory(), new ServerProperties(serverProperties))
            .getUserRepository();
    User created =
        userRepository.createUser(
            CreateUser.builder().name("Disabled User").email(disabledEmail).build());
    userRepository.deleteUser(created.getId());

    AggregatedHttpResponse response = listCatalogs(disabledEmail);
    assertThat(response.status()).isEqualTo(HttpStatus.FORBIDDEN);
  }
}
