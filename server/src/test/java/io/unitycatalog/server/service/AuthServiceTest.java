package io.unitycatalog.server.service;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import com.linecorp.armeria.common.auth.AuthToken;
import io.unitycatalog.server.base.BaseServerTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AuthServiceTest extends BaseServerTest {

  private static final String LOGOUT_ENDPOINT = "/api/1.0/unity-control/auth/logout";
  private WebClient client;
  private static final String EMPTY_RESPONSE = "{}";

  @BeforeEach
  public void setUp() {
    super.setUp();
    String uri = serverConfig.getServerUrl();
    String token = serverConfig.getAuthToken();
    client = WebClient.builder(uri).auth(AuthToken.ofOAuth2(token)).build();
  }

  @Test
  public void testLogout() {

    // Logout with cookie should return status as 200 and  empty json content
    RequestHeaders headersWithCookie = buildRequestHeader(true);

    AggregatedHttpResponse response = client.execute(headersWithCookie).aggregate().join();
    assertEquals(HttpStatus.OK, response.status());
    assertThat(response.contentUtf8()).isEqualTo(EMPTY_RESPONSE);

    // Logout without cookie should return 200 status
    RequestHeaders headersWithoutCookie = buildRequestHeader(false);
    response = client.execute(headersWithoutCookie).aggregate().join();
    assertEquals(HttpStatus.OK, response.status());
    assertThat(response.contentUtf8()).isEqualTo(EMPTY_RESPONSE);
  }

  public RequestHeaders buildRequestHeader(boolean includeCookie) {
    RequestHeadersBuilder builder =
        RequestHeaders.builder()
            .method(HttpMethod.POST)
            .path(LOGOUT_ENDPOINT)
            .contentType(MediaType.JSON);

    if (includeCookie) {
      builder.add(HttpHeaderNames.COOKIE, "UC_TOKEN=1234");
    }

    return builder.build();
  }
}
