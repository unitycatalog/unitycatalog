package io.unitycatalog.spark.auth.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.spark.RetryingApiClient;
import io.unitycatalog.spark.utils.Clock;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class OAuthUCTokenProviderTest {

  private static final String OAUTH_URI = "https://oauth.example.com/token";
  private static final String CLIENT_ID = "test-client-id";
  private static final String CLIENT_SECRET = "test-client-secret";
  private static final String ACCESS_TOKEN_1 = "access-token-1";
  private static final String ACCESS_TOKEN_2 = "access-token-2";
  private static final long EXPIRES_IN_SECONDS = 3600L;

  private HttpClient mockHttpClient;
  private RetryingApiClient mockRetryingApiClient;
  private Clock.ManualClock clock;

  @BeforeEach
  public void setUp() {
    mockHttpClient = mock(HttpClient.class);
    mockRetryingApiClient = mock(RetryingApiClient.class);
    when(mockRetryingApiClient.getHttpClient()).thenReturn(mockHttpClient);
    clock = (Clock.ManualClock) Clock.manualClock(Instant.now());
  }

  @Test
  public void testAccessTokenCachesTokenAndReusesIt() throws Exception {
    mockOAuthResponse(ACCESS_TOKEN_1, EXPIRES_IN_SECONDS);
    OAuthUCTokenProvider provider =
        new OAuthUCTokenProvider(
            OAUTH_URI, CLIENT_ID, CLIENT_SECRET, 30L, mockRetryingApiClient, clock);

    String token1 = provider.accessToken();
    verifyOAuthRequest(1);

    String token2 = provider.accessToken();
    verifyOAuthRequest(1);

    String token3 = provider.accessToken();
    verifyOAuthRequest(1);

    assertThat(token1).isEqualTo(ACCESS_TOKEN_1);
    assertThat(token2).isEqualTo(ACCESS_TOKEN_1);
    assertThat(token3).isEqualTo(ACCESS_TOKEN_1);
  }

  @Test
  public void testAccessTokenRenewsTokenBeforeExpiration() throws Exception {
    mockOAuthResponse(ACCESS_TOKEN_1, 60L);
    OAuthUCTokenProvider provider =
        new OAuthUCTokenProvider(
            OAUTH_URI, CLIENT_ID, CLIENT_SECRET, 30L, mockRetryingApiClient, clock);

    String token1 = provider.accessToken();
    assertThat(token1).isEqualTo(ACCESS_TOKEN_1);
    verifyOAuthRequest(1);

    // Token still valid after 29 seconds (renewal lead time is 30 seconds)
    clock.sleep(Duration.ofSeconds(29));
    String token2 = provider.accessToken();
    assertThat(token2).isEqualTo(ACCESS_TOKEN_1);
    verifyOAuthRequest(1);

    // After 31 seconds total, token should be renewed
    clock.sleep(Duration.ofSeconds(2));
    mockOAuthResponse(ACCESS_TOKEN_2, 60L);

    String token3 = provider.accessToken();
    assertThat(token3).isEqualTo(ACCESS_TOKEN_2);
    verifyOAuthRequest(2);
  }

  @Test
  public void testAccessTokenSendsCorrectHttpRequest() throws Exception {
    mockOAuthResponse(ACCESS_TOKEN_1, EXPIRES_IN_SECONDS);
    OAuthUCTokenProvider provider =
        new OAuthUCTokenProvider(
            OAUTH_URI, CLIENT_ID, CLIENT_SECRET, 30L, mockRetryingApiClient, clock);

    provider.accessToken();

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockHttpClient).send(requestCaptor.capture(), any());

    HttpRequest request = requestCaptor.getValue();
    assertThat(request.uri().toString()).isEqualTo(OAUTH_URI);
    assertThat(request.method()).isEqualTo("POST");
    assertThat(request.headers().firstValue("Authorization"))
        .isPresent()
        .get()
        .asString()
        .startsWith("Basic ");
    assertThat(request.headers().firstValue("Content-Type"))
        .isPresent()
        .get()
        .isEqualTo("application/x-www-form-urlencoded");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAccessTokenHandlesHttpError() throws Exception {
    HttpResponse<String> mockResponse = mock(HttpResponse.class);
    when(mockResponse.statusCode()).thenReturn(401);
    when(mockResponse.body()).thenReturn("Unauthorized");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    OAuthUCTokenProvider provider =
        new OAuthUCTokenProvider(
            OAUTH_URI, CLIENT_ID, CLIENT_SECRET, 30L, mockRetryingApiClient, clock);

    assertThatThrownBy(provider::accessToken)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Failed to renew OAuth token")
        .hasCauseInstanceOf(IOException.class);
  }

  @Test
  public void testAccessTokenRenewsImmediatelyWhenExpired() throws Exception {
    mockOAuthResponse(ACCESS_TOKEN_1, 10L);
    OAuthUCTokenProvider provider =
        new OAuthUCTokenProvider(
            OAUTH_URI, CLIENT_ID, CLIENT_SECRET, 30L, mockRetryingApiClient, clock);

    String token1 = provider.accessToken();
    assertThat(token1).isEqualTo(ACCESS_TOKEN_1);
    verifyOAuthRequest(1);

    // Token needs immediate renewal (expiry 10s < lead time 30s)
    mockOAuthResponse(ACCESS_TOKEN_2, 10L);

    String token2 = provider.accessToken();
    assertThat(token2).isEqualTo(ACCESS_TOKEN_2);
    verifyOAuthRequest(2);
  }

  @Test
  public void testConstructor() {
    assertThatThrownBy(
            () ->
                new OAuthUCTokenProvider(
                    null, CLIENT_ID, CLIENT_SECRET, 30L, mockRetryingApiClient, clock))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("OAuth URI must not be null");

    assertThatThrownBy(
            () ->
                new OAuthUCTokenProvider(
                    OAUTH_URI, null, CLIENT_SECRET, 30L, mockRetryingApiClient, clock))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("OAuth client ID must not be null");

    assertThatThrownBy(
            () ->
                new OAuthUCTokenProvider(
                    OAUTH_URI, CLIENT_ID, null, 30L, mockRetryingApiClient, clock))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("OAuth client secret must not be null");

    assertThatThrownBy(
            () ->
                new OAuthUCTokenProvider(
                    OAUTH_URI, CLIENT_ID, CLIENT_SECRET, -1L, mockRetryingApiClient, clock))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Lead renewal time must be non-negative");
  }

  /** Helper method to mock a successful OAuth response. */
  private void mockOAuthResponse(String accessToken, long expiresIn) throws Exception {
    String jsonResponse =
        String.format(
            "{\"access_token\":\"%s\",\"token_type\":\"Bearer\",\"expires_in\":%d}",
            accessToken, expiresIn);

    @SuppressWarnings("unchecked")
    HttpResponse<String> mockResponse = mock(HttpResponse.class);
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(jsonResponse);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);
  }

  /** Helper method to verify OAuth request was made expected number of times. */
  private void verifyOAuthRequest(int times) throws Exception {
    verify(mockHttpClient, times(times))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }
}
