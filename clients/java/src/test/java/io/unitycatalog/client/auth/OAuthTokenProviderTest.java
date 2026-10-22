package io.unitycatalog.client.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.client.internal.RetryingApiClient;
import java.io.IOException;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

public class OAuthTokenProviderTest {

  private static final String OAUTH_URI = "https://oauth.example.com/token";
  private static final String EXCHANGE_URI =
      "https://uc.example.com/api/1.0/unity-control/auth/tokens";
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
    OAuthTokenProvider provider = defaultOAuthTokenProvider();

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
    OAuthTokenProvider provider = defaultOAuthTokenProvider();

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
    OAuthTokenProvider provider = defaultOAuthTokenProvider();

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

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("unchecked")
  public void testAccessTokenHandlesHttpError(boolean exchangeEnabled) throws Exception {
    HttpResponse<String> mockResponse = mock(HttpResponse.class);
    when(mockResponse.statusCode()).thenReturn(401);
    when(mockResponse.body()).thenReturn("Unauthorized");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    OAuthTokenProvider provider =
        oauthTokenProvider(AuthConfigs.DEFAULT_OAUTH_SCOPE, exchangeEnabled ? EXCHANGE_URI : null);

    assertThatThrownBy(provider::accessToken)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Failed to renew OAuth token")
        .hasCauseInstanceOf(IOException.class);
    verifyOAuthRequest(1);
  }

  @Test
  public void testAccessTokenExchangesIdpToken() throws Exception {
    String idpToken = "idp+token&scope=extra/segment%";
    HttpResponse<String> idpResponse = tokenResponse(idpToken, EXPIRES_IN_SECONDS);
    HttpResponse<String> ucResponse = tokenResponse("uc-token", EXPIRES_IN_SECONDS);
    when(mockHttpClient.<String>send(any(HttpRequest.class), any()))
        .thenReturn(idpResponse)
        .thenReturn(ucResponse);
    OAuthTokenProvider provider = oauthTokenProvider(AuthConfigs.DEFAULT_OAUTH_SCOPE, EXCHANGE_URI);

    assertThat(provider.accessToken()).isEqualTo("uc-token");

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockHttpClient, times(2)).send(requestCaptor.capture(), any());
    List<HttpRequest> requests = requestCaptor.getAllValues();
    assertThat(requests.get(0).uri().toString()).isEqualTo(OAUTH_URI);
    assertThat(requests.get(0).headers().firstValue("Authorization")).isPresent();

    HttpRequest exchangeRequest = requests.get(1);
    assertThat(exchangeRequest.uri().toString()).isEqualTo(EXCHANGE_URI);
    assertThat(exchangeRequest.method()).isEqualTo("POST");
    assertThat(exchangeRequest.headers().firstValue("Content-Type"))
        .contains("application/x-www-form-urlencoded");
    assertThat(exchangeRequest.headers().firstValue("Authorization")).isEmpty();
    assertThat(requestForm(exchangeRequest))
        .isEqualTo(
            Map.of(
                "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
                "requested_token_type", "urn:ietf:params:oauth:token-type:access_token",
                "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
                "subject_token", idpToken));
  }

  @ParameterizedTest
  @ValueSource(longs = {10L, 3600L})
  public void testAccessTokenCachesAndRenewsUsingUcExpiration(long idpExpiresIn) throws Exception {
    HttpResponse<String> idpResponse = tokenResponse("idp-token", idpExpiresIn);
    HttpResponse<String> firstUcResponse = tokenResponse("uc-token-1", 60L);
    HttpResponse<String> secondUcResponse = tokenResponse("uc-token-2", 60L);
    when(mockHttpClient.<String>send(any(HttpRequest.class), any()))
        .thenReturn(idpResponse)
        .thenReturn(firstUcResponse)
        .thenReturn(idpResponse)
        .thenReturn(secondUcResponse);
    OAuthTokenProvider provider = oauthTokenProvider(AuthConfigs.DEFAULT_OAUTH_SCOPE, EXCHANGE_URI);

    assertThat(provider.accessToken()).isEqualTo("uc-token-1");
    verifyOAuthRequest(2);

    clock.sleep(Duration.ofSeconds(29));
    assertThat(provider.accessToken()).isEqualTo("uc-token-1");
    verifyOAuthRequest(2);

    clock.sleep(Duration.ofSeconds(2));
    assertThat(provider.accessToken()).isEqualTo("uc-token-2");
    verifyOAuthRequest(4);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAccessTokenFailsWhenExchangeIsRejectedDuringRenewal() throws Exception {
    HttpResponse<String> idpResponse = tokenResponse("idp-token", EXPIRES_IN_SECONDS);
    HttpResponse<String> firstUcResponse = tokenResponse("uc-token-1", 60L);
    HttpResponse<String> failedResponse = mock(HttpResponse.class);
    when(failedResponse.statusCode()).thenReturn(401);
    when(failedResponse.body()).thenReturn("Exchange rejected");
    HttpResponse<String> secondUcResponse = tokenResponse("uc-token-2", 60L);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(
            idpResponse,
            firstUcResponse,
            idpResponse,
            failedResponse,
            idpResponse,
            secondUcResponse);
    OAuthTokenProvider provider = oauthTokenProvider(AuthConfigs.DEFAULT_OAUTH_SCOPE, EXCHANGE_URI);

    assertThat(provider.accessToken()).isEqualTo("uc-token-1");
    clock.sleep(Duration.ofSeconds(31));

    assertThatThrownBy(provider::accessToken)
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Failed to renew OAuth token")
        .hasCauseInstanceOf(IOException.class)
        .hasRootCauseMessage(
            "Failed to fetch OAuth token. HTTP status: 401, Response: Exchange rejected");
    verifyOAuthRequest(4);

    assertThat(provider.accessToken()).isEqualTo("uc-token-2");
    verifyOAuthRequest(6);
    assertThat(provider.accessToken()).isEqualTo("uc-token-2");
    verifyOAuthRequest(6);
  }

  @ParameterizedTest
  @ValueSource(strings = {"not-json", "{}", "{\"access_token\":\"uc-token\"}"})
  @SuppressWarnings("unchecked")
  public void testAccessTokenRejectsInvalidExchangeResponse(String responseBody) throws Exception {
    HttpResponse<String> idpResponse = tokenResponse("idp-token", EXPIRES_IN_SECONDS);
    HttpResponse<String> invalidResponse = mock(HttpResponse.class);
    when(invalidResponse.statusCode()).thenReturn(200);
    when(invalidResponse.body()).thenReturn(responseBody);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(idpResponse, invalidResponse);
    OAuthTokenProvider provider = oauthTokenProvider(AuthConfigs.DEFAULT_OAUTH_SCOPE, EXCHANGE_URI);

    assertThatThrownBy(provider::accessToken)
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Failed to renew OAuth token");
    verifyOAuthRequest(2);
  }

  @Test
  public void testAccessTokenRenewsImmediatelyWhenExpired() throws Exception {
    mockOAuthResponse(ACCESS_TOKEN_1, 10L);
    OAuthTokenProvider provider = defaultOAuthTokenProvider();

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
                new OAuthTokenProvider(
                    null,
                    CLIENT_ID,
                    CLIENT_SECRET,
                    AuthConfigs.DEFAULT_OAUTH_SCOPE,
                    null,
                    30L,
                    mockRetryingApiClient,
                    clock))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("OAuth URI must not be null");

    assertThatThrownBy(
            () ->
                new OAuthTokenProvider(
                    OAUTH_URI,
                    null,
                    CLIENT_SECRET,
                    AuthConfigs.DEFAULT_OAUTH_SCOPE,
                    null,
                    30L,
                    mockRetryingApiClient,
                    clock))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("OAuth client ID must not be null");

    assertThatThrownBy(
            () ->
                new OAuthTokenProvider(
                    OAUTH_URI,
                    CLIENT_ID,
                    null,
                    AuthConfigs.DEFAULT_OAUTH_SCOPE,
                    null,
                    30L,
                    mockRetryingApiClient,
                    clock))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("OAuth client secret must not be null");

    assertThatThrownBy(
            () ->
                new OAuthTokenProvider(
                    OAUTH_URI,
                    CLIENT_ID,
                    CLIENT_SECRET,
                    AuthConfigs.DEFAULT_OAUTH_SCOPE,
                    null,
                    -1L,
                    mockRetryingApiClient,
                    clock))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Lead renewal time must be non-negative");
  }

  @Test
  public void testAccessTokenUsesConfiguredScope() throws Exception {
    mockOAuthResponse(ACCESS_TOKEN_1, EXPIRES_IN_SECONDS);
    String customScope = "https://example.com/.default";
    OAuthTokenProvider provider = oauthTokenProvider(customScope);

    provider.accessToken();

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockHttpClient).send(requestCaptor.capture(), any());
    String body = requestBody(requestCaptor.getValue());
    assertThat(body)
        .isEqualTo("grant_type=client_credentials&scope=https%3A%2F%2Fexample.com%2F.default");
  }

  @Test
  public void testAccessTokenNormalizesAndEncodesSpaceSeparatedScopes() throws Exception {
    mockOAuthResponse(ACCESS_TOKEN_1, EXPIRES_IN_SECONDS);
    String customScope = "  https://example.com/.default  \t catalog.tables:read  ";
    OAuthTokenProvider provider = oauthTokenProvider(customScope);

    provider.accessToken();

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockHttpClient).send(requestCaptor.capture(), any());
    assertThat(requestBody(requestCaptor.getValue()))
        .isEqualTo(
            "grant_type=client_credentials&scope="
                + "https%3A%2F%2Fexample.com%2F.default+catalog.tables%3Aread");
  }

  @Test
  public void testBlankScopeIsRejected() {
    assertThatThrownBy(() -> oauthTokenProvider("   "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Configuration key 'oauth.scope' must not be empty");
  }

  @Test
  public void testAccessTokenDefaultsScopeToAllApis() throws Exception {
    mockOAuthResponse(ACCESS_TOKEN_1, EXPIRES_IN_SECONDS);
    OAuthTokenProvider provider = defaultOAuthTokenProvider();

    provider.accessToken();

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockHttpClient).send(requestCaptor.capture(), any());
    assertThat(requestBody(requestCaptor.getValue())).contains("scope=all-apis");
  }

  private OAuthTokenProvider defaultOAuthTokenProvider() {
    return oauthTokenProvider(AuthConfigs.DEFAULT_OAUTH_SCOPE);
  }

  private OAuthTokenProvider oauthTokenProvider(String oauthScope) {
    return oauthTokenProvider(oauthScope, null);
  }

  private OAuthTokenProvider oauthTokenProvider(String oauthScope, String exchangeUri) {
    return new OAuthTokenProvider(
        OAUTH_URI,
        CLIENT_ID,
        CLIENT_SECRET,
        oauthScope,
        exchangeUri,
        30L,
        mockRetryingApiClient,
        clock);
  }

  private static Map<String, String> requestForm(HttpRequest request) throws InterruptedException {
    return Arrays.stream(requestBody(request).split("&"))
        .map(parameter -> parameter.split("=", 2))
        .collect(
            Collectors.toMap(
                parameter -> URLDecoder.decode(parameter[0], StandardCharsets.UTF_8),
                parameter -> URLDecoder.decode(parameter[1], StandardCharsets.UTF_8)));
  }

  /** Reads the string body of an HttpRequest built from a BodyPublishers.ofString publisher. */
  private static String requestBody(HttpRequest request) throws InterruptedException {
    HttpRequest.BodyPublisher publisher = request.bodyPublisher().orElseThrow();
    StringBuilder body = new StringBuilder();
    CountDownLatch latch = new CountDownLatch(1);
    publisher.subscribe(
        new Flow.Subscriber<ByteBuffer>() {
          @Override
          public void onSubscribe(Flow.Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
          }

          @Override
          public void onNext(ByteBuffer item) {
            body.append(StandardCharsets.UTF_8.decode(item));
          }

          @Override
          public void onError(Throwable throwable) {
            latch.countDown();
          }

          @Override
          public void onComplete() {
            latch.countDown();
          }
        });
    latch.await(5, TimeUnit.SECONDS);
    return body.toString();
  }

  /** Helper method to mock a successful OAuth response. */
  private void mockOAuthResponse(String accessToken, long expiresIn) throws Exception {
    HttpResponse<String> mockResponse = tokenResponse(accessToken, expiresIn);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);
  }

  @SuppressWarnings("unchecked")
  private HttpResponse<String> tokenResponse(String accessToken, long expiresIn) {
    String jsonResponse =
        String.format(
            "{\"access_token\":\"%s\",\"token_type\":\"Bearer\",\"expires_in\":%d}",
            accessToken, expiresIn);

    HttpResponse<String> mockResponse = mock(HttpResponse.class);
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(jsonResponse);
    return mockResponse;
  }

  /** Helper method to verify OAuth request was made expected number of times. */
  private void verifyOAuthRequest(int times) throws Exception {
    verify(mockHttpClient, times(times))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }
}
