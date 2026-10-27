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
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class OAuthTokenProviderTest {

  private static final String OAUTH_URI = "https://oauth.example.com/token";
  private static final String CLIENT_ID = "test-client-id";
  private static final String CLIENT_SECRET = "test-client-secret";
  private static final String ACCESS_TOKEN_1 = "access-token-1";
  private static final String ACCESS_TOKEN_2 = "access-token-2";
  private static final long EXPIRES_IN_SECONDS = 3600L;
  private static final String CATALOG_URI = "https://uc.example.com/prefix/";
  private static final String UC_TOKEN_EXCHANGE_URI =
      "https://uc.example.com/prefix/api/1.0/unity-control/auth/tokens";
  private static final String UC_TOKEN_1 = "uc-token-1";
  private static final String UC_TOKEN_2 = "uc-token-2";

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

  @Test
  @SuppressWarnings("unchecked")
  public void testAccessTokenHandlesHttpError() throws Exception {
    HttpResponse<String> mockResponse = mock(HttpResponse.class);
    when(mockResponse.statusCode()).thenReturn(401);
    when(mockResponse.body()).thenReturn("Unauthorized");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    OAuthTokenProvider provider = defaultOAuthTokenProvider();

    assertThatThrownBy(provider::accessToken)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Failed to renew OAuth token")
        .hasCauseInstanceOf(IOException.class);
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
                    /* catalogUri= */ null,
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
                    /* catalogUri= */ null,
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
                    /* catalogUri= */ null,
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
                    /* catalogUri= */ null,
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
  public void testEmptyScopeSendsNoScope() throws Exception {
    // Empty, or empty after trimming: the request carries no scope, and the identity provider
    // applies its own default.
    mockOAuthResponse(ACCESS_TOKEN_1, EXPIRES_IN_SECONDS);
    oauthTokenProvider("").accessToken();
    oauthTokenProvider("   ").accessToken();

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockHttpClient, times(2)).send(requestCaptor.capture(), any());
    for (HttpRequest request : requestCaptor.getAllValues()) {
      assertThat(requestBody(request)).isEqualTo("grant_type=client_credentials");
    }
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

  @Test
  public void testExchangeForUcTokenSendsExchangeRequestAndReturnsUcToken() throws Exception {
    mockTokenResponses(tokenResponse(ACCESS_TOKEN_1, 60L), tokenResponse(UC_TOKEN_1, 3600L));
    OAuthTokenProvider provider = exchangingOAuthTokenProvider();

    assertThat(provider.accessToken()).isEqualTo(UC_TOKEN_1);

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockHttpClient, times(2)).send(requestCaptor.capture(), any());
    HttpRequest oauthRequest = requestCaptor.getAllValues().get(0);
    HttpRequest exchangeRequest = requestCaptor.getAllValues().get(1);
    assertThat(oauthRequest.uri().toString()).isEqualTo(OAUTH_URI);
    assertThat(exchangeRequest.uri().toString()).isEqualTo(UC_TOKEN_EXCHANGE_URI);
    assertThat(exchangeRequest.headers().firstValue("Authorization")).isEmpty();
    assertThat(requestBody(exchangeRequest))
        .isEqualTo(
            "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange"
                + "&requested_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Aaccess_token"
                + "&subject_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Aaccess_token"
                + "&subject_token="
                + ACCESS_TOKEN_1);
  }

  @Test
  public void testExchangeForUcTokenCachesByUcTokenExpiry() throws Exception {
    // The OAuth token expires after 60s, but the cache follows the UC token's 3600s lifetime.
    mockTokenResponses(tokenResponse(ACCESS_TOKEN_1, 60L), tokenResponse(UC_TOKEN_1, 3600L));
    OAuthTokenProvider provider = exchangingOAuthTokenProvider();

    assertThat(provider.accessToken()).isEqualTo(UC_TOKEN_1);
    clock.sleep(Duration.ofSeconds(3500));
    assertThat(provider.accessToken()).isEqualTo(UC_TOKEN_1);
    verifyOAuthRequest(2);

    // Within the lead renewal time, both tokens are fetched again.
    clock.sleep(Duration.ofSeconds(80));
    mockTokenResponses(tokenResponse(ACCESS_TOKEN_2, 60L), tokenResponse(UC_TOKEN_2, 3600L));
    assertThat(provider.accessToken()).isEqualTo(UC_TOKEN_2);
    verifyOAuthRequest(4);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExchangeForUcTokenHandlesHttpError() throws Exception {
    HttpResponse<String> exchangeFailure = mock(HttpResponse.class);
    when(exchangeFailure.statusCode()).thenReturn(401);
    when(exchangeFailure.body()).thenReturn("Invalid issuer");
    mockTokenResponses(tokenResponse(ACCESS_TOKEN_1, 60L), exchangeFailure);
    OAuthTokenProvider provider = exchangingOAuthTokenProvider();

    assertThatThrownBy(provider::accessToken)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Failed to renew OAuth token")
        .cause()
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Failed to exchange OAuth token for a UC token")
        .hasMessageContaining("401");
  }

  @Test
  public void testTokenExchangeUri() {
    String[][] cases = {
      {"https://uc.example.com", "https://uc.example.com/api/1.0/unity-control/auth/tokens"},
      {"https://uc.example.com/", "https://uc.example.com/api/1.0/unity-control/auth/tokens"},
      {"http://uc:8080/prefix", "http://uc:8080/prefix/api/1.0/unity-control/auth/tokens"},
      {"http://uc:8080/prefix//", "http://uc:8080/prefix/api/1.0/unity-control/auth/tokens"},
      // A query or fragment on the catalog URI is dropped rather than absorbing the path.
      {"https://uc.example.com?x=1", "https://uc.example.com/api/1.0/unity-control/auth/tokens"},
      {
        "https://uc.example.com/prefix/?x=1#f",
        "https://uc.example.com/prefix/api/1.0/unity-control/auth/tokens"
      },
      {"https://uc.example.com#f", "https://uc.example.com/api/1.0/unity-control/auth/tokens"},
    };
    for (String[] c : cases) {
      assertThat(OAuthTokenProvider.tokenExchangeUri(c[0]).toString()).isEqualTo(c[1]);
    }
  }

  @Test
  public void testInvalidCatalogUriIsRejectedAtInitialization() {
    for (String catalogUri :
        new String[] {"uc.example.com", "localhost:8080", "ftp://uc.example.com"}) {
      assertThatThrownBy(() -> new OAuthTokenProvider().initialize(exchangeConfigs(catalogUri)))
          .as(catalogUri)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("must be an http or https URI with a host");
    }
    for (String catalogUri : new String[] {"https://", "https://uc ex"}) {
      assertThatThrownBy(() -> new OAuthTokenProvider().initialize(exchangeConfigs(catalogUri)))
          .as(catalogUri)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("is not a valid URI");
    }
  }

  @Test
  public void testTokenExpiryAcceptsNumericString() throws Exception {
    mockTokenResponses(
        tokenResponse("{\"access_token\":\"" + ACCESS_TOKEN_1 + "\",\"expires_in\":\"3600\"}"));
    OAuthTokenProvider provider = defaultOAuthTokenProvider();

    provider.accessToken();
    provider.accessToken();
    verifyOAuthRequest(1);
  }

  @Test
  public void testExchangeDoesNotNeedOAuthTokenExpiry() throws Exception {
    mockTokenResponses(
        tokenResponse("{\"access_token\":\"" + ACCESS_TOKEN_1 + "\"}"),
        tokenResponse(UC_TOKEN_1, 3600L));
    OAuthTokenProvider provider = exchangingOAuthTokenProvider();

    assertThat(provider.accessToken()).isEqualTo(UC_TOKEN_1);
    assertThat(provider.accessToken()).isEqualTo(UC_TOKEN_1);
    verifyOAuthRequest(2);
  }

  @Test
  public void testMissingExpiresInIsReported() throws Exception {
    mockTokenResponses(tokenResponse("{\"access_token\":\"" + ACCESS_TOKEN_1 + "\"}"));
    OAuthTokenProvider provider = defaultOAuthTokenProvider();

    assertThatThrownBy(provider::accessToken)
        .isInstanceOf(RuntimeException.class)
        .cause()
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Token response has no valid 'expires_in'");
  }

  @Test
  public void testMissingAccessTokenIsReported() throws Exception {
    mockTokenResponses(tokenResponse("{\"expires_in\":3600}"));
    OAuthTokenProvider provider = defaultOAuthTokenProvider();

    assertThatThrownBy(provider::accessToken)
        .isInstanceOf(RuntimeException.class)
        .cause()
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Token response has no 'access_token'");
  }

  private static Map<String, String> exchangeConfigs(String catalogUri) {
    return Map.of(
        AuthConfigs.OAUTH_URI, OAUTH_URI,
        AuthConfigs.OAUTH_CLIENT_ID, CLIENT_ID,
        AuthConfigs.OAUTH_CLIENT_SECRET, CLIENT_SECRET,
        AuthConfigs.OAUTH_EXCHANGE_FOR_UC_TOKEN, "true",
        AuthConfigs.CATALOG_URI, catalogUri);
  }

  private OAuthTokenProvider exchangingOAuthTokenProvider() {
    return new OAuthTokenProvider(
        OAUTH_URI,
        CLIENT_ID,
        CLIENT_SECRET,
        AuthConfigs.DEFAULT_OAUTH_SCOPE,
        CATALOG_URI,
        30L,
        mockRetryingApiClient,
        clock);
  }

  private OAuthTokenProvider defaultOAuthTokenProvider() {
    return oauthTokenProvider(AuthConfigs.DEFAULT_OAUTH_SCOPE);
  }

  private OAuthTokenProvider oauthTokenProvider(String oauthScope) {
    return new OAuthTokenProvider(
        OAUTH_URI,
        CLIENT_ID,
        CLIENT_SECRET,
        oauthScope,
        /* catalogUri= */ null,
        30L,
        mockRetryingApiClient,
        clock);
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

  @SuppressWarnings("unchecked")
  private static HttpResponse<String> tokenResponse(String accessToken, long expiresIn) {
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
    when(response.body())
        .thenReturn(
            String.format(
                "{\"access_token\":\"%s\",\"token_type\":\"Bearer\",\"expires_in\":%d}",
                accessToken, expiresIn));
    return response;
  }

  @SuppressWarnings("unchecked")
  private static HttpResponse<String> tokenResponse(String body) {
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
    when(response.body()).thenReturn(body);
    return response;
  }

  /** Mocks consecutive token responses, e.g. the OAuth token and then the UC exchange. */
  @SafeVarargs
  @SuppressWarnings("unchecked")
  private void mockTokenResponses(HttpResponse<String> first, HttpResponse<String>... rest)
      throws Exception {
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(first, rest);
  }

  /** Helper method to verify OAuth request was made expected number of times. */
  private void verifyOAuthRequest(int times) throws Exception {
    verify(mockHttpClient, times(times))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }
}
