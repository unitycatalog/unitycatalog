package io.unitycatalog.client.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TokenProviderTest {

  private static final String OAUTH_URI = "https://oauth.example.com/token";
  private static final String CLIENT_ID = "test-client-id";
  private static final String CLIENT_SECRET = "test-client-secret";

  @Test
  public void testCreateTokenProviderViaOptions() {
    // Test with valid token - should create StaticTokenProvider
    Map<String, String> tokenOptions = Map.of(AuthProps.STATIC_TOKEN, "test-token");
    TokenProvider tokenProvider = TokenProvider.createFromConfigs(tokenOptions);
    assertThat(tokenProvider).isInstanceOf(StaticTokenProvider.class);
    assertThat(tokenProvider.accessToken()).isEqualTo("test-token");

    // Test with complete OAuth config - should create OAuthUCTokenProvider
    TokenProvider oauthProvider =
        TokenProvider.createFromConfigs(
            Map.of(
                AuthProps.AUTH_TYPE, AuthProps.OAUTH_AUTH_TYPE,
                AuthProps.OAUTH_URI, OAUTH_URI,
                AuthProps.OAUTH_CLIENT_ID, CLIENT_ID,
                AuthProps.OAUTH_CLIENT_SECRET, CLIENT_SECRET));
    assertThat(oauthProvider).isInstanceOf(OAuthTokenProvider.class);
    assertThat(oauthProvider.getConfigs())
        .hasSize(4)
        .containsEntry(AuthProps.AUTH_TYPE, AuthProps.OAUTH_AUTH_TYPE)
        .containsEntry(AuthProps.OAUTH_URI, OAUTH_URI)
        .containsEntry(AuthProps.OAUTH_CLIENT_ID, CLIENT_ID)
        .containsEntry(AuthProps.OAUTH_CLIENT_SECRET, CLIENT_SECRET);

    // Test with incomplete OAuth config - should throw
    Map<String, String> incompleteOAuthOptions =
        Map.of(AuthProps.AUTH_TYPE, AuthProps.OAUTH_AUTH_TYPE, AuthProps.OAUTH_URI, OAUTH_URI);
    assertThatThrownBy(() -> TokenProvider.createFromConfigs(incompleteOAuthOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("OAuth Client ID must not be null or empty");

    // Test with no valid config - should throw
    assertThatThrownBy(() -> TokenProvider.createFromConfigs(Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid Unity Catalog authentication configuration: token cannot be null");

    // Test token takes precedence over OAuth when both are present
    Map<String, String> bothOptions = new HashMap<>();
    bothOptions.put(AuthProps.STATIC_TOKEN, "fixed-token");
    bothOptions.put(AuthProps.OAUTH_URI, OAUTH_URI);
    bothOptions.put(AuthProps.OAUTH_CLIENT_ID, CLIENT_ID);
    bothOptions.put(AuthProps.OAUTH_CLIENT_SECRET, CLIENT_SECRET);
    TokenProvider provider = TokenProvider.createFromConfigs(bothOptions);
    assertThat(provider).isInstanceOf(StaticTokenProvider.class);
  }

  @Test
  public void testStaticTokenProvider() {
    TokenProvider provider = TokenProvider.create("my-token");
    assertThat(provider.accessToken()).isEqualTo("my-token");

    TokenProvider consistentProvider = TokenProvider.create("consistent");
    assertThat(consistentProvider.accessToken()).isEqualTo("consistent");
    assertThat(consistentProvider.accessToken()).isEqualTo("consistent");
    assertThat(consistentProvider.accessToken()).isEqualTo("consistent");

    TokenProvider providerWithProperties = TokenProvider.create("test-token");
    assertThat(providerWithProperties.getConfigs())
        .hasSize(2)
        .containsEntry(AuthProps.STATIC_TOKEN, "test-token")
        .containsEntry(AuthProps.AUTH_TYPE, AuthProps.STATIC_AUTH_TYPE);

    TokenProvider factoryProvider = TokenProvider.create("factory-token");
    assertThat(factoryProvider).isNotNull();
    assertThat(factoryProvider.accessToken()).isEqualTo("factory-token");
  }

  @Test
  public void testOAuthTokenProvider() {
    TokenProvider provider =
        TokenProvider.createFromOAuthConfigs(OAUTH_URI, CLIENT_ID, CLIENT_SECRET);
    assertThat(provider).isNotNull();
    assertThat(provider).isInstanceOf(OAuthTokenProvider.class);

    assertThat(provider.getConfigs())
        .hasSize(4)
        .containsEntry(AuthProps.AUTH_TYPE, AuthProps.OAUTH_AUTH_TYPE)
        .containsEntry(AuthProps.OAUTH_URI, OAUTH_URI)
        .containsEntry(AuthProps.OAUTH_CLIENT_ID, CLIENT_ID)
        .containsEntry(AuthProps.OAUTH_CLIENT_SECRET, CLIENT_SECRET);
  }
}
