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
  public void testCreateTokenProviderViaConfigs() {
    // Test with valid token - should create StaticTokenProvider
    TokenProvider tokenProvider = TokenProviderUtils.create("test-token");
    assertThat(tokenProvider).isInstanceOf(StaticTokenProvider.class);
    assertThat(tokenProvider.accessToken()).isEqualTo("test-token");

    // Test with complete OAuth config - should create OAuthUCTokenProvider
    TokenProvider oauthProvider =
        TokenProvider.create(
            Map.of(
                AuthConfigs.TYPE, AuthConfigs.OAUTH_TYPE,
                AuthConfigs.OAUTH_URI, OAUTH_URI,
                AuthConfigs.OAUTH_CLIENT_ID, CLIENT_ID,
                AuthConfigs.OAUTH_CLIENT_SECRET, CLIENT_SECRET));
    assertThat(oauthProvider).isInstanceOf(OAuthTokenProvider.class);
    assertThat(oauthProvider.getConfigs())
        .hasSize(4)
        .containsEntry(AuthConfigs.TYPE, AuthConfigs.OAUTH_TYPE)
        .containsEntry(AuthConfigs.OAUTH_URI, OAUTH_URI)
        .containsEntry(AuthConfigs.OAUTH_CLIENT_ID, CLIENT_ID)
        .containsEntry(AuthConfigs.OAUTH_CLIENT_SECRET, CLIENT_SECRET);

    // Test with incomplete OAuth config - should throw
    Map<String, String> incompleteOAuthOptions =
        Map.of(AuthConfigs.TYPE, AuthConfigs.OAUTH_TYPE, AuthConfigs.OAUTH_URI, OAUTH_URI);
    assertThatThrownBy(() -> TokenProvider.create(incompleteOAuthOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("OAuth Client ID must not be null or empty");

    // Test with no valid config - should throw
    assertThatThrownBy(() -> TokenProvider.create(Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Value of 'type' to instantiate TokenProvider cannot be null or empty");

    // Test token takes precedence over OAuth when both are present
    Map<String, String> bothOptions = new HashMap<>();
    bothOptions.put(AuthConfigs.STATIC_TOKEN, "fixed-token");
    bothOptions.put(AuthConfigs.OAUTH_URI, OAUTH_URI);
    bothOptions.put(AuthConfigs.OAUTH_CLIENT_ID, CLIENT_ID);
    bothOptions.put(AuthConfigs.OAUTH_CLIENT_SECRET, CLIENT_SECRET);
    assertThatThrownBy(() -> TokenProvider.create(bothOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Value of 'type' to instantiate TokenProvider cannot be null or empty");
  }

  @Test
  public void testStaticTokenProvider() {
    TokenProvider provider = TokenProviderUtils.create("my-token");
    assertThat(provider.accessToken()).isEqualTo("my-token");

    TokenProvider consistentProvider = TokenProviderUtils.create("consistent");
    assertThat(consistentProvider.accessToken()).isEqualTo("consistent");
    assertThat(consistentProvider.accessToken()).isEqualTo("consistent");
    assertThat(consistentProvider.accessToken()).isEqualTo("consistent");

    TokenProvider providerWithProperties = TokenProviderUtils.create("test-token");
    assertThat(providerWithProperties.getConfigs())
        .hasSize(2)
        .containsEntry(AuthConfigs.STATIC_TOKEN, "test-token")
        .containsEntry(AuthConfigs.TYPE, AuthConfigs.STATIC_TYPE);

    TokenProvider factoryProvider = TokenProviderUtils.create("factory-token");
    assertThat(factoryProvider).isNotNull();
    assertThat(factoryProvider.accessToken()).isEqualTo("factory-token");
  }

  @Test
  public void testOAuthTokenProvider() {
    TokenProvider provider = TokenProviderUtils.create(OAUTH_URI, CLIENT_ID, CLIENT_SECRET);
    assertThat(provider).isNotNull();
    assertThat(provider).isInstanceOf(OAuthTokenProvider.class);

    assertThat(provider.getConfigs())
        .hasSize(4)
        .containsEntry(AuthConfigs.TYPE, AuthConfigs.OAUTH_TYPE)
        .containsEntry(AuthConfigs.OAUTH_URI, OAUTH_URI)
        .containsEntry(AuthConfigs.OAUTH_CLIENT_ID, CLIENT_ID)
        .containsEntry(AuthConfigs.OAUTH_CLIENT_SECRET, CLIENT_SECRET);
  }
}
