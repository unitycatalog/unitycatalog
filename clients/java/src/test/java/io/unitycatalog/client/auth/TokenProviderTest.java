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
    TokenProvider oauthProvider = TokenProviderUtils.create(OAUTH_URI, CLIENT_ID, CLIENT_SECRET);
    assertThat(oauthProvider).isInstanceOf(OAuthTokenProvider.class);
    assertThat(oauthProvider.configs())
        .hasSize(5)
        .containsEntry(AuthConfigs.TYPE, AuthConfigs.OAUTH_TYPE_VALUE)
        .containsEntry(AuthConfigs.OAUTH_URI, OAUTH_URI)
        .containsEntry(AuthConfigs.OAUTH_CLIENT_ID, CLIENT_ID)
        .containsEntry(AuthConfigs.OAUTH_CLIENT_SECRET, CLIENT_SECRET)
        .containsEntry(AuthConfigs.OAUTH_SCOPE, AuthConfigs.DEFAULT_OAUTH_SCOPE);

    // Test with incomplete OAuth config - should throw
    Map<String, String> incompleteOAuthOptions =
        Map.of(AuthConfigs.TYPE, AuthConfigs.OAUTH_TYPE_VALUE, AuthConfigs.OAUTH_URI, OAUTH_URI);
    assertThatThrownBy(() -> TokenProvider.create(incompleteOAuthOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Configuration key 'oauth.clientId' is missing or empty");

    // Test with no valid config - should throw
    assertThatThrownBy(() -> TokenProvider.create(Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Required configuration key 'type' is missing or empty");

    // Test with no 'type' even both static token and oauth are provided - should throw
    Map<String, String> bothOptions = new HashMap<>();
    bothOptions.put(AuthConfigs.STATIC_TOKEN, "fixed-token");
    bothOptions.put(AuthConfigs.OAUTH_URI, OAUTH_URI);
    bothOptions.put(AuthConfigs.OAUTH_CLIENT_ID, CLIENT_ID);
    bothOptions.put(AuthConfigs.OAUTH_CLIENT_SECRET, CLIENT_SECRET);
    assertThatThrownBy(() -> TokenProvider.create(bothOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Required configuration key 'type' is missing or empty");
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
    assertThat(providerWithProperties.configs())
        .hasSize(2)
        .containsEntry(AuthConfigs.STATIC_TOKEN, "test-token")
        .containsEntry(AuthConfigs.TYPE, AuthConfigs.STATIC_TYPE_VALUE);

    TokenProvider factoryProvider = TokenProviderUtils.create("factory-token");
    assertThat(factoryProvider).isNotNull();
    assertThat(factoryProvider.accessToken()).isEqualTo("factory-token");
  }

  @Test
  public void testOAuthTokenProvider() {
    TokenProvider provider = TokenProviderUtils.create(OAUTH_URI, CLIENT_ID, CLIENT_SECRET);
    assertThat(provider).isNotNull();
    assertThat(provider).isInstanceOf(OAuthTokenProvider.class);

    assertThat(provider.configs())
        .hasSize(5)
        .containsEntry(AuthConfigs.TYPE, AuthConfigs.OAUTH_TYPE_VALUE)
        .containsEntry(AuthConfigs.OAUTH_URI, OAUTH_URI)
        .containsEntry(AuthConfigs.OAUTH_CLIENT_ID, CLIENT_ID)
        .containsEntry(AuthConfigs.OAUTH_CLIENT_SECRET, CLIENT_SECRET)
        .containsEntry(AuthConfigs.OAUTH_SCOPE, AuthConfigs.DEFAULT_OAUTH_SCOPE);
  }

  @Test
  public void testOAuthScopeIsConfigurableAndRoundTrips() {
    Map<String, String> configs = new HashMap<>();
    configs.put(AuthConfigs.TYPE, AuthConfigs.OAUTH_TYPE_VALUE);
    configs.put(AuthConfigs.OAUTH_URI, OAUTH_URI);
    configs.put(AuthConfigs.OAUTH_CLIENT_ID, CLIENT_ID);
    configs.put(AuthConfigs.OAUTH_CLIENT_SECRET, CLIENT_SECRET);
    configs.put(AuthConfigs.OAUTH_SCOPE, "https://example.com/.default");

    // A configured scope is honored and round-trips through configs().
    assertThat(TokenProvider.create(configs).configs())
        .containsEntry(AuthConfigs.OAUTH_SCOPE, "https://example.com/.default");

    // When unset, it falls back to the default scope.
    configs.remove(AuthConfigs.OAUTH_SCOPE);
    assertThat(TokenProvider.create(configs).configs())
        .containsEntry(AuthConfigs.OAUTH_SCOPE, AuthConfigs.DEFAULT_OAUTH_SCOPE);

    // When set to an empty string, it also falls back to the default scope.
    configs.put(AuthConfigs.OAUTH_SCOPE, "");
    assertThat(TokenProvider.create(configs).configs())
        .containsEntry(AuthConfigs.OAUTH_SCOPE, AuthConfigs.DEFAULT_OAUTH_SCOPE);

    // When set to whitespace, it also falls back to the default scope.
    configs.put(AuthConfigs.OAUTH_SCOPE, "   ");
    assertThat(TokenProvider.create(configs).configs())
        .containsEntry(AuthConfigs.OAUTH_SCOPE, AuthConfigs.DEFAULT_OAUTH_SCOPE);
  }

  @Test
  public void testCustomTokenProvider() {
    // Test with custom TokenProvider using fully qualified class name
    Map<String, String> customConfigs = new HashMap<>();
    customConfigs.put(AuthConfigs.TYPE, CustomTestTokenProvider.class.getName());
    customConfigs.put("custom.token", "custom-test-token");

    TokenProvider customProvider = TokenProvider.create(customConfigs);
    assertThat(customProvider).isNotNull();
    assertThat(customProvider).isInstanceOf(CustomTestTokenProvider.class);
    assertThat(customProvider.accessToken()).isEqualTo("custom-test-token");
    assertThat(customProvider.configs())
        .containsEntry(AuthConfigs.TYPE, CustomTestTokenProvider.class.getName())
        .containsEntry("custom.token", "custom-test-token");

    // Test that configs() can be used to create a new instance
    TokenProvider clonedProvider = TokenProvider.create(customProvider.configs());
    assertThat(clonedProvider).isInstanceOf(CustomTestTokenProvider.class);
    assertThat(clonedProvider.accessToken()).isEqualTo("custom-test-token");
  }

  @Test
  public void testCustomTokenProviderWithInvalidClassName() {
    // Test with non-existent class name - should throw RuntimeException
    Map<String, String> invalidConfigs = new HashMap<>();
    invalidConfigs.put(AuthConfigs.TYPE, "com.example.NonExistentTokenProvider");

    assertThatThrownBy(() -> TokenProvider.create(invalidConfigs))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Failed to instantiate custom TokenProvider")
        .hasCauseInstanceOf(ClassNotFoundException.class);
  }

  @Test
  public void testCustomTokenProviderWithInvalidClass() {
    // Test with a class that doesn't implement TokenProvider - should throw RuntimeException
    Map<String, String> invalidConfigs = new HashMap<>();
    invalidConfigs.put(AuthConfigs.TYPE, String.class.getName());

    assertThatThrownBy(() -> TokenProvider.create(invalidConfigs))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Failed to instantiate custom TokenProvider");
  }

  /** Custom TokenProvider implementation for testing purposes. */
  public static class CustomTestTokenProvider implements TokenProvider {
    private Map<String, String> configs;

    @Override
    public void initialize(Map<String, String> configs) {
      this.configs = new HashMap<>(configs);
    }

    @Override
    public String accessToken() {
      return configs.get("custom.token");
    }

    @Override
    public Map<String, String> configs() {
      return new HashMap<>(configs);
    }
  }
}
