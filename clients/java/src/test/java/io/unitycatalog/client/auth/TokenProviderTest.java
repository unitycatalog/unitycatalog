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
    // Test with valid token - should create FixedUCTokenProvider
    Map<String, String> tokenOptions = Map.of(AuthProps.TOKEN, "test-token");
    TokenProvider tokenProvider = TokenProvider.builder().options(tokenOptions).build();
    assertThat(tokenProvider).isInstanceOf(FixedTokenProvider.class);
    assertThat(tokenProvider.accessToken()).isEqualTo("test-token");

    // Test with complete OAuth config - should create OAuthUCTokenProvider
    TokenProvider oauthProvider =
        TokenProvider.builder()
            .options(
                Map.of(
                    AuthProps.OAUTH_URI, OAUTH_URI,
                    AuthProps.OAUTH_CLIENT_ID, CLIENT_ID,
                    AuthProps.OAUTH_CLIENT_SECRET, CLIENT_SECRET))
            .build();
    assertThat(oauthProvider).isInstanceOf(OAuthTokenProvider.class);
    assertThat(oauthProvider.properties())
        .containsEntry(AuthProps.OAUTH_URI, OAUTH_URI)
        .containsEntry(AuthProps.OAUTH_CLIENT_ID, CLIENT_ID)
        .containsEntry(AuthProps.OAUTH_CLIENT_SECRET, CLIENT_SECRET);

    // Test with incomplete OAuth config - should throw
    Map<String, String> incompleteOAuthOptions = Map.of(AuthProps.OAUTH_URI, OAUTH_URI);
    assertThatThrownBy(() -> TokenProvider.builder().options(incompleteOAuthOptions).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Incomplete OAuth configuration detected");

    // Test with no valid config - should throw
    assertThatThrownBy(() -> TokenProvider.builder().options(Map.of()).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot determine UC authentication configuration from options");

    // Test token takes precedence over OAuth when both are present
    Map<String, String> bothOptions = new HashMap<>();
    bothOptions.put(AuthProps.TOKEN, "fixed-token");
    bothOptions.put(AuthProps.OAUTH_URI, OAUTH_URI);
    bothOptions.put(AuthProps.OAUTH_CLIENT_ID, CLIENT_ID);
    bothOptions.put(AuthProps.OAUTH_CLIENT_SECRET, CLIENT_SECRET);
    TokenProvider precedenceProvider = TokenProvider.builder().options(bothOptions).build();
    assertThat(precedenceProvider).isInstanceOf(FixedTokenProvider.class);
    assertThat(precedenceProvider.accessToken()).isEqualTo("fixed-token");
  }

  @Test
  public void testFixedTokenProvider() {
    FixedTokenProvider provider = new FixedTokenProvider("my-token");
    assertThat(provider.accessToken()).isEqualTo("my-token");

    FixedTokenProvider consistentProvider = new FixedTokenProvider("consistent");
    assertThat(consistentProvider.accessToken()).isEqualTo("consistent");
    assertThat(consistentProvider.accessToken()).isEqualTo("consistent");
    assertThat(consistentProvider.accessToken()).isEqualTo("consistent");

    FixedTokenProvider providerWithProperties = new FixedTokenProvider("test-token");
    assertThat(providerWithProperties.properties())
        .hasSize(1)
        .containsEntry(AuthProps.TOKEN, "test-token");

    FixedTokenProvider factoryProvider = FixedTokenProvider.create("factory-token");
    assertThat(factoryProvider).isNotNull();
    assertThat(factoryProvider.accessToken()).isEqualTo("factory-token");
  }
}
