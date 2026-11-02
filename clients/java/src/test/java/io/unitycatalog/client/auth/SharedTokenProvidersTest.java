package io.unitycatalog.client.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Tests that go through {@link TokenProvider#create} share the JVM-wide instance, so each one uses
 * its own client secret to stay independent of the others.
 */
public class SharedTokenProvidersTest {

  private static final String OAUTH_URI = "https://oauth.example.com/token";
  private static final String CLIENT_ID = "test-client-id";

  private static String uniqueSecret() {
    return "secret-" + UUID.randomUUID();
  }

  private static Map<String, String> oauthConfigs(String clientSecret) {
    Map<String, String> configs = new HashMap<>();
    configs.put(AuthConfigs.TYPE, AuthConfigs.OAUTH_TYPE_VALUE);
    configs.put(AuthConfigs.OAUTH_URI, OAUTH_URI);
    configs.put(AuthConfigs.OAUTH_CLIENT_ID, CLIENT_ID);
    configs.put(AuthConfigs.OAUTH_CLIENT_SECRET, clientSecret);
    return configs;
  }

  @Test
  public void testCreateSharesProviderForSameConfig() {
    String secret = uniqueSecret();

    TokenProvider first = TokenProvider.create(oauthConfigs(secret));
    TokenProvider second = TokenProvider.create(oauthConfigs(secret));

    assertThat(first).isInstanceOf(OAuthTokenProvider.class).isSameAs(second);
  }

  @Test
  public void testSharingIgnoresKeyCasing() {
    String secret = uniqueSecret();
    Map<String, String> lowerCased = new HashMap<>();
    oauthConfigs(secret).forEach((key, value) -> lowerCased.put(key.toLowerCase(), value));

    TokenProvider viaLowerCased = TokenProvider.create(lowerCased);
    TokenProvider viaDefault = TokenProvider.create(oauthConfigs(secret));

    assertThat(viaLowerCased).isSameAs(viaDefault);
  }

  @Test
  public void testDifferentSecretGetsDifferentProvider() {
    String newSecret = uniqueSecret();

    TokenProvider oldProvider = TokenProvider.create(oauthConfigs(uniqueSecret()));
    TokenProvider newProvider = TokenProvider.create(oauthConfigs(newSecret));

    assertThat(oldProvider).isNotSameAs(newProvider);
    assertThat(newProvider.configs()).containsEntry(AuthConfigs.OAUTH_CLIENT_SECRET, newSecret);
  }

  @Test
  public void testStaticProvidersAreNotShared() {
    Map<String, String> staticConfigs =
        Map.of(AuthConfigs.TYPE, AuthConfigs.STATIC_TYPE_VALUE, AuthConfigs.STATIC_TOKEN, "token");

    TokenProvider first = TokenProvider.create(staticConfigs);
    TokenProvider second = TokenProvider.create(staticConfigs);

    assertThat(first).isInstanceOf(StaticTokenProvider.class).isNotSameAs(second);
  }

  @Test
  public void testLeastRecentlyLookedUpProviderIsEvictedAtMaxSize() {
    SharedTokenProviders providers = new SharedTokenProviders(/* maxSize= */ 2);
    TokenProvider kept = providers.get(oauthConfigs("kept-secret"));
    TokenProvider evicted = providers.get(oauthConfigs("evicted-secret"));

    providers.get(oauthConfigs("kept-secret"));
    providers.get(oauthConfigs("new-secret"));

    assertThat(providers.size()).isEqualTo(2);
    assertThat(providers.get(oauthConfigs("kept-secret"))).isSameAs(kept);
    assertThat(providers.get(oauthConfigs("evicted-secret"))).isNotSameAs(evicted);
  }
}
