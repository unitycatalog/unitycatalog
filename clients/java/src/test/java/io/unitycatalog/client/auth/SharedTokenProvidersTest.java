package io.unitycatalog.client.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.internal.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests that go through {@link TokenProvider#create} share the JVM-wide instance, so each one uses
 * its own client secret to stay independent of the others.
 */
public class SharedTokenProvidersTest {

  private static final String OAUTH_URI = "https://oauth.example.com/token";
  private static final String CLIENT_ID = "test-client-id";

  private Clock clock;
  private SharedTokenProviders providers;

  @BeforeEach
  public void setUp() {
    clock = Clock.manualClock(Instant.parse("2026-10-27T12:00:00Z"));
    providers = new SharedTokenProviders(clock);
  }

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

  private static Map<String, String> oauthConfigs(String clientSecret, String sharedFlag) {
    Map<String, String> configs = oauthConfigs(clientSecret);
    if (sharedFlag != null) {
      configs.put(AuthConfigs.OAUTH_SHARED, sharedFlag);
    }
    return configs;
  }

  @ParameterizedTest
  @ValueSource(strings = {"true", "TRUE", "True"})
  public void testCreateSharesProviderWhenFlagIsTrue(String sharedFlag) {
    String secret = uniqueSecret();

    TokenProvider first = TokenProvider.create(oauthConfigs(secret, sharedFlag));
    TokenProvider second = TokenProvider.create(oauthConfigs(secret, sharedFlag));

    assertThat(first).isInstanceOf(OAuthTokenProvider.class).isSameAs(second);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"false", "FALSE"})
  public void testCreateReturnsNewProvidersUnlessFlagIsTrue(String sharedFlag) {
    String secret = uniqueSecret();

    TokenProvider first = TokenProvider.create(oauthConfigs(secret, sharedFlag));
    TokenProvider second = TokenProvider.create(oauthConfigs(secret, sharedFlag));

    assertThat(first).isInstanceOf(OAuthTokenProvider.class).isNotSameAs(second);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "yes", "1"})
  public void testCreateRejectsNonBooleanFlag(String sharedFlag) {
    assertThatThrownBy(() -> TokenProvider.create(oauthConfigs(uniqueSecret(), sharedFlag)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Configuration key 'oauth.shared' must be 'true' or 'false', but got '"
                + sharedFlag
                + "'");
  }

  @Test
  public void testSharingIgnoresKeyCasing() {
    String secret = uniqueSecret();
    Map<String, String> lowerCased = new HashMap<>();
    oauthConfigs(secret, "true").forEach((key, value) -> lowerCased.put(key.toLowerCase(), value));

    TokenProvider viaLowerCased = TokenProvider.create(lowerCased);
    TokenProvider viaDefault = TokenProvider.create(oauthConfigs(secret, "true"));

    assertThat(viaLowerCased).isSameAs(viaDefault);
  }

  @Test
  public void testDifferentSecretGetsDifferentProvider() {
    String newSecret = uniqueSecret();

    TokenProvider oldProvider = TokenProvider.create(oauthConfigs(uniqueSecret(), "true"));
    TokenProvider newProvider = TokenProvider.create(oauthConfigs(newSecret, "true"));

    assertThat(oldProvider).isNotSameAs(newProvider);
    assertThat(newProvider.configs()).containsEntry(AuthConfigs.OAUTH_CLIENT_SECRET, newSecret);
  }

  @Test
  public void testFlagDoesNotShareStaticProviders() {
    Map<String, String> staticConfigs =
        Map.of(
            AuthConfigs.TYPE,
            AuthConfigs.STATIC_TYPE_VALUE,
            AuthConfigs.STATIC_TOKEN,
            "token",
            AuthConfigs.OAUTH_SHARED,
            "true");

    TokenProvider first = TokenProvider.create(staticConfigs);
    TokenProvider second = TokenProvider.create(staticConfigs);

    assertThat(first).isInstanceOf(StaticTokenProvider.class).isNotSameAs(second);
  }

  @Test
  public void testIdleEntriesAreEvicted() throws InterruptedException {
    TokenProvider idle = providers.get(oauthConfigs("idle-secret"));

    clock.sleep(SharedTokenProviders.IDLE_TIMEOUT.plus(Duration.ofMinutes(1)));
    providers.get(oauthConfigs("active-secret"));

    assertThat(providers.size()).isEqualTo(1);
    assertThat(providers.get(oauthConfigs("idle-secret"))).isNotSameAs(idle);
  }

  @Test
  public void testEntriesLookedUpWithinTheIdleTimeoutAreKept() throws InterruptedException {
    TokenProvider kept = providers.get(oauthConfigs("kept-secret"));

    clock.sleep(SharedTokenProviders.IDLE_TIMEOUT.minus(Duration.ofMinutes(1)));
    providers.get(oauthConfigs("kept-secret"));
    clock.sleep(Duration.ofMinutes(2));
    providers.get(oauthConfigs("other-secret"));

    assertThat(providers.size()).isEqualTo(2);
    assertThat(providers.get(oauthConfigs("kept-secret"))).isSameAs(kept);
  }
}
