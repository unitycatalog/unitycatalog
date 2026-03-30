package io.unitycatalog.spark.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.auth.TokenProvider;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class AuthConfigUtilsTest {

  private static Map<String, String> normalizeConfigs(String... keyValues) {
    Map<String, String> configs = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      configs.put(keyValues[i], keyValues[i + 1]);
    }
    return AuthConfigUtils.buildAuthConfigs(configs);
  }

  @Test
  public void testAuthPrefixTransformation() {
    assertThat(normalizeConfigs("auth.type", "oauth", "auth.client_id", "id123", "other", "val"))
        .hasSize(2)
        .containsEntry("type", "oauth")
        .containsEntry("client_id", "id123")
        .doesNotContainKey("other");

    assertThat(
            normalizeConfigs(
                "auth.type", "custom",
                "auth.endpoint", "https://auth.example.com",
                "notauth.config", "ignored",
                "authconfig", "ignored"))
        .hasSize(2)
        .containsEntry("type", "custom")
        .containsEntry("endpoint", "https://auth.example.com")
        .doesNotContainKeys("notauth.config", "authconfig");
  }

  @Test
  public void testStaticTokenSupport() {
    Map<String, String> configs = normalizeConfigs("token", "legacy-token");
    assertMapEquals(
        TokenProvider.create(configs).configs(), Map.of("type", "static", "token", "legacy-token"));

    configs = normalizeConfigs("auth.type", "static", "token", "legacy-token");
    assertMapEquals(
        TokenProvider.create(configs).configs(), Map.of("type", "static", "token", "legacy-token"));

    configs = normalizeConfigs("auth.extra", "value", "token", "legacy-token");
    assertMapEquals(configs, Map.of("type", "static", "token", "legacy-token", "extra", "value"));
    assertMapEquals(
        TokenProvider.create(configs).configs(), Map.of("type", "static", "token", "legacy-token"));

    configs = normalizeConfigs("auth.type", "static", "auth.token", "new-token");
    assertMapEquals(
        TokenProvider.create(configs).configs(), Map.of("type", "static", "token", "new-token"));
  }

  @Test
  public void testOAuthTokenSupport() {
    // Case-sensitive OAuth configs.
    Map<String, String> configs =
        normalizeConfigs(
            "auth.type",
            "oauth",
            "auth.oauth.uri",
            "https://auth.example.com",
            "auth.oauth.clientId",
            "client-id",
            "auth.oauth.clientSecret",
            "client-secret");
    assertMapEquals(
        TokenProvider.create(configs).configs(),
        Map.of(
            "type",
            "oauth",
            "oauth.uri",
            "https://auth.example.com",
            "oauth.clientId",
            "client-id",
            "oauth.clientSecret",
            "client-secret"));

    // Case-insensitive OAuth configs.
    configs =
        normalizeConfigs(
            "auth.type",
            "oauth",
            "auth.OaUTH.URI",
            "https://auth.example.com",
            "auth.oAuth.clientid",
            "client-id",
            "auth.oauth.clientsecret",
            "client-secret");
    assertMapEquals(
        TokenProvider.create(configs).configs(),
        Map.of(
            "type",
            "oauth",
            "oauth.uri",
            "https://auth.example.com",
            "oauth.clientId",
            "client-id",
            "oauth.clientSecret",
            "client-secret"));
  }

  @Test
  public void testConflictingTokens() {
    assertThatThrownBy(() -> normalizeConfigs("token", "legacy", "auth.token", "new"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Static token was configured twice");
  }

  @Test
  public void testNullAndEdgeCases() {
    assertMapEquals(normalizeConfigs("auth.type", "oauth", "token", null), Map.of("type", "oauth"));
    assertThat(normalizeConfigs("auth.", "value")).hasSize(0);
  }

  private static void assertMapEquals(Map<String, String> expected, Map<String, String> actual) {
    assertThat(expected).hasSize(actual.size()).containsAllEntriesOf(actual);
  }
}
