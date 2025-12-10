package io.unitycatalog.spark.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
  public void testLegacyTokenSupport() {
    assertThat(normalizeConfigs("token", "legacy-token"))
        .containsEntry("type", "static")
        .containsEntry("token", "legacy-token");

    assertThat(normalizeConfigs("auth.type", "oauth", "token", "legacy-token"))
        .containsEntry("type", "static")
        .containsEntry("token", "legacy-token");

    assertThat(normalizeConfigs("auth.extra", "value", "token", "legacy-token"))
        .hasSize(3)
        .containsEntry("type", "static")
        .containsEntry("token", "legacy-token")
        .containsEntry("extra", "value");
  }

  @Test
  public void testNewStyleToken() {
    assertThat(normalizeConfigs("auth.type", "static", "auth.token", "new-token"))
        .containsEntry("type", "static")
        .containsEntry("token", "new-token");
  }

  @Test
  public void testConflictingTokens() {
    assertThatThrownBy(() -> normalizeConfigs("token", "legacy", "auth.token", "new"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Static token was configured twice");
  }

  @Test
  public void testNullAndEdgeCases() {
    assertThat(normalizeConfigs("auth.type", "oauth", "token", null))
        .hasSize(1)
        .containsEntry("type", "oauth");

    assertThat(normalizeConfigs("auth.", "value")).hasSize(0);
  }
}
