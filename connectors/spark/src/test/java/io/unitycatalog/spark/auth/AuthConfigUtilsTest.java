package io.unitycatalog.spark.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.auth.TokenProvider;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

public class AuthConfigUtilsTest {

  /** Spark passes catalog options as a CaseInsensitiveStringMap, which lowercases keys. */
  private static CaseInsensitiveStringMap caseInsensitive(String... keyValues) {
    Map<String, String> configs = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      configs.put(keyValues[i], keyValues[i + 1]);
    }
    return new CaseInsensitiveStringMap(configs);
  }

  private static Map<String, String> normalizeConfigs(String... keyValues) {
    return AuthConfigUtils.buildAuthConfigs(caseInsensitive(keyValues));
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
            "client-secret",
            "auth.oauth.scope",
            "https://example.com/.default");
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
            "client-secret",
            "oauth.scope",
            "https://example.com/.default"));

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
            "client-secret",
            "auth.oAuth.scope",
            "catalog.tables:read");
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
            "client-secret",
            "oauth.scope",
            "catalog.tables:read"));
  }

  @Test
  public void testOAuthEmptyScopeSurvives() {
    // An empty scope means "send no scope"; dropping it on the way would fall back to all-apis.
    Map<String, String> configs =
        normalizeConfigs(
            "auth.type",
            "oauth",
            "auth.oauth.uri",
            "https://auth.example.com",
            "auth.oauth.clientId",
            "client-id",
            "auth.oauth.clientSecret",
            "client-secret",
            "auth.oauth.scope",
            "");
    assertThat(TokenProvider.create(configs).configs()).containsEntry("oauth.scope", "");
  }

  @Test
  public void testCatalogUriIsPassedToTokenProvider() {
    Map<String, String> configs =
        normalizeConfigs(
            "uri", "https://uc.example.com",
            "auth.type", "oauth",
            "auth.oauth.uri", "https://auth.example.com",
            "auth.oauth.clientId", "client-id",
            "auth.oauth.clientSecret", "client-secret",
            "auth.oauth.exchangeForUcToken", "true");
    assertThat(configs).containsEntry("catalogUri", "https://uc.example.com");
    assertThat(TokenProvider.create(configs).configs())
        .containsEntry("catalogUri", "https://uc.example.com")
        .containsEntry("oauth.exchangeForUcToken", "true");
  }

  @Test
  public void testCatalogUriAlwaysComesFromCatalogUriOption() {
    Map<String, String> configs =
        normalizeConfigs(
            "uri", "https://uc.example.com",
            "auth.type", "oauth",
            "auth.catalogUri", "https://elsewhere.example.com");
    assertThat(configs).hasSize(2).containsEntry("catalogUri", "https://uc.example.com");
  }

  @Test
  public void testWithAuthCatalogUriForDeltaCatalog() {
    // With auth options, Delta's catalog gets auth.catalogUri from the catalog uri, replacing any
    // user value. Delta builds its provider from the auth options alone, as below.
    CaseInsensitiveStringMap options =
        AuthConfigUtils.withAuthCatalogUri(
            caseInsensitive(
                "uri", "https://uc.example.com",
                "auth.type", "oauth",
                "auth.oauth.uri", "https://auth.example.com",
                "auth.oauth.clientId", "client-id",
                "auth.oauth.clientSecret", "client-secret",
                "auth.oauth.exchangeForUcToken", "true",
                "auth.catalogUri", "https://elsewhere.example.com"));
    Map<String, String> deltaAuthConfigs = new HashMap<>();
    options.forEach(
        (k, v) -> {
          if (k.startsWith("auth.")) {
            deltaAuthConfigs.put(k.substring("auth.".length()), v);
          }
        });
    assertThat(TokenProvider.create(deltaAuthConfigs).configs())
        .containsEntry("catalogUri", "https://uc.example.com");

    // Without auth options (legacy token) or without a uri, the options are unchanged, so Delta
    // still falls back to the legacy token.
    for (CaseInsensitiveStringMap unchanged :
        new CaseInsensitiveStringMap[] {
          caseInsensitive("uri", "https://uc.example.com", "token", "legacy"),
          caseInsensitive("auth.type", "static", "auth.token", "t")
        }) {
      assertThat(AuthConfigUtils.withAuthCatalogUri(unchanged)).isSameAs(unchanged);
    }
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
