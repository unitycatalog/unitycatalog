package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class AudienceAllowlistTest {

  @Test
  void exactMatch() {
    assertThat(
            AudienceAllowlist.isAllowed(
                List.of("unity-catalog-local"), List.of("unity-catalog-local")))
        .isTrue();
    assertThat(
            AudienceAllowlist.isAllowed(List.of("wrong-audience"), List.of("unity-catalog-local")))
        .isFalse();
  }

  @Test
  void anyTokenAudienceMayMatch() {
    List<String> patterns = List.of("unity-catalog-local");
    assertThat(
            AudienceAllowlist.isAllowed(
                List.of("oauth-client-uuid", "unity-catalog-local"), patterns))
        .isTrue();
    assertThat(AudienceAllowlist.isAllowed(List.of("oauth-client-uuid"), patterns)).isFalse();
  }

  @Test
  void wildcardMatchesRealmHostAudience() {
    String pattern = "https://*.dev.example.com";
    assertThat(
            AudienceAllowlist.isAllowed(List.of("https://dev.dev.example.com"), List.of(pattern)))
        .isTrue();
    assertThat(
            AudienceAllowlist.isAllowed(
                List.of("oauth-client-uuid", "https://backend.dev.example.com"), List.of(pattern)))
        .isTrue();
    assertThat(
            AudienceAllowlist.isAllowed(List.of("https://acme.eu.example.com"), List.of(pattern)))
        .isFalse();
  }

  @Test
  void mixedExactAndWildcard() {
    List<String> patterns = List.of("unity-catalog-local", "https://*.dev.example.com");
    assertThat(AudienceAllowlist.isAllowed(List.of("unity-catalog-local"), patterns)).isTrue();
    assertThat(AudienceAllowlist.isAllowed(List.of("https://team.dev.example.com"), patterns))
        .isTrue();
    assertThat(AudienceAllowlist.isAllowed(List.of("https://other.example.com"), patterns))
        .isFalse();
  }

  @Test
  void rejectsNullOrEmptyTokenAudiences() {
    assertThat(AudienceAllowlist.isAllowed(null, List.of("unity-catalog-local"))).isFalse();
    assertThat(AudienceAllowlist.isAllowed(List.of(), List.of("unity-catalog-local"))).isFalse();
  }
}
