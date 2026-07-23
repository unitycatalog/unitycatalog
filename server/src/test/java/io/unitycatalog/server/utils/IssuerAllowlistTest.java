package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class IssuerAllowlistTest {

  @Test
  void exactMatch() {
    assertThat(
            IssuerAllowlist.isAllowed(
                "https://dev.dev.example.com", List.of("https://dev.dev.example.com")))
        .isTrue();
    assertThat(
            IssuerAllowlist.isAllowed(
                "https://backend.dev.example.com", List.of("https://dev.dev.example.com")))
        .isFalse();
  }

  @Test
  void wildcardMatchesAnyTeamOnRealm() {
    String pattern = "https://*.dev.example.com";
    assertThat(IssuerAllowlist.isAllowed("https://dev.dev.example.com", List.of(pattern)))
        .isTrue();
    assertThat(IssuerAllowlist.isAllowed("https://backend.dev.example.com", List.of(pattern)))
        .isTrue();
    assertThat(IssuerAllowlist.isAllowed("https://acme.dev.example.com", List.of(pattern)))
        .isTrue();
  }

  @Test
  void wildcardRejectsOtherRealmsAndMalformedIssuers() {
    String pattern = "https://*.dev.example.com";
    assertThat(IssuerAllowlist.isAllowed("https://acme.eu.example.com", List.of(pattern)))
        .isFalse();
    assertThat(IssuerAllowlist.isAllowed("https://evil.com", List.of(pattern))).isFalse();
    assertThat(IssuerAllowlist.isAllowed("https://foo.bar.dev.example.com", List.of(pattern)))
        .isFalse();
  }

  @Test
  void mixedExactAndWildcard() {
    List<String> patterns = List.of("https://accounts.google.com", "https://*.dev.example.com");
    assertThat(IssuerAllowlist.isAllowed("https://accounts.google.com", patterns)).isTrue();
    assertThat(IssuerAllowlist.isAllowed("https://team.dev.example.com", patterns)).isTrue();
    assertThat(IssuerAllowlist.isAllowed("https://other.example.com", patterns)).isFalse();
  }
}
