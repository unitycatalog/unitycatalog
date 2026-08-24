package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.OAuthInvalidRequestException;
import java.util.List;
import org.junit.jupiter.api.Test;

class WildcardAllowlistTest {

  @Test
  void exactMatch() {
    WildcardAllowlist allowlist =
        WildcardAllowlist.forAllowedIssuers("https://dev.dev.example.com");
    assertThat(allowlist.isAllowed("https://dev.dev.example.com")).isTrue();
    assertThat(allowlist.isAllowed("https://backend.dev.example.com")).isFalse();
  }

  @Test
  void wildcardMatchesAnyTeamOnRealm() {
    WildcardAllowlist allowlist = WildcardAllowlist.forAllowedIssuers("https://*.dev.example.com");
    assertThat(allowlist.isAllowed("https://dev.dev.example.com")).isTrue();
    assertThat(allowlist.isAllowed("https://backend.dev.example.com")).isTrue();
    assertThat(allowlist.isAllowed("https://acme.dev.example.com")).isTrue();
  }

  @Test
  void wildcardRejectsOtherRealmsAndMalformedValues() {
    WildcardAllowlist allowlist = WildcardAllowlist.forAllowedIssuers("https://*.dev.example.com");
    assertThat(allowlist.isAllowed("https://acme.eu.example.com")).isFalse();
    assertThat(allowlist.isAllowed("https://evil.com")).isFalse();
    assertThat(allowlist.isAllowed("https://foo.bar.dev.example.com")).isFalse();
  }

  @Test
  void mixedExactAndWildcard() {
    WildcardAllowlist allowlist =
        WildcardAllowlist.forAllowedIssuers(
            "https://accounts.google.com,https://*.dev.example.com");
    assertThat(allowlist.isAllowed("https://accounts.google.com")).isTrue();
    assertThat(allowlist.isAllowed("https://team.dev.example.com")).isTrue();
    assertThat(allowlist.isAllowed("https://other.example.com")).isFalse();
  }

  @Test
  void isAnyAllowedMatchesAnyTokenAudience() {
    WildcardAllowlist allowlist = WildcardAllowlist.forAudiences("unity-catalog-local");
    assertThat(allowlist.isAnyAllowed(List.of("oauth-client-uuid", "unity-catalog-local")))
        .isTrue();
    assertThat(allowlist.isAnyAllowed(List.of("oauth-client-uuid"))).isFalse();
  }

  @Test
  void isAnyAllowedSupportsWildcardAudiencePatterns() {
    WildcardAllowlist allowlist = WildcardAllowlist.forAudiences("https://*.dev.example.com");
    assertThat(allowlist.isAnyAllowed(List.of("https://dev.dev.example.com"))).isTrue();
    assertThat(
            allowlist.isAnyAllowed(List.of("oauth-client-uuid", "https://backend.dev.example.com")))
        .isTrue();
    assertThat(allowlist.isAnyAllowed(List.of("https://acme.eu.example.com"))).isFalse();
  }

  @Test
  void rejectsNullOrBlankValues() {
    WildcardAllowlist allowlist = WildcardAllowlist.forAudiences("unity-catalog-local");
    assertThat(allowlist.isAllowed(null)).isFalse();
    assertThat(allowlist.isAllowed("")).isFalse();
    assertThat(allowlist.isAnyAllowed(null)).isFalse();
    assertThat(allowlist.isAnyAllowed(List.of())).isFalse();
  }

  @Test
  void skipsBlankPatterns() {
    WildcardAllowlist allowlist = WildcardAllowlist.forAudiences("a,,  ,b");
    assertThat(allowlist.isAllowed("a")).isTrue();
    assertThat(allowlist.isAllowed("b")).isTrue();
    assertThat(allowlist.isAllowed("c")).isFalse();
  }

  @Test
  void rejectsMissingIssuerConfiguration() {
    assertThatThrownBy(() -> WildcardAllowlist.forAllowedIssuers(null))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasMessage(
            "No allowed issuers configured. Set server.allowed-issuers in server.properties");

    assertThatThrownBy(() -> WildcardAllowlist.forAllowedIssuers(""))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasMessage(
            "No allowed issuers configured. Set server.allowed-issuers in server.properties");

    assertThatThrownBy(() -> WildcardAllowlist.forAllowedIssuers(",  ,"))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasMessage(
            "No allowed issuers configured. Set server.allowed-issuers in server.properties");
  }

  @Test
  void rejectsMissingAudienceConfiguration() {
    assertThatThrownBy(() -> WildcardAllowlist.forAudiences(""))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasMessage("No audiences configured. Set server.audiences in server.properties");
  }

  @Test
  void storesSourceForCacheComparison() {
    WildcardAllowlist allowlist = WildcardAllowlist.forAllowedIssuers("https://issuer.example.com");
    assertThat(allowlist.source()).isEqualTo("https://issuer.example.com");
  }
}
