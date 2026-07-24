package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.OAuthInvalidRequestException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/** Matches values against configured allowlist entries (exact or wildcard). */
public final class WildcardAllowlist {

  public static final String ALLOWED_ISSUERS_NAME = "allowed issuers";
  public static final String AUDIENCES_NAME = "audiences";

  private final String source;
  private final Set<String> exact;
  private final List<Pattern> wildcards;

  /**
   * @param allowlistName human-readable name used in configuration error messages (e.g. {@link
   *     #ALLOWED_ISSUERS_NAME})
   * @param propertyKey server property key (e.g. {@link ServerProperties.Property#ALLOWED_ISSUERS})
   * @param source raw comma-separated property value from {@code server.properties}
   */
  WildcardAllowlist(String allowlistName, String propertyKey, String source) {
    this.source = source == null ? "" : source;
    Set<String> exactEntries = new HashSet<>();
    List<Pattern> wildcardEntries = new ArrayList<>();
    for (String pattern : parseCommaSeparated(this.source)) {
      if (pattern.indexOf('*') >= 0) {
        wildcardEntries.add(toRegex(pattern));
      } else {
        exactEntries.add(pattern);
      }
    }
    if (exactEntries.isEmpty() && wildcardEntries.isEmpty()) {
      throw misconfigured(allowlistName, propertyKey);
    }
    this.exact = Set.copyOf(exactEntries);
    this.wildcards = List.copyOf(wildcardEntries);
  }

  /** Raw property value this allowlist was built from; used to detect config changes. */
  public String source() {
    return source;
  }

  public static WildcardAllowlist forAllowedIssuers(String source) {
    return new WildcardAllowlist(
        ALLOWED_ISSUERS_NAME, ServerProperties.Property.ALLOWED_ISSUERS.key, source);
  }

  public static WildcardAllowlist forAudiences(String source) {
    return new WildcardAllowlist(AUDIENCES_NAME, ServerProperties.Property.AUDIENCES.key, source);
  }

  /**
   * Returns true if {@code value} matches any configured entry.
   *
   * <p>Entries without {@code *} require an exact match. Entries with {@code *} match a single DNS
   * label at each wildcard position (e.g. {@code https://*.dev.example.com} matches {@code
   * https://acme.dev.example.com}).
   */
  public boolean isAllowed(String value) {
    if (value == null || value.isBlank()) {
      return false;
    }
    if (exact.contains(value)) {
      return true;
    }
    for (Pattern wildcard : wildcards) {
      if (wildcard.matcher(value).matches()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if any value in {@code values} matches a configured entry.
   *
   * <p>Used for JWT {@code aud} claims, which may contain multiple audience values.
   */
  public boolean isAnyAllowed(List<String> values) {
    if (values == null || values.isEmpty()) {
      return false;
    }
    for (String value : values) {
      if (isAllowed(value)) {
        return true;
      }
    }
    return false;
  }

  private static List<String> parseCommaSeparated(String value) {
    if (value.isBlank()) {
      return List.of();
    }
    return Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
  }

  private static OAuthInvalidRequestException misconfigured(
      String allowlistName, String propertyKey) {
    return new OAuthInvalidRequestException(
        ErrorCode.INVALID_ARGUMENT,
        "No " + allowlistName + " configured. Set " + propertyKey + " in server.properties");
  }

  private static Pattern toRegex(String pattern) {
    String[] parts = pattern.split("\\*", -1);
    StringBuilder regex = new StringBuilder("^");
    for (int i = 0; i < parts.length; i++) {
      if (i > 0) {
        regex.append("[^./]+");
      }
      regex.append(Pattern.quote(parts[i]));
    }
    regex.append('$');
    return Pattern.compile(regex.toString());
  }
}
