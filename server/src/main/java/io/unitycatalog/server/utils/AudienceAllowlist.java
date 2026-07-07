package io.unitycatalog.server.utils;

import java.util.List;
import java.util.regex.Pattern;

/** Matches JWT {@code aud} claim values against configured audience entries (exact or wildcard). */
public final class AudienceAllowlist {

  private AudienceAllowlist() {}

  /**
   * Returns true if any value in {@code tokenAudiences} matches an entry in {@code
   * allowedAudiences}.
   *
   * <p>Entries without {@code *} require an exact match. Entries with {@code *} match a single DNS
   * label at each wildcard position (e.g. {@code https://*.dev.example.com} matches {@code
   * https://acme.dev.example.com}).
   */
  public static boolean isAllowed(List<String> tokenAudiences, List<String> allowedAudiences) {
    if (tokenAudiences == null || tokenAudiences.isEmpty()) {
      return false;
    }
    for (String audience : tokenAudiences) {
      if (matchesEntry(audience, allowedAudiences)) {
        return true;
      }
    }
    return false;
  }

  private static boolean matchesEntry(String value, List<String> patterns) {
    if (value == null || value.isBlank()) {
      return false;
    }
    for (String pattern : patterns) {
      if (pattern == null || pattern.isBlank()) {
        continue;
      }
      if (pattern.indexOf('*') >= 0) {
        if (toRegex(pattern).matcher(value).matches()) {
          return true;
        }
      } else if (pattern.equals(value)) {
        return true;
      }
    }
    return false;
  }

  private static Pattern toRegex(String pattern) {
    StringBuilder regex = new StringBuilder("^");
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);
      if (c == '*') {
        regex.append("[^./]+");
      } else if ("\\.[]{}()+-^$|?".indexOf(c) >= 0) {
        regex.append('\\').append(c);
      } else {
        regex.append(c);
      }
    }
    regex.append('$');
    return Pattern.compile(regex.toString());
  }
}
