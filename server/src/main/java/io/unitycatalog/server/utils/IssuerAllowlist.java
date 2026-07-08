package io.unitycatalog.server.utils;

import java.util.List;
import java.util.regex.Pattern;

/** Matches token issuers against configured allowlist entries (exact or wildcard). */
public final class IssuerAllowlist {

  private IssuerAllowlist() {}

  /**
   * Returns true if {@code issuer} matches any entry in {@code allowedIssuers}.
   *
   * <p>Entries without {@code *} require an exact match. Entries with {@code *} match a single DNS
   * label at each wildcard position (e.g. {@code https://*.dev.example.com} matches {@code
   * https://acme.dev.example.com}).
   */
  public static boolean isAllowed(String issuer, List<String> allowedIssuers) {
    if (issuer == null || issuer.isBlank()) {
      return false;
    }
    for (String pattern : allowedIssuers) {
      if (pattern == null || pattern.isBlank()) {
        continue;
      }
      if (pattern.indexOf('*') >= 0) {
        if (toRegex(pattern).matcher(issuer).matches()) {
          return true;
        }
      } else if (pattern.equals(issuer)) {
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
