package io.unitycatalog.server.utils;

import java.util.List;

/** Matches JWT {@code aud} claim values against configured audience entries (exact or wildcard). */
public final class AudienceAllowlist {

  private AudienceAllowlist() {}

  /**
   * Returns true if any value in {@code tokenAudiences} matches an entry in {@code
   * allowedAudiences}.
   *
   * <p>Entries without {@code *} require an exact match. Entries with {@code *} match a single DNS
   * label at each wildcard position (same rules as {@link IssuerAllowlist}).
   */
  public static boolean isAllowed(List<String> tokenAudiences, List<String> allowedAudiences) {
    if (tokenAudiences == null || tokenAudiences.isEmpty()) {
      return false;
    }
    for (String audience : tokenAudiences) {
      if (IssuerAllowlist.isAllowed(audience, allowedAudiences)) {
        return true;
      }
    }
    return false;
  }
}
