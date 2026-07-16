package io.unitycatalog.hadoop.internal;

import java.util.List;
import java.util.function.Function;

/** Internal helpers for reasoning about storage locations and their path prefixes. */
public final class StorageLocationUtil {
  private StorageLocationUtil() {}

  /**
   * Returns true if {@code prefix} covers {@code location}: they are equal (ignoring trailing
   * slashes) or {@code location} is a path-boundary descendant of {@code prefix}.
   */
  public static boolean covers(String location, String prefix) {
    String l = stripTrailingSlashes(location);
    String p = stripTrailingSlashes(prefix);
    return !p.isEmpty() && (l.equals(p) || (l.startsWith(p) && l.charAt(p.length()) == '/'));
  }

  /**
   * Returns the item whose prefix (via {@code prefixOf}) covers {@code location} with the longest
   * match, or null if none cover it. Items with a null prefix are skipped.
   */
  public static <T> T longestCovering(
      String location, List<T> items, Function<T, String> prefixOf) {
    T best = null;
    int bestLen = -1;
    for (T item : items) {
      String prefix = prefixOf.apply(item);
      if (prefix == null || !covers(location, prefix)) {
        continue;
      }
      if (prefix.length() > bestLen) {
        best = item;
        bestLen = prefix.length();
      }
    }
    return best;
  }

  private static String stripTrailingSlashes(String value) {
    int end = value.length();
    int min = value.indexOf("://");
    min = min >= 0 ? min + 3 : 1;
    while (end > min && value.charAt(end - 1) == '/') {
      end--;
    }
    return value.substring(0, end);
  }
}
