package io.unitycatalog.spark;

/** Utility for Delta version checks in tests. */
public class DeltaVersionUtils {

  private DeltaVersionUtils() {}

  public static boolean isDeltaAtLeast(String required) {
    return compareVersions(ApiClientFactory.getDeltaVersion(), required) >= 0;
  }

  private static int compareVersions(String actual, String required) {
    if (actual == null) return -1;
    String[] a = actual.split("-")[0].split("\\.");
    String[] r = required.split("\\.");
    for (int i = 0; i < Math.max(a.length, r.length); i++) {
      int ai = i < a.length ? Integer.parseInt(a[i]) : 0;
      int ri = i < r.length ? Integer.parseInt(r[i]) : 0;
      if (ai != ri) return Integer.compare(ai, ri);
    }
    return 0;
  }
}
