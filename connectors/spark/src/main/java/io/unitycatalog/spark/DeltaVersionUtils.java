package io.unitycatalog.spark;

/** Utility for Delta version checks in tests. */
public class DeltaVersionUtils {

  private DeltaVersionUtils() {}

  public static boolean isDeltaAtLeast(String required) {
    return compareVersions(getDeltaVersion(), required) >= 0;
  }

  private static int compareVersions(String actual, String required) {
    if (actual == null) {
      return -1;
    }
    String[] a = actual.split("-")[0].split("\\.");
    String[] r = required.split("\\.");
    for (int i = 0; i < Math.max(a.length, r.length); i++) {
      int ai = i < a.length ? Integer.parseInt(a[i]) : 0;
      int ri = i < r.length ? Integer.parseInt(r[i]) : 0;
      if (ai != ri) {
        return Integer.compare(ai, ri);
      }
    }
    return 0;
  }

  public static String getDeltaVersion() {
    // Try io.delta.Version.getVersion() first (preferred method)
    try {
      Class<?> versionClass = Class.forName("io.delta.Version");
      Object versionObj = versionClass.getMethod("getVersion").invoke(null);
      return versionObj != null ? versionObj.toString() : null;
    } catch (Exception e) {
      // Fall back to io.delta.VERSION constant (older versions)
      try {
        Class<?> packageClass = Class.forName("io.delta.package$");
        Object versionObj =
            packageClass.getMethod("VERSION").invoke(packageClass.getField("MODULE$").get(null));
        return versionObj != null ? versionObj.toString() : null;
      } catch (Exception e2) {
        // Delta not available or version not accessible
        return null;
      }
    }
  }

  /**
   * Minimum Delta library version (as reported by `io.delta.VERSION`) that ships the
   * `UCDeltaCatalogClientImpl` UC Delta API path.
   */
  static String MIN_DELTA_VERSION_FOR_REST_API = "4.3.0";

  /**
   * Returns true when all the following are true: 1. Delta is loaded as the catalog delegate
   * (otherwise nothing on the other side would stage the managed-Delta create); 2. the catalog is
   * not opted out via the `deltaRestApi.enabled=false` option; 3. the Delta library on the
   * classpath is at least [[MIN_DELTA_VERSION_FOR_REST_API]] (older versions silently skip staging
   * if we hand the request to them).
   */
  public static boolean isDeltaRestApiReady(
      boolean deltaCatalogLoaded, boolean deltaRestApiEnabled) {
    return deltaCatalogLoaded
        && deltaRestApiEnabled
        && isDeltaAtLeast(MIN_DELTA_VERSION_FOR_REST_API);
  }
}
