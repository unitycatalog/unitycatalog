package io.unitycatalog.spark

/**
 * Helpers for detecting whether the bundled Delta library is present and new enough to take
 * the UC Delta API path that `UCDeltaCatalogClientImpl` (in delta-io/delta) implements. UC
 * catalogs (e.g. `UCSingleCatalog`) consult [[isDeltaRestApiReady]] to decide whether they
 * can skip their legacy `createStagingTable` call and let the Delta catalog handle staging
 * + finalization itself.
 */
private[spark] object DeltaAvailability {

  /**
   * Minimum Delta library version (as reported by `io.delta.VERSION`) that ships the
   * `UCDeltaCatalogClientImpl` UC Delta API path.
   */
  val MIN_DELTA_VERSION_FOR_REST_API: String = "4.3.0"

  /**
   * Returns true when all the following are true:
   *   1. Delta is loaded as the catalog delegate (otherwise nothing on the other side
   *      would stage the managed-Delta create);
   *   2. the catalog opted in via the `deltaRestApi.enabled` option;
   *   3. the Delta library on the classpath is at least
   *      [[MIN_DELTA_VERSION_FOR_REST_API]] (older versions silently skip staging if we
   *      hand the request to them).
   */
  def isDeltaRestApiReady(
      deltaCatalogLoaded: Boolean,
      deltaRestApiEnabled: Boolean): Boolean =
    deltaCatalogLoaded && deltaRestApiEnabled &&
      deltaVersionAtLeast(MIN_DELTA_VERSION_FOR_REST_API)

  /**
   * Returns true when `io.delta.VERSION` (the bundled Delta library version) compares
   * greater-or-equal to `target`. Uses reflection so this object stays compilable even when
   * Delta isn't on the classpath; any failure to read the version is treated as
   * "no, not at least target".
   */
  def deltaVersionAtLeast(target: String): Boolean = {
    val version =
      try {
        val pkg = Class.forName("io.delta.package$")
        val module = pkg.getField("MODULE$").get(null)
        pkg.getMethod("VERSION").invoke(module).asInstanceOf[String]
      } catch {
        case _: Throwable => return false
      }
    compareVersions(version, target) >= 0
  }

  /**
   * Compares two semver-style version strings (with optional pre-release suffix). Returns
   * negative, zero, or positive in the usual `compareTo` sense. Missing minor/patch parts
   * are treated as 0 (e.g. `"5"` compares as `"5.0.0"`). Pre-release suffixes (after `-`)
   * are ignored, so `"4.3.0-SNAPSHOT"` compares equal to `"4.3.0"`.
   */
  def compareVersions(a: String, b: String): Int = {
    def numericParts(v: String): Array[Int] =
      v.takeWhile(_ != '-').split('.').map { piece =>
        try piece.toInt catch { case _: NumberFormatException => 0 }
      }
    val pa = numericParts(a)
    val pb = numericParts(b)
    var i = 0
    while (i < pa.length || i < pb.length) {
      val ai = if (i < pa.length) pa(i) else 0
      val bi = if (i < pb.length) pb(i) else 0
      if (ai != bi) return ai - bi
      i += 1
    }
    0
  }
}
