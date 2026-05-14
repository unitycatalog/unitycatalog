import sbt._
import sbt.Keys._

/**
 * Cross-Spark build helpers for the `unitycatalog-spark` connector.
 *
 * Modeled on Delta Lake's `project/CrossSparkVersions.scala` but trimmed down to what
 * Unity Catalog needs today: per-Spark-version artifact suffixing and per-version
 * `scala-shims/spark-X.Y/` source directory wiring. The connector is published once per
 * supported Spark major.minor (currently `unitycatalog-spark_4.0_2.13`,
 * `unitycatalog-spark_4.1_2.13`, `unitycatalog-spark_4.2_2.13`), all built from the
 * same shared sources plus a per-version shim directory.
 *
 * The Spark version is selected via the `-DsparkVersion=...` system property (full or
 * short form -- e.g. `4.0.0` or `4.0` both resolve to the spark40 spec). When the
 * property is unset, `DEFAULT` (currently spark40) is used. This matches how Delta and
 * the existing UC build behave, and is read by `build.sbt` via the existing
 * `lazy val sparkVersion = sys.props.getOrElse("sparkVersion", "4.0.0")` line.
 *
 * To publish a backward-compatible un-suffixed artifact during release (e.g.
 * `unitycatalog-spark_2.13`), set `-DskipSparkSuffix=true`. This is intended only for
 * release scripts that publish the default-Spark-version artifact under both names.
 */
object CrossSparkVersions {

  /**
   * Specification for a single Spark version this connector cross-builds against.
   *
   * @param fullVersion         e.g. "4.0.0", "4.2.0-SNAPSHOT"
   * @param additionalSourceDir Path (under `src/main/` and `src/test/`) of the
   *                            per-version source directory; e.g. "scala-shims/spark-4.0".
   */
  case class SparkVersionSpec(fullVersion: String, additionalSourceDir: String) {
    /** Major.minor of `fullVersion`, e.g. "4.0". Used as the artifact suffix. */
    def shortVersion: String = fullVersion.split("\\.").take(2).mkString(".")
  }

  val spark40: SparkVersionSpec =
    SparkVersionSpec("4.0.0", "scala-shims/spark-4.0")
  val spark41: SparkVersionSpec =
    SparkVersionSpec("4.1.0", "scala-shims/spark-4.1")
  val spark42Snapshot: SparkVersionSpec =
    SparkVersionSpec("4.2.0-SNAPSHOT", "scala-shims/spark-4.2")

  val ALL_SPECS: Seq[SparkVersionSpec] = Seq(spark40, spark41, spark42Snapshot)
  val DEFAULT: SparkVersionSpec = spark40

  /**
   * Resolves the active Spark version spec from the `-DsparkVersion` system property.
   * Accepts either the full version (`"4.0.0"`) or the short form (`"4.0"`). Falls back
   * to [[DEFAULT]] when unset.
   */
  def getSparkVersionSpec(): SparkVersionSpec = {
    val input = sys.props.getOrElse("sparkVersion", DEFAULT.fullVersion)
    ALL_SPECS.find { spec =>
      spec.fullVersion == input || spec.shortVersion == input
    }.getOrElse {
      val valid = ALL_SPECS.flatMap(s => Seq(s.fullVersion, s.shortVersion)).distinct
      throw new IllegalArgumentException(
        s"Invalid sparkVersion: '$input'. Valid values: ${valid.mkString(", ")}")
    }
  }

  /**
   * Computes the Spark-versioned artifact module name. Adds the `_<shortVersion>`
   * suffix (e.g. `unitycatalog-spark` -> `unitycatalog-spark_4.0`); the trailing
   * `_2.13` Scala suffix is appended later by SBT's normal cross-build machinery.
   *
   * Setting `-DskipSparkSuffix=true` returns `baseName` unchanged so a release script
   * can publish the default-Spark-version artifact under both the suffixed and the
   * legacy un-suffixed coordinates in two passes.
   */
  def sparkVersionedModuleName(baseName: String): String = {
    val skipSparkSuffix = sys.props.getOrElse("skipSparkSuffix", "false").toBoolean
    if (skipSparkSuffix) baseName
    else s"${baseName}_${getSparkVersionSpec().shortVersion}"
  }

  /**
   * Adds the per-Spark-version source directories to the `Compile` and `Test`
   * `unmanagedSourceDirectories`. The directories live under
   * `<module>/src/main/scala-shims/spark-X.Y/` and
   * `<module>/src/test/scala-shims/spark-X.Y/`.
   *
   * Source files placed in the shim directory of a Spark version are included only
   * when the connector is built against that Spark version. Symbols whose definition
   * differs between Spark versions (or that exist in only one Spark version) live in
   * these per-version directories rather than in the shared sources.
   */
  def sparkSourceDirSettings: Seq[Setting[_]] = {
    val dir = getSparkVersionSpec().additionalSourceDir
    Seq(
      Compile / unmanagedSourceDirectories +=
        (Compile / baseDirectory).value / "src" / "main" / dir,
      Test / unmanagedSourceDirectories +=
        (Test / baseDirectory).value / "src" / "test" / dir
    )
  }
}
