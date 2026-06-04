import sbt._
import sbt.Keys._
import scala.util.parsing.json.JSON

/**
 * Cross-Spark build and publish infrastructure for the UC Spark connector.
 *
 * Enables publishing per-Spark-version artifacts:
 *   unitycatalog-spark_4.0_2.13, unitycatalog-spark_4.1_2.13, unitycatalog-spark_4.2_2.13
 *
 * Mirrors the pattern from Delta Lake's CrossSparkVersions.scala, trimmed to UC's needs.
 *
 * Key concepts:
 *   - SparkVersionSpec: version metadata (full version, shim source dir, source-build defaults)
 *   - DEFAULT: latest stable Spark version (used when -DsparkVersion is not set)
 *   - sparkVersionedModuleName: appends _X.Y suffix to artifact names
 *   - sparkSourceDirSettings: wires per-version shim source dirs
 *   - source-build metadata: optional CI/cache overlay for testing a Spark source ref
 *     with an existing compatibility line; it is not a separate release target
 */

case class SparkVersionSpec(
  fullVersion: String,
  additionalSourceDir: String,
  requiresSparkCommit: Boolean = false,
  sourceBuildArtifactBaseVersion: Option[String] = None,
  sourceBuildDefaultRef: Option[String] = None
) {
  def shortVersion: String = fullVersion.split("\\.").take(2).mkString(".")
  def isSnapshot: Boolean = fullVersion.contains("SNAPSHOT")
  /** Base version used when deriving a local, commit-qualified artifact version for source-built Spark. */
  def artifactBaseVersion: String =
    sourceBuildArtifactBaseVersion.getOrElse(fullVersion.stripSuffix("-SNAPSHOT"))
}

object SparkVersionSpec {

  private def loadFromJson(): (String, Seq[SparkVersionSpec]) = {
    val jsonFile = new java.io.File("project/spark-versions.json")
    val source = scala.io.Source.fromFile(jsonFile)
    try {
      JSON.parseFull(source.mkString) match {
        case Some(map: Map[String, Any] @unchecked) =>
          val default = map("default").asInstanceOf[String]
          val versions = map("versions").asInstanceOf[List[Map[String, Any]]].map { v =>
            SparkVersionSpec(
              v("version").asInstanceOf[String],
              v("sourceDir").asInstanceOf[String],
              v.get("requiresSparkCommit").exists(_.asInstanceOf[Boolean]),
              v.get("sourceBuildArtifactBaseVersion").map(_.asInstanceOf[String]),
              v.get("sourceBuildDefaultRef").map(_.asInstanceOf[String])
            )
          }
          (default, versions)
        case _ =>
          sys.error("Failed to parse project/spark-versions.json")
      }
    } finally source.close()
  }

  private val (defaultVersion, allSpecs) = loadFromJson()

  val ALL_SPECS: Seq[SparkVersionSpec] = allSpecs
  val DEFAULT: SparkVersionSpec = ALL_SPECS.find(_.fullVersion == defaultVersion).getOrElse {
    sys.error(
      s"Default version '$defaultVersion' not found in spark-versions.json versions: " +
        ALL_SPECS.map(_.fullVersion).mkString(", ")
    )
  }
}

object CrossSparkVersions extends AutoPlugin {

  override def trigger = allRequirements

  object autoImport {
    val sparkVersion = settingKey[String]("The Spark version this module is built against")
  }

  /** Resolves the active SparkVersionSpec from `-DsparkVersion` (full or short form). */
  def getSparkVersionSpec(): SparkVersionSpec = {
    val input = sys.props.getOrElse("sparkVersion", SparkVersionSpec.DEFAULT.fullVersion)
    SparkVersionSpec.ALL_SPECS.find { spec =>
      spec.fullVersion == input || spec.shortVersion == input
    }.getOrElse {
      val valid = SparkVersionSpec.ALL_SPECS.flatMap(s => Seq(s.fullVersion, s.shortVersion))
      throw new IllegalArgumentException(
        s"Invalid sparkVersion: $input. Valid values: ${valid.mkString(", ")}"
      )
    }
  }

  /**
   * Returns the module name with a Spark major.minor suffix appended.
   * Pass `-DskipSparkSuffix=true` to suppress (used during release for backward-compat artifacts).
   */
  def sparkVersionedModuleName(baseName: String): String = {
    val skip = sys.props.getOrElse("skipSparkSuffix", "false").toBoolean
    if (skip) baseName
    else s"${baseName}_${getSparkVersionSpec().shortVersion}"
  }

  /**
   * Settings for Spark-dependent modules. Includes:
   * - sparkVersion setting (queryable via `show spark/sparkVersion`)
   * - Per-version shim source directories for main and test
   */
  def sparkDependentSettings: Seq[Setting[_]] = {
    val spec = getSparkVersionSpec()
    Seq(
      autoImport.sparkVersion := spec.fullVersion,
      Compile / unmanagedSourceDirectories +=
        (Compile / baseDirectory).value / "src" / "main" / spec.additionalSourceDir,
      Test / unmanagedSourceDirectories +=
        (Test / baseDirectory).value / "src" / "test" / spec.additionalSourceDir
    )
  }

}
