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
 *   - SparkVersionSpec: version metadata (full version, shim source dirs, source-build defaults);
 *     a version can list several shim dirs, incl. ones shared across versions (e.g. spark-4.0-4.1)
 *   - DEFAULT: latest stable Spark version (used when -DsparkVersion is not set)
 *   - sparkVersionedModuleName: appends _X.Y suffix to artifact names
 *   - sparkSourceDirSettings: wires per-version shim source dirs
 *   - source-build metadata: optional CI/cache overlay for testing a Spark source ref
 *     with an existing compatibility line; it is not a separate release target
 *   - sparkCommit: optional exact Spark commit built locally for source-built profiles
 */

case class SparkVersionSpec(
  fullVersion: String,
  // Shim source-dir suffixes compiled for this Spark version (e.g. "scala-shims/spark-4.0-4.1").
  // A file in a dir shared by several versions is compiled for every version that lists it, so
  // identical shims are written once instead of copied per version. sbt ignores non-existent
  // dirs, so listing a dir a module doesn't use is harmless.
  additionalSourceDirs: Seq[String],
  requiresSparkCommit: Boolean = false,
  sourceBuildArtifactBaseVersion: Option[String] = None,
  sourceBuildDefaultRef: Option[String] = None
) {
  def shortVersion: String = fullVersion.split("\\.").take(2).mkString(".")
  def isSnapshot: Boolean = fullVersion.contains("SNAPSHOT")

  // Numeric major/minor parsed from `fullVersion` (e.g. "4.2.0-SNAPSHOT" -> (4, 2)), for
  // version comparisons that must not rely on lexicographic string ordering (e.g. "4.10" vs "4.2").
  private def majorMinor: (Int, Int) = {
    val parts = fullVersion.split("[.-]")
    (parts(0).toInt, parts(1).toInt)
  }

  /** True iff this Spark version is >= the given major.minor. */
  def isAtLeast(major: Int, minor: Int): Boolean = majorMinor match {
    case (maj, min) => maj > major || (maj == major && min >= minor)
  }
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
            // `sourceDirs` is a JSON array of shim-dir suffixes; also accept a bare string.
            val sourceDirs = v("sourceDirs") match {
              case s: String => Seq(s)
              case l: List[_] => l.map(_.asInstanceOf[String])
            }
            SparkVersionSpec(
              v("version").asInstanceOf[String],
              sourceDirs,
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
    val candidates =
      if (input == "master") SparkVersionSpec.ALL_SPECS.filter(_.requiresSparkCommit)
      else SparkVersionSpec.ALL_SPECS.filter { spec =>
        spec.fullVersion == input || spec.shortVersion == input
      }

    val spec = if (getSparkCommit().isDefined || getSparkArtifactVersionOverride().isDefined) {
      candidates.find(_.requiresSparkCommit).orElse(candidates.headOption)
    } else {
      candidates.find(!_.requiresSparkCommit).orElse(candidates.headOption)
    }

    spec.getOrElse {
      val valid = SparkVersionSpec.ALL_SPECS.flatMap(s => Seq(s.fullVersion, s.shortVersion))
      throw new IllegalArgumentException(
        s"Invalid sparkVersion: $input. Valid values: ${(valid :+ "master").mkString(", ")}"
      )
    }
  }

  private def propertyOrEnv(propertyName: String, envName: String): Option[String] = {
    sys.props.get(propertyName).orElse(sys.env.get(envName)).filter(_.nonEmpty)
  }

  private def getSparkCommit(): Option[String] =
    propertyOrEnv("sparkCommit", "SPARK_COMMIT")

  private def getSparkArtifactVersionOverride(): Option[String] =
    propertyOrEnv("sparkArtifactVersion", "SPARK_ARTIFACT_VERSION")

  private def shortCommit(commit: String): String = {
    val trimmed = commit.trim.toLowerCase
    if (!trimmed.matches("[0-9a-f]{7,40}")) {
      throw new IllegalArgumentException(
        s"Invalid sparkCommit '$commit'. Expected a 7 to 40 character git SHA."
      )
    }
    trimmed.take(12)
  }

  def sparkArtifactVersionForCommit(spec: SparkVersionSpec, commit: String): String = {
    s"${spec.artifactBaseVersion}-${shortCommit(commit)}-SNAPSHOT"
  }

  def getSparkArtifactVersion(): String = {
    val spec = getSparkVersionSpec()
    getSparkArtifactVersionOverride().getOrElse {
      getSparkCommit() match {
        case Some(commit) => sparkArtifactVersionForCommit(spec, commit)
        case None if spec.requiresSparkCommit =>
          throw new IllegalArgumentException(
            s"Spark ${spec.fullVersion} requires -DsparkCommit=<sha> " +
              "or -DsparkArtifactVersion=<local-maven-version>."
          )
        case None => spec.fullVersion
      }
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
      Compile / unmanagedSourceDirectories ++=
        spec.additionalSourceDirs.map((Compile / baseDirectory).value / "src" / "main" / _),
      Test / unmanagedSourceDirectories ++=
        spec.additionalSourceDirs.map((Test / baseDirectory).value / "src" / "test" / _)
    )
  }

}
