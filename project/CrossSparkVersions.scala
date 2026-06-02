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
 *   - SparkVersionSpec: version metadata (full version, shim source dir)
 *   - DEFAULT: latest stable Spark version (used when -DsparkVersion is not set)
 *   - sparkVersionedModuleName: appends _X.Y suffix to artifact names
 *   - sparkSourceDirSettings: wires per-version shim source dirs
 *   - sparkCommit: optional exact Spark commit built locally for source-built profiles
 */

case class SparkVersionSpec(
  fullVersion: String,
  additionalSourceDir: String,
  requiresSparkCommit: Boolean = false
) {
  def shortVersion: String = fullVersion.split("\\.").take(2).mkString(".")
  def isSnapshot: Boolean = fullVersion.contains("SNAPSHOT")
  def artifactBaseVersion: String = fullVersion.stripSuffix("-SNAPSHOT")
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
              v.get("requiresSparkCommit").exists(_.asInstanceOf[Boolean])
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
      Compile / unmanagedSourceDirectories +=
        (Compile / baseDirectory).value / "src" / "main" / spec.additionalSourceDir,
      Test / unmanagedSourceDirectories +=
        (Test / baseDirectory).value / "src" / "test" / spec.additionalSourceDir
    )
  }

}
