import sbt._
import sbt.Keys._
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.ReleaseStateTransformations._

import scala.language.implicitConversions

/**
 * This plugin provides support for building and releasing Unity Catalog for multiple Spark versions.
 * 
 * The plugin:
 * 1. Defines supported Spark versions with their specifications (Java version, Jackson version, etc.)
 * 2. Provides settings for configuring Spark-dependent modules
 * 3. Handles artifact naming (adds Spark version suffix for non-latest versions)
 * 4. Provides release steps for publishing artifacts for all supported Spark versions
 * 
 * Artifact naming convention:
 * - Latest Spark version: unitycatalog-spark_2.13
 * - Older Spark versions: unitycatalog-spark_X.Y_2.13 (e.g., unitycatalog-spark_4.0_2.13)
 * 
 * Build examples:
 * - Default build (latest Spark): build/sbt publishM2
 * - Specific Spark version: build/sbt -DsparkVersion=4.0.0 publishM2
 * - Build for specific Spark only: build/sbt -DsparkVersion=4.0.0 "runOnlyForSparkModules publishM2"
 * 
 * Release workflow:
 * - Run: build/sbt release
 * - This will automatically publish artifacts for all supported Spark versions
 */
object CrossSparkVersions {

  /**
   * Specification for a supported Spark version
   */
  case class SparkVersionSpec(
    sparkVersion: String,
    scalaVersion: String,
    javacRelease: String,
    jacksonVersion: String,
    deltaVersion: String,
    hadoopVersion: String,
    antlrVersion: String
  ) {
    val sparkMajorMinor: String = sparkVersion.split("\\.").take(2).mkString(".")
    
    // Extract X.Y from version string like "4.0.0"
    def getMajorMinor: (Int, Int) = {
      val parts = sparkVersion.split("\\.").map(_.toInt)
      (parts(0), parts(1))
    }
  }

  // ==================================================================================
  // Supported Spark versions
  // ==================================================================================

  val spark40Spec = SparkVersionSpec(
    sparkVersion = "4.0.0",
    scalaVersion = "2.13.16",
    javacRelease = "17",
    jacksonVersion = "2.15.0",
    deltaVersion = "4.0.0",
    hadoopVersion = "3.4.0",
    antlrVersion = "4.13.1"
  )

  // All supported versions in order (latest last)
  val allSparkVersions: Seq[SparkVersionSpec] = Seq(spark40Spec)
  
  // The latest/default Spark version
  val latestSparkSpec: SparkVersionSpec = allSparkVersions.last

  // ==================================================================================
  // Version selection and configuration
  // ==================================================================================

  /**
   * Get the Spark version spec to use based on system property
   */
  def getSparkVersionSpec(): SparkVersionSpec = {
    val requestedVersion = sys.props.get("sparkVersion")
    requestedVersion match {
      case Some(v) =>
        allSparkVersions.find(_.sparkVersion == v) match {
          case Some(spec) => spec
          case None =>
            throw new IllegalArgumentException(
              s"Unsupported Spark version: $v. Supported versions: ${allSparkVersions.map(_.sparkVersion).mkString(", ")}"
            )
        }
      case None => latestSparkSpec
    }
  }

  /**
   * Check if the current build is for the latest Spark version
   */
  def isLatestSpark(spec: SparkVersionSpec): Boolean = {
    spec.sparkVersion == latestSparkSpec.sparkVersion
  }

  // ==================================================================================
  // Artifact naming
  // ==================================================================================

  /**
   * Get the artifact name suffix for Spark-dependent modules.
   * For the latest Spark version, no suffix is added.
   * For older versions, add "_X.Y" suffix (e.g., "_4.0")
   */
  def getSparkVersionSuffix(spec: SparkVersionSpec): String = {
    if (isLatestSpark(spec)) {
      ""
    } else {
      s"_${spec.sparkMajorMinor}"
    }
  }

  /**
   * SBT setting to configure the module name for Spark-dependent modules
   */
  def sparkDependentModuleName(sparkVersionKey: SettingKey[String]): Def.Setting[String] = {
    moduleName := {
      val baseName = name.value
      val spec = getSparkVersionSpec()
      baseName + getSparkVersionSuffix(spec)
    }
  }

  // ==================================================================================
  // Build settings for Spark-dependent modules
  // ==================================================================================

  /**
   * Common settings for all Spark-dependent modules
   */
  def sparkDependentSettings(sparkVersionKey: SettingKey[String]): Seq[Setting[_]] = {
    val spec = getSparkVersionSpec()
    
    Seq(
      sparkVersionKey := spec.sparkVersion,
      scalaVersion := spec.scalaVersion,
      
      // Configure Java release version
      Compile / compile / javacOptions ++= {
        if (spec.javacRelease == "11") {
          Seq("--release", "11")
        } else {
          Seq("--release", "17")
        }
      }
    )
  }

  /**
   * Library dependencies for Spark modules
   */
  def sparkDependencies(spec: SparkVersionSpec): Seq[ModuleID] = {
    Seq(
      "org.apache.spark" %% "spark-sql" % spec.sparkVersion % Provided,
      "com.fasterxml.jackson.core" % "jackson-databind" % spec.jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % spec.jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-annotations" % spec.jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-core" % spec.jacksonVersion,
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % spec.jacksonVersion,
      "org.antlr" % "antlr4-runtime" % spec.antlrVersion,
      "org.antlr" % "antlr4" % spec.antlrVersion
    )
  }

  /**
   * Test dependencies for Spark modules
   */
  def sparkTestDependencies(spec: SparkVersionSpec): Seq[ModuleID] = {
    Seq(
      "org.apache.spark" %% "spark-sql" % spec.sparkVersion % Test,
      "io.delta" %% "delta-spark" % spec.deltaVersion % Test
    )
  }

  /**
   * Dependency overrides for Spark modules
   */
  def sparkDependencyOverrides(spec: SparkVersionSpec): Seq[ModuleID] = {
    Seq(
      "com.fasterxml.jackson.core" % "jackson-databind" % spec.jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % spec.jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-annotations" % spec.jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-core" % spec.jacksonVersion,
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % spec.jacksonVersion,
      "org.antlr" % "antlr4-runtime" % spec.antlrVersion,
      "org.antlr" % "antlr4" % spec.antlrVersion
    )
  }

  // ==================================================================================
  // SBT commands for selective building
  // ==================================================================================

  /**
   * Names of Spark-dependent modules that should be built for each Spark version
   */
  val sparkDependentModuleNames = Seq("spark", "integrationTests")

  /**
   * Command to run a task only for Spark-dependent modules
   * Usage: runOnlyForSparkModules <task>
   * Example: build/sbt -DsparkVersion=4.0.0 "runOnlyForSparkModules publishM2"
   */
  val runOnlyForSparkModulesCommand = Command.args("runOnlyForSparkModules", "<task>") { (state, args) =>
    if (args.isEmpty) {
      println("Usage: runOnlyForSparkModules <task>")
      println("Example: build/sbt -DsparkVersion=4.0.0 \"runOnlyForSparkModules publishM2\"")
      state.fail
    } else {
      val task = args.mkString(" ")
      val commands = sparkDependentModuleNames.map { module =>
        s"$module/$task"
      }
      commands.toList ::: state
    }
  }

  // ==================================================================================
  // Release configuration
  // ==================================================================================

  /**
   * Generate release steps for publishing artifacts for all Spark versions
   * 
   * @param publishTask The publish task to run (e.g., "+publishSigned")
   * @return Sequence of release steps
   */
  def crossSparkReleaseSteps(publishTask: String): Seq[ReleaseStep] = {
    // Step 1: Publish all modules for the latest Spark version
    val latestSparkSteps = Seq[ReleaseStep](
      releaseStepCommandAndRemaining(publishTask)
    )

    // Step 2: For each older Spark version, publish only Spark-dependent modules
    val olderSparkSteps = allSparkVersions.reverse.tail.flatMap { spec =>
      Seq[ReleaseStep](
        releaseStepCommandAndRemaining(
          s"""-DsparkVersion=${spec.sparkVersion} "runOnlyForSparkModules $publishTask" """
        )
      )
    }

    latestSparkSteps ++ olderSparkSteps
  }

  /**
   * Print information about the current Spark build configuration
   */
  def printSparkInfo(): Unit = {
    val spec = getSparkVersionSpec()
    println("=" * 80)
    println("Unity Catalog Cross-Spark Build Configuration")
    println("=" * 80)
    println(s"Spark Version: ${spec.sparkVersion}")
    println(s"Scala Version: ${spec.scalaVersion}")
    println(s"Java Release: ${spec.javacRelease}")
    println(s"Jackson Version: ${spec.jacksonVersion}")
    println(s"Delta Version: ${spec.deltaVersion}")
    println(s"Hadoop Version: ${spec.hadoopVersion}")
    println(s"Latest Spark: ${isLatestSpark(spec)}")
    println(s"Artifact Suffix: ${getSparkVersionSuffix(spec)}")
    println("=" * 80)
  }

  // Print info when plugin is loaded
  printSparkInfo()
}

