import sbt._
import sbt.Keys._
import sbtrelease.ReleasePlugin.autoImport.ReleaseStep

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
 *   - crossSparkReleaseSteps: orchestrates multi-version release publishing
 *   - runOnlyForReleasableSparkModules: discovers and runs tasks on Spark-dependent modules
 */

case class SparkVersionSpec(
  fullVersion: String,
  additionalSourceDir: String
) {
  def shortVersion: String = fullVersion.split("\\.").take(2).mkString(".")
  def isSnapshot: Boolean = fullVersion.contains("SNAPSHOT")
}

object SparkVersionSpec {

  val spark40 = SparkVersionSpec(
    fullVersion = "4.0.0",
    additionalSourceDir = "scala-shims/spark-4.0"
  )

  val spark41 = SparkVersionSpec(
    fullVersion = "4.1.0",
    additionalSourceDir = "scala-shims/spark-4.1"
  )

  val spark42Snapshot = SparkVersionSpec(
    fullVersion = "4.2.0-SNAPSHOT",
    additionalSourceDir = "scala-shims/spark-4.2"
  )

  val DEFAULT: SparkVersionSpec = spark41
  val ALL_SPECS: Seq[SparkVersionSpec] = Seq(spark40, spark41, spark42Snapshot)
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
   * - sparkVersion setting (enables dynamic discovery by runOnlyForReleasableSparkModules)
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

  /**
   * Release steps that publish artifacts for all Spark versions.
   *
   * Step 1: publish ALL modules WITHOUT Spark suffix (backward compat)
   * Step 2+: publish Spark-dependent modules WITH suffix for each non-snapshot version
   *
   * Each step runs as a subprocess so SBT reloads with the correct settings.
   */
  def crossSparkReleaseSteps(task: String): Seq[ReleaseStep] = {

    def runSbtSubprocess(state: State, sbtArgs: Seq[String], description: String): State = {
      val extracted = Project.extract(state)
      val baseDir = extracted.get(ThisBuild / Keys.baseDirectory)
      val cmd = Seq(s"${baseDir.getAbsolutePath}/build/sbt") ++ sbtArgs
      println(s"[info] ========================================")
      println(s"[info] $description")
      println(s"[info] Running: ${cmd.mkString(" ")}")
      println(s"[info] ========================================")
      val exitCode = scala.sys.process.Process(cmd, baseDir).!
      if (exitCode != 0) {
        sys.error(s"$description failed with exit code $exitCode")
      }
      state
    }

    val backwardCompatStep: ReleaseStep = { (state: State) =>
      runSbtSubprocess(
        state,
        Seq("-DskipSparkSuffix=true", task),
        "Publishing all modules without Spark suffix (backward compat)"
      )
    }

    val suffixedSteps: Seq[ReleaseStep] = SparkVersionSpec.ALL_SPECS
      .filterNot(_.isSnapshot)
      .map { spec =>
        { (state: State) =>
          runSbtSubprocess(
            state,
            Seq(
              s"-DsparkVersion=${spec.fullVersion}",
              s"runOnlyForReleasableSparkModules $task"
            ),
            s"Publishing Spark-dependent modules with suffix for Spark ${spec.fullVersion}"
          )
        }: ReleaseStep
      }

    backwardCompatStep +: suffixedSteps
  }

  override lazy val projectSettings = Seq(
    commands += Command.args("runOnlyForReleasableSparkModules", "<task>") { (state, args) =>
      if (args.isEmpty) {
        sys.error(
          "Usage: runOnlyForReleasableSparkModules <task>\n" +
          "Example: build/sbt -DsparkVersion=4.0 \"runOnlyForReleasableSparkModules publishM2\""
        )
      }

      val task = args.mkString(" ")
      val extracted = sbt.Project.extract(state)
      val sparkVersionKey = autoImport.sparkVersion
      val sparkModules = extracted.structure.allProjectRefs.filter { ref =>
        val hasSpark = (ref / sparkVersionKey).get(extracted.structure.data).isDefined
        // Reads project-scoped publishArtifact (not Test/publishArtifact from build.sbt).
        val publishable = (ref / Keys.publishArtifact).get(extracted.structure.data).getOrElse(true)
        hasSpark && publishable
      }

      if (sparkModules.isEmpty) {
        println("[warn] No publishable Spark-dependent modules found")
        state
      } else {
        val names = sparkModules.map(_.project).mkString(", ")
        println(s"[info] Running '$task' on Spark-dependent modules: $names")
        sparkModules.foldLeft(state) { (s, ref) =>
          val scoped = if (task.startsWith("+")) {
            s"+${ref.project}/${task.stripPrefix("+")}"
          } else {
            s"${ref.project}/$task"
          }
          Command.process(scoped, s)
        }
      }
    },
    commands += Command.command("showSparkVersions") { state =>
      SparkVersionSpec.ALL_SPECS.foreach { spec =>
        println(spec.fullVersion)
      }
      state
    }
  )
}
