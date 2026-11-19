import sbt._
import sbt.Keys._

import scala.util.Try

/**
 * Opt-in parallel test execution: splits each project's test suites across several forked JVMs.
 *
 * By default sbt forks one JVM per project and runs its suites serially, one project at a time.
 * When TEST_PARALLELISM is set, each project's suites are split into groups, each run in its own
 * forked JVM, and at most TEST_PARALLELISM test JVMs run at once across all projects.
 * TEST_JVM_HEAP, if set, is the max heap of each test JVM (e.g. `4g`).
 *
 * `settings` goes on every project; `globalSettings` goes once on the build.
 *
 * Suites are assigned longest first to the least loaded group. Durations come from
 * TEST_DURATIONS_FILE (CSV of `suite,seconds`, as written by dev/run-tests.py); a suite missing
 * from it counts as the average known duration. A project gets no more groups than needed to
 * finish in the time of its longest suite, which leaves JVM slots to other projects. Each group
 * gets its own java.io.tmpdir under the project's target directory.
 */
object TestParallelization {

  private lazy val parallelism: Option[Int] = sys.env.get("TEST_PARALLELISM").map { value =>
    Try(value.trim.toInt).toOption.filter(_ >= 1).getOrElse(
      sys.error(s"TEST_PARALLELISM must be a positive integer, got '$value'"))
  }

  private lazy val jvmHeapOptions: Seq[String] =
    sys.env.get("TEST_JVM_HEAP").map(_.trim).filter(_.nonEmpty).map(heap => s"-Xmx$heap").toSeq

  private lazy val suiteDurations: Map[String, Double] =
    sys.env.get("TEST_DURATIONS_FILE").map(file).filter(_.isFile).toSeq
      .flatMap(f => IO.readLines(f))
      .flatMap { line =>
        line.split(',') match {
          case Array(suite, seconds) => Try(suite.trim -> seconds.trim.toDouble).toOption
          case _ => None
        }
      }
      .toMap

  lazy val settings: Seq[Setting[_]] = parallelism.toSeq.flatMap { maxJvms =>
    Seq(
      Test / testGrouping := {
        val tests = (Test / definedTests).value
        val forkOpts = (Test / forkOptions).value
        val tmpRoot = target.value / "test-tmp"
        IO.delete(tmpRoot)
        val groups = balance(tests, maxJvms)
        val estimate = groups.map(_._2).foldLeft(0.0)(math.max)
        streams.value.log.info(
          f"${name.value}: ${tests.size} suites in ${groups.size} test JVMs, " +
            f"longest group ~$estimate%.0fs")
        groups.zipWithIndex.map { case ((groupTests, load), i) =>
          streams.value.log.info(
            f"${name.value} test-group-$i: ${groupTests.size} suites, ~$load%.0fs")
          val tmpDir = tmpRoot / i.toString
          IO.createDirectory(tmpDir)
          val opts = forkOpts.withRunJVMOptions(
            forkOpts.runJVMOptions ++ jvmHeapOptions :+
              s"-Djava.io.tmpdir=${tmpDir.getAbsolutePath}")
          Tests.Group(s"test-group-$i", groupTests, Tests.SubProcess(opts))
        }
      }
    )
  }

  lazy val globalSettings: Seq[Setting[_]] = parallelism.toSeq.map { maxJvms =>
    // Replaces sbt's default rules, which allow a single forked test group at a time. This drops
    // the rules plugins add too, so restate them.
    Global / concurrentRestrictions := Seq(
      Tags.limitAll(java.lang.Runtime.getRuntime.availableProcessors),
      Tags.limit(Tags.ForkedTestGroup, maxJvms),
      Tags.exclusiveGroup(Tags.Clean),
      // sbt-assembly's own rule.
      Tags.limit(sbtassembly.Assembly.assemblyTag, 1))
  }

  /**
   * Splits tests into groups of roughly equal total duration. Uses at most `maxGroups` groups, and
   * no more than `total / longest`: more groups could not finish before the longest suite does.
   * Returns each group with its estimated duration in seconds.
   */
  private def balance(
      tests: Seq[TestDefinition],
      maxGroups: Int): Seq[(Seq[TestDefinition], Double)] = {
    if (tests.isEmpty) return Seq.empty
    val known = tests.flatMap(t => suiteDurations.get(t.name))
    val defaultDuration = if (known.isEmpty) 1.0 else known.sum / known.size
    def duration(t: TestDefinition) = suiteDurations.getOrElse(t.name, defaultDuration)

    val sorted = tests.sortBy(t => (-duration(t), t.name))
    val total = sorted.map(duration).sum
    val longest = duration(sorted.head)
    val numGroups = Seq(maxGroups, tests.size, math.ceil(total / longest).toInt).min.max(1)
    val loads = Array.fill(numGroups)(0.0)
    val groups = Array.fill(numGroups)(Vector.empty[TestDefinition])
    sorted.foreach { t =>
      val i = loads.indices.minBy(loads)
      loads(i) += duration(t)
      groups(i) :+= t
    }
    groups.toSeq.zip(loads)
  }
}
