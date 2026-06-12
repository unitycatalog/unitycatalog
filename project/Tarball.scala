import sbt.Keys.*
import sbt.*

import scala.sys.process.*
import scala.util.{Failure, Success, Try}
object Tarball {
  lazy val createTarball = taskKey[Unit]("Create a tarball for the project")
  lazy val stageDist =
    taskKey[File]("Stage the distribution tree (jars/ bin/ etc/) without tarring; returns the staged dir")

  // Stage the distribution layout into `outputDir`:
  //
  //   jars/server/*   server jar + its resolved runtime classpath
  //   jars/cli/*      cli jar    + its resolved runtime classpath
  //   bin/  etc/
  //
  // The server and CLI classpaths are kept in SEPARATE directories on purpose:
  // the CLI drags in old ANTLR (antlr4-4.5.1, via hadoop-client) while the server
  // needs antlr4-runtime-4.13.x (via Hibernate). sbt resolves each module's
  // classpath with correct eviction, so each directory is internally consistent.
  // The launchers use a JVM wildcard (`-cp jars/<component>/*`) over its own dir,
  // which is relocatable AND collision-free (no merged, order-dependent classpath).
  private def copyJarsInto(jars: Seq[File], dir: File): Unit =
    jars.distinct.foreach(j => IO.copyFile(j, dir / j.getName))

  def createTarballSettings(): Def.SettingsDefinition = {
    Seq(
      stageDist := {
        val log = streams.value.log
        val outputDir = target.value / "dist"

        val serverJar = (LocalProject("server") / Compile / packageBin).value.getAbsoluteFile
        val serverModelsJar = (LocalProject("serverModels") / Compile / packageBin).value.getAbsoluteFile
        val serverClasspath = (LocalProject("server") / Compile / managedClasspath).value.files

        val cliJar = (LocalProject("cli") / Compile / packageBin).value.getAbsoluteFile
        val clientJar = (LocalProject("client") / Compile / packageBin).value.getAbsoluteFile
        val controlApiJar = (LocalProject("controlApi") / Compile / packageBin).value.getAbsoluteFile
        val cliClasspath = (LocalProject("cli") / Compile / managedClasspath).value.files

        IO.delete(outputDir)
        IO.createDirectory(outputDir)

        copyJarsInto(serverClasspath :+ serverJar :+ serverModelsJar, outputDir / "jars" / "server")
        copyJarsInto(
          cliClasspath :+ cliJar :+ clientJar :+ controlApiJar :+ serverJar :+ serverModelsJar,
          outputDir / "jars" / "cli")

        IO.copyDirectory(baseDirectory.value / "bin", outputDir / "bin")
        IO.copyDirectory(baseDirectory.value / "etc", outputDir / "etc")

        log.info(s"Staged distribution at $outputDir")
        outputDir
      },
      createTarball := {
        val log = streams.value.log

        // Reuse the staged dist tree
        val outputDir = stageDist.value

        // Create the tarball
        val tarballFile = target.value / s"${name.value}-${version.value}.tar.gz"
        log.info(s"Creating tarball at $tarballFile")

        // Execute the tar command
        val tarCommand = Seq(
          "tar",
          "-czvf",
          tarballFile.getAbsolutePath,
          "-C",
          outputDir.getAbsolutePath,
          "."
        )

        Try(Process(tarCommand).!) match {
          case Success(0) => log.info(s"Tarball created successfully: $tarballFile")
          case Success(exitCode) => sys.error(s"Tarball creation failed with exit code $exitCode")
          case Failure(exception) =>
            sys.error(s"Tarball creation failed with exception: ${exception.getMessage}")
        }

        // Clean up the output directory
        IO.delete(outputDir)
        log.info(s"Deleted temporary directory: $outputDir")
      }
    )
  }
}
