import sbt.Keys.*
import sbt.*

import java.nio.file.Files
import scala.sys.process.*
import scala.util.{Failure, Success, Try}
object Tarball {
  lazy val createTarball = taskKey[Unit]("Create a tarball for the project")
  private lazy val buildTarballSampleDatabase =
    taskKey[File]("Build a new sample database for the tarball")

  def createTarballSettings(): Def.SettingsDefinition = {
    Seq(
      // Built anew, so the tarball never ships the sample database a local server has changed.
      buildTarballSampleDatabase := Def.taskDyn {
        val file = target.value / "sample-database" / "h2db.mv.db"
        IO.delete(file.getParentFile)
        Def.task {
          (LocalProject("sampleDatabase") / Compile / runMain)
            .toTask(" io.unitycatalog.server.utils.SampleDatabase \"" + file + "\"")
            .value
          file
        }
      }.value,
      createTarball := {
        val log = streams.value.log
        val sampleDatabase = buildTarballSampleDatabase.value

        // Output directory for the tarball
        val outputDir = target.value / "dist"
        val projectJarFiles = Seq(
          (LocalProject("server") / Compile / packageBin).value.getAbsoluteFile,
          (LocalProject("cli") / Compile / packageBin).value.getAbsoluteFile,
          (LocalProject("client") / Compile / packageBin).value.getAbsoluteFile,
          (LocalProject("controlApi") / Compile / packageBin).value.getAbsoluteFile,
          (LocalProject("serverModels") / Compile / packageBin).value.getAbsoluteFile
        )
        val scriptsDir = baseDirectory.value / "bin"
        val etcDir = baseDirectory.value / "etc"

        // All JAR files in the classpath
        val allJars = (LocalProject("server") / Compile / managedClasspath).value.files ++
          (LocalProject("cli") / Compile / managedClasspath).value.files ++
          projectJarFiles

        // Clean and create the output directory
        IO.delete(outputDir)
        IO.createDirectory(outputDir)

        // Copy the JAR file to the output directory
        allJars.foreach { jarFile =>
          IO.copyFile(jarFile, outputDir / "jars" / jarFile.getName)
        }
        val classpathFile = outputDir / "jars" / "classpath"
        Files.write(classpathFile.toPath, allJars.mkString(":").getBytes)
        // Copy the script files to the output directory
        IO.copyDirectory(scriptsDir, outputDir / "bin")
        // Copy the etc files to the output directory, with the new sample database
        IO.copyDirectory(etcDir, outputDir / "etc")
        // A local sample database, and what a killed sample database build leaves behind.
        IO.delete((outputDir / "etc" / "db" * ("h2db.*" || "building-*")).get)
        IO.copyFile(sampleDatabase, outputDir / "etc" / "db" / "h2db.mv.db")

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
