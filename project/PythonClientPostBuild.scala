import sbt._
import sbt.Keys._
import java.nio.file.{Files, Paths, Path, StandardCopyOption}
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

/**
 * Object containing build processing utilities for Python client generation.
 *
 * This object provides methods to prepare for code generation, process generated files,
 * and handle version updates, ensuring the build logic is encapsulated and `build.sbt` remains clean.
 */
object PythonClientPostBuild {

  /**
   * Prepares the environment for OpenAPI code generation.
   *
   * This method ensures that the target directory exists and copies the
   * `.openapi-generator-ignore` file into it.
   *
   * @param log              The logger to output informational messages.
   * @param baseDir          The base directory of the project.
   * @param openApiOutputDir The directory where the OpenAPI generator outputs the files.
   */
  def prepareGeneration(
      log: Logger,
      baseDir: File,
      openApiOutputDir: String
  ): Unit = {
    val targetPath = Paths.get(openApiOutputDir)

    Files.createDirectories(targetPath)
    log.info(s"Ensured target directory exists at $openApiOutputDir")

    val ignoreFileSource = baseDir.toPath.resolve("build/.openapi-generator-ignore")
    val ignoreFileTarget = targetPath.resolve(".openapi-generator-ignore")

    if (Files.exists(ignoreFileSource)) {
      Files.copy(ignoreFileSource, ignoreFileTarget, StandardCopyOption.REPLACE_EXISTING)
      log.info(s"Copied .openapi-generator-ignore to $openApiOutputDir")
    } else {
      sys.error(s".openapi-generator-ignore file not found at ${ignoreFileSource.toAbsolutePath}")
    }
  }

  /**
   * Processes the generated Python client files after OpenAPI code generation.
   *
   * This method copies custom files into the output directory and updates the version information.
   *
   * @param log              The logger to output informational messages.
   * @param openApiOutputDir The directory where the OpenAPI generator outputs the files.
   * @param baseDir          The base directory of the project.
   * @param version          The version string from sbt.
   */
  def processGeneratedFiles(
      log: Logger,
      openApiOutputDir: String,
      baseDir: File,
      version: String
  ): Unit = {

    val pythonVersion = convertVersionForPython(version)
    val buildDir = baseDir.toPath.resolve("build")

    Seq("setup.py", "README.md", "pyproject.toml").foreach { fileName =>
      val sourcePath = buildDir.resolve(fileName)
      val targetPath = Paths.get(openApiOutputDir, fileName)
      if (Files.exists(sourcePath)) {
        Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING)
        log.info(s"Copied $fileName to $targetPath.")
      } else {
        sys.error(s"The file $fileName was not found. Expected at: $sourcePath")
      }
    }

    Seq("pyproject.toml", "setup.py").foreach { fileName =>
      val filePath = Paths.get(openApiOutputDir, fileName)
      if (Files.exists(filePath)) {
        val content = Files.readAllLines(filePath, StandardCharsets.UTF_8).asScala
        val updatedContent = content.map { line =>
          val trimmedLine = line.trim
          if (trimmedLine.startsWith("version =")) {
            s"""version = "$pythonVersion""""
          } else if (trimmedLine.startsWith("version=")) {
            val prefix = line.takeWhile(_.isWhitespace)
            s"""${prefix}version="$pythonVersion","""
          } else {
            line
          }
        }
        Files.write(filePath, updatedContent.asJava, StandardCharsets.UTF_8)
        log.info(s"Updated version in $fileName to $pythonVersion")
      } else {
        sys.error(s"File not found: $filePath")
      }
    }
  }

  /**
   * Converts the sbt version string to a Python-compatible version string.
   *
   * If the version ends with "-SNAPSHOT", it replaces it with ".dev0".
   *
   * @param sbtVersion The version string from sbt.
   * @return The Python-compatible version string.
   */
  def convertVersionForPython(sbtVersion: String): String = {
    if (sbtVersion.endsWith("-SNAPSHOT")) {
      sbtVersion.stripSuffix("-SNAPSHOT") + ".dev0"
    } else {
      sbtVersion
    }
  }
}
