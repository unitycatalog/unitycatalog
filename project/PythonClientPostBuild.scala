import sbt._
import sbt.Keys._
import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.sys.process._
import scala.util.{Try, Success, Failure}

/**
 * Object containing build processing utilities for Python client generation.
 *
 * This object provides methods to prepare for code generation, process generated files,
 * and handle version updates by calling a Python script, ensuring the build logic is encapsulated
 * and `build.sbt` remains clean.
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

    Try(Files.createDirectories(targetPath)) match {
      case Success(_) =>
        log.info(s"Ensured target directory exists at $openApiOutputDir")
      case Failure(exception) =>
        sys.error(s"Failed to create target directory at $openApiOutputDir: ${exception.getMessage}")
    }

    val ignoreFileSource = baseDir.toPath.resolve("build").resolve(".openapi-generator-ignore")
    val ignoreFileTarget = targetPath.resolve(".openapi-generator-ignore")

    if (Files.exists(ignoreFileSource)) {
      Try(Files.copy(ignoreFileSource, ignoreFileTarget, StandardCopyOption.REPLACE_EXISTING)) match {
        case Success(_) =>
          log.info(s"Copied .openapi-generator-ignore to $openApiOutputDir")
        case Failure(exception) =>
          sys.error(s"Failed to copy .openapi-generator-ignore to $openApiOutputDir: ${exception.getMessage}")
      }
    } else {
      sys.error(s".openapi-generator-ignore file not found at ${ignoreFileSource.toAbsolutePath}")
    }
  }

  /**
   * Processes the generated Python client files after OpenAPI code generation.
   *
   * This method runs the Python script to update the version information in the build directory,
   * and then copies the updated files into the output directory.
   *
   * @param log              The logger to output informational messages.
   * @param openApiOutputDir The directory where the OpenAPI generator outputs the files.
   * @param baseDir          The base directory of the project.
   */
  def processGeneratedFiles(
      log: Logger,
      openApiOutputDir: String,
      baseDir: File
  ): Unit = {

    val buildDir = baseDir.toPath.resolve("build")
    val pythonScriptName = "update_version.py"
    val pythonScriptPath = buildDir.resolve(pythonScriptName)

    if (!Files.exists(pythonScriptPath)) {
      sys.error(s"Python script not found at ${pythonScriptPath.toAbsolutePath}")
    }

    val command = Seq("python3", pythonScriptPath.toString)
    log.info(s"Executing version update script: ${command.mkString(" ")}")

    val exitCode = Try(Process(command, buildDir.toFile).!) match {
      case Success(code) => code
      case Failure(exception) =>
        sys.error(s"Failed to execute version update script: ${exception.getMessage}")
    }

    if (exitCode != 0) {
      sys.error(s"Version update script failed with exit code $exitCode")
    } else {
      log.info("Version update script executed successfully")
    }

    Seq("setup.py", "README.md", "pyproject.toml").foreach { fileName =>
      val sourcePath = buildDir.resolve(fileName)
      val targetPath = Paths.get(openApiOutputDir, fileName)

      if (Files.exists(sourcePath)) {
        Try {
          Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING)
        } match {
          case Success(_) =>
            log.info(s"Copied $fileName to $targetPath.")
          case Failure(exception) =>
            sys.error(s"Failed to copy $fileName to $targetPath: ${exception.getMessage}")
        }
      } else {
        sys.error(s"The file $fileName was not found. Expected at: $sourcePath")
      }
    }
  }
}