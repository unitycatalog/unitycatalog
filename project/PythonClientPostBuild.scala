import sbt._
import sbt.Keys._
import java.nio.file.{Files, Paths, Path, StandardCopyOption}
import java.nio.charset.StandardCharsets

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

    if (!Files.exists(targetPath)) {
      Files.createDirectories(targetPath)
      log.info(s"Created target directory at $openApiOutputDir")
    }

    val ignoreFileSource = baseDir.toPath.resolve("build").resolve(".openapi-generator-ignore")
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
   * @param log                 The logger to output informational messages.
   * @param openApiOutputDir    The directory where the OpenAPI generator outputs the files.
   * @param baseDir             The base directory of the project.
   * @param version             The version string from sbt.
   */
  def processGeneratedFiles(
      log: Logger,
      openApiOutputDir: String,
      baseDir: File,
      version: String
  ): Unit = {

    /**
     * Copies a custom file to the output directory, overwriting if it exists.
     *
     * @param source       The path to the custom file to copy from.
     * @param target       The path to the location to copy to.
     * @param description  A description of the file being copied, used in log messages.
     */
    def copyFile(source: Path, target: Path, description: String): Unit = {
      if (Files.exists(source)) {
        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING)
        log.info(s"Copied $description to ${target}.")
      } else {
        sys.error(s"Required $description not found! Expected at: ${source.toAbsolutePath}")
      }
    }

    /**
     * Updates the version in the specified file.
     *
     * @param filePath  The path to the file to update.
     * @param version   The version string to set.
     */
    def updateVersionInFile(filePath: Path, version: String): Unit = {
      if (Files.exists(filePath)) {
        val content = Files.readAllLines(filePath, StandardCharsets.UTF_8)
        val updatedContent = content.toArray.map {
          case line: String =>
            if (line.trim.startsWith("version =")) {
              s"""version = "$version""""
            } else if (line.trim.startsWith("    version=")) {
              s"""    version="$version",""""
            } else {
              line
            }
        }
        Files.write(filePath, updatedContent.mkString("\n").getBytes(StandardCharsets.UTF_8))
        log.info(s"Updated version in ${filePath.getFileName} to $version")
      } else {
        sys.error(s"File not found: ${filePath.toAbsolutePath}")
      }
    }

    val pythonVersion = convertVersionForPython(version)

    val buildDir = baseDir.toPath.resolve("build")
    val clientPyproject = buildDir.resolve("pyproject.toml")
    val clientSetupPy = buildDir.resolve("setup.py")
    val projectReadme = buildDir.resolve("README.md")

    val generatedPyproject = Paths.get(openApiOutputDir, "pyproject.toml")
    val generatedSetupPy = Paths.get(openApiOutputDir, "setup.py")
    val generatedReadme = Paths.get(openApiOutputDir, "README.md")

    copyFile(clientPyproject, generatedPyproject, "pyproject.toml")
    copyFile(clientSetupPy, generatedSetupPy, "setup.py")
    copyFile(projectReadme, generatedReadme, "README.md")

    updateVersionInFile(generatedPyproject, pythonVersion)
    updateVersionInFile(generatedSetupPy, pythonVersion)
  }

  /**
   * Converts the sbt version string to a Python-compatible version string.
   *
   * If the version ends with "-SNAPSHOT", it replaces it with ".dev0".
   *
   * @param sbtVersion  The version string from sbt.
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
