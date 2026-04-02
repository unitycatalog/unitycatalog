import sbt._
import sbt.Keys._
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.util.Comparator
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
    val versionScriptName = "update-python-versions.sh"
    val versionScriptPath = buildDir.resolve(versionScriptName)

    if (!Files.exists(versionScriptPath)) {
      sys.error(s"Version updating script not found at ${versionScriptPath.toAbsolutePath}")
    }

    val command = Seq("bash", versionScriptPath.toString)
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
          Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING)
          log.info(s"Copied $fileName to $targetPath.")
      } else {
        sys.error(s"The file $fileName was not found. Expected at: $sourcePath")
      }
    }
    moveGeneratedUnityCatalog(log, openApiOutputDir)
    patchDeltaTypeModule(log, baseDir, openApiOutputDir)
  }

  /**
   * Copies the hand-written delta_type_module.py into the generated delta package
   * and appends an import to the generated __init__.py so the patch is applied
   * automatically when the package is imported.
   */
  def patchDeltaTypeModule(
      log: Logger,
      baseDir: File,
      openApiOutputDir: String
  ): Unit = {
    val srcModule = baseDir.toPath.resolve("src")
      .resolve("unitycatalog").resolve("delta").resolve("delta_type_module.py")
    val targetDir = Paths.get(openApiOutputDir, "src", "unitycatalog", "delta")
    val targetModule = targetDir.resolve("delta_type_module.py")
    val targetInit = targetDir.resolve("__init__.py")

    if (!Files.exists(srcModule)) {
      log.warn(s"delta_type_module.py not found at $srcModule, skipping patch")
      return
    }
    if (!Files.exists(targetDir)) {
      log.warn(s"Delta package not found at $targetDir, skipping patch")
      return
    }

    // Copy the module
    Files.copy(srcModule, targetModule, StandardCopyOption.REPLACE_EXISTING)
    log.info(s"Copied delta_type_module.py to $targetModule")

    // Append import to models/__init__.py so the patch activates on any model import
    val modelsInit = targetDir.resolve("models").resolve("__init__.py")
    if (Files.exists(modelsInit)) {
      val patchLine = "\n# Auto-import DeltaType string-or-object patch\nimport unitycatalog.delta.delta_type_module  # noqa: F401\n"
      val content = new String(Files.readAllBytes(modelsInit))
      if (!content.contains("delta_type_module")) {
        Files.write(modelsInit, (content + patchLine).getBytes)
        log.info(s"Patched $modelsInit with delta_type_module import")
      } else {
        log.info(s"$modelsInit already patched, skipping")
      }
    } else {
      log.warn(s"$modelsInit not found, skipping patch")
    }
  }

    /**
   * Moves the generated 'unitycatalog' directory from target/unitycatalog to target/src/unitycatalog.
   * This is required for hatch to properly build a shared namespace package without conflicts.
   *
   * @param log              The logger to output informational messages.
   * @param openApiOutputDir The directory where the OpenAPI generator outputs the files (target/).
   */
  def moveGeneratedUnityCatalog(
      log: Logger,
      openApiOutputDir: String
  ): Unit = {
    val sourceDir = Paths.get(openApiOutputDir, "unitycatalog")
    val targetDir = Paths.get(openApiOutputDir, "src", "unitycatalog")

    if (!Files.exists(sourceDir)) {
      sys.error(s"Generated 'unitycatalog' directory not found at $sourceDir")
    }

    if (Files.exists(targetDir)) {
      log.info(s"Target directory $targetDir already exists. Deleting it to overwrite.")
      deleteRecursively(targetDir) match {
        case Success(_) =>
          log.info(s"Successfully deleted existing target directory at $targetDir")
        case Failure(exception) =>
          sys.error(s"Failed to delete existing target directory at $targetDir: ${exception.getMessage}")
      }
    }

    val targetParentDir = targetDir.getParent
    Try(Files.createDirectories(targetParentDir)) match {
      case Success(_) =>
        log.info(s"Ensured target directory exists at $targetParentDir")
      case Failure(exception) =>
        sys.error(s"Failed to create target directory at $targetParentDir: ${exception.getMessage}")
    }

    Try(Files.move(sourceDir, targetDir, StandardCopyOption.REPLACE_EXISTING)) match {
      case Success(_) =>
        log.info(s"Moved 'unitycatalog' from $sourceDir to $targetDir")
      case Failure(exception) =>
        sys.error(s"Failed to move 'unitycatalog' to $targetDir: ${exception.getMessage}")
    }
  }

  def deleteRecursively(path: Path): Try[Unit] = {
    Try {
      if (Files.exists(path)) {
        Files.walk(path)
          .sorted(Comparator.reverseOrder())
          .forEach { p =>
            Try(Files.delete(p)) match {
              case Success(_) => // Deleted successfully
              case Failure(e) => sys.error(s"Failed to delete $p: ${e.getMessage}")
            }
          }
      }
    }
  }
}
