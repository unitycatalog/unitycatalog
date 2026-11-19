import sbt._
import sbt.Keys._
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.util.Comparator
import scala.sys.process._
import scala.jdk.CollectionConverters._
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

    Seq("README.md", "pyproject.toml").foreach { fileName =>
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
    clientPackageInitProblem(Paths.get(openApiOutputDir, "src", "unitycatalog", "client", "__init__.py"))
      .foreach(problem => sys.error(problem))
    relocateDeltaImports(log, openApiOutputDir)
    patchDeltaDataTypeModule(log, baseDir, openApiOutputDir)
  }

  /** Matches an import from the bare `delta` package, which the generator writes for the delta.yaml run. */
  private val bareDeltaImport = """(?m)^\s*(from|import)\s+delta\.""".r

  /**
   * Says why the generated unitycatalog/client/__init__.py is not the one from the all.yaml run, or returns
   * None if it is. The delta.yaml run writes this file too, with imports from the bare delta package, so it
   * has to run first. Both versions export ApiClient, so the check looks for the UC API imports and for no
   * bare delta import instead. Called by processGeneratedFiles.
   */
  def clientPackageInitProblem(clientInit: Path): Option[String] = {
    if (!Files.exists(clientInit)) {
      return Some(s"$clientInit was not generated.")
    }
    val content = new String(Files.readAllBytes(clientInit), StandardCharsets.UTF_8)
    // The trailing dot keeps this from matching the api_client import. These are the generator's exact import
    // lines, so a change to its import style also fails here, with the run-order message.
    val fromAllYaml = content.contains("from unitycatalog.client.api.") &&
      content.contains("from unitycatalog.client.api_client import ApiClient") &&
      bareDeltaImport.findFirstIn(content).isEmpty
    if (fromAllYaml) None
    else
      Some(
        s"$clientInit is not the one from the all.yaml run. The all.yaml run in pythonClient/generate must " +
          "run last, after the delta.yaml run, or the generator's import style changed.")
  }

  private val unrelocatedDeltaImports = Seq(
    """(?m)^\s*(from|import)\s+unitycatalog\.client\.models\b""".r,
    bareDeltaImport
  )

  /**
   * Points the imports in the generated unitycatalog.client.delta package at its own modules. The Python
   * generator writes the delta.yaml models into the configured modelPackage but always imports them from
   * <packageName>.models, and the api/__init__.py imports from the bare apiPackage. Also writes an empty
   * unitycatalog/client/delta/__init__.py, which the generator does not emit, so delta is a regular package.
   * Runs before patchDeltaDataTypeModule copies the serde module in. Fails the build if client/delta is
   * missing, or if any module under it still imports from unitycatalog.client.models or the bare delta
   * package after the rewrite, so a change in how the generator spells these imports breaks the build
   * instead of leaving the Delta APIs on the UC models.
   */
  def relocateDeltaImports(
      log: Logger,
      openApiOutputDir: String
  ): Unit = {
    val deltaDir = Paths.get(openApiOutputDir, "src", "unitycatalog", "client", "delta")
    if (!Files.exists(deltaDir)) {
      sys.error(s"Delta package not found at $deltaDir")
    }
    val rewrites = Seq(
      "unitycatalog.client.models." -> "unitycatalog.client.delta.models.",
      "from delta.api." -> "from unitycatalog.client.delta.api."
    )
    val stream = Files.walk(deltaDir)
    val files = try stream.iterator().asScala.filter(_.toString.endsWith(".py")).toList finally stream.close()
    val leftovers = files.flatMap { file =>
      val content = new String(Files.readAllBytes(file), StandardCharsets.UTF_8)
      val relocated = rewrites.foldLeft(content) { case (text, (from, to)) => text.replace(from, to) }
      Files.write(file, relocated.getBytes(StandardCharsets.UTF_8))
      unrelocatedDeltaImports.filter(_.findFirstIn(relocated).isDefined).map(pattern => s"$file matches $pattern")
    }
    if (leftovers.nonEmpty) {
      sys.error(s"Delta imports were not relocated: ${leftovers.mkString("; ")}")
    }
    val packageInit = deltaDir.resolve("__init__.py")
    if (!Files.exists(packageInit)) {
      Files.write(packageInit, Array.emptyByteArray)
    }
    log.info(s"Relocated the imports under $deltaDir to unitycatalog.client.delta")
  }

  /**
   * Copies the hand-written delta_data_type_module.py into the generated unitycatalog.client.delta
   * package's internal/serde/ subpackage and appends an import to the generated delta models/__init__.py
   * so the patch is applied automatically when the package is imported. Fails the build if the source
   * module, the generated delta package or its models/__init__.py is missing, so the patch can never be
   * silently dropped.
   */
  def patchDeltaDataTypeModule(
      log: Logger,
      baseDir: File,
      openApiOutputDir: String
  ): Unit = {
    val srcModule = baseDir.toPath.resolve("src")
      .resolve("unitycatalog").resolve("client").resolve("delta").resolve("internal").resolve("serde")
      .resolve("delta_data_type_module.py")
    val targetDir = Paths.get(openApiOutputDir, "src", "unitycatalog", "client", "delta")
    val targetInternalDir = targetDir.resolve("internal")
    val targetSerdeDir = targetInternalDir.resolve("serde")
    val targetModule = targetSerdeDir.resolve("delta_data_type_module.py")

    if (!Files.exists(srcModule)) {
      sys.error(s"delta_data_type_module.py not found at $srcModule")
    }
    if (!Files.exists(targetDir)) {
      sys.error(s"Delta package not found at $targetDir")
    }

    // Copy the module into internal/serde/ subdirectory
    Files.createDirectories(targetSerdeDir)
    Files.copy(srcModule, targetModule, StandardCopyOption.REPLACE_EXISTING)
    // Create __init__.py for the internal and serde packages
    val internalInit = targetInternalDir.resolve("__init__.py")
    if (!Files.exists(internalInit)) {
      Files.writeString(internalInit, "")
    }
    val serdeInit = targetSerdeDir.resolve("__init__.py")
    if (!Files.exists(serdeInit)) {
      Files.writeString(serdeInit, "")
    }
    log.info(s"Copied delta_data_type_module.py to $targetModule")

    // Append import to models/__init__.py so the patch activates on any model import
    val modelsInit = targetDir.resolve("models").resolve("__init__.py")
    if (Files.exists(modelsInit)) {
      val patchLine = "\n# Auto-import DeltaDataType string-or-object patch\nimport unitycatalog.client.delta.internal.serde.delta_data_type_module  # noqa: F401\n"
      val content = new String(Files.readAllBytes(modelsInit))
      if (!content.contains("delta_data_type_module")) {
        Files.write(modelsInit, (content + patchLine).getBytes)
        log.info(s"Patched $modelsInit with delta_data_type_module import")
      } else {
        log.info(s"$modelsInit already patched, skipping")
      }
    } else {
      sys.error(s"$modelsInit not found, so the DeltaDataType patch would not be auto-imported")
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
