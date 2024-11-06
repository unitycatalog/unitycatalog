import sbt._
import sbt.Keys._
import java.nio.file.{Files, Paths, Path, StandardCopyOption}

object PythonPostBuild {
  def processGeneratedFiles(
      log: Logger,
      openApiOutputDir: String,
      openApiPackageName: String,
      baseDir: File
  ): Unit = {

    def replaceFileOrFail(source: Path, target: Path, description: String): Unit = {
      if (Files.exists(source)) {
        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING)
        log.info(s"Replaced generated ${target.getFileName} with $description.")
      } else {
        sys.error(s"Required $description not found! Expected at: ${source.toString}")
      }
    }

    def removeFile(source: Path, description: String): Unit = {
      if (Files.exists(source)) {
        Files.delete(source)
        log.info(s"Removed $description at ${source}")
      } else {
        sys.error(s"File $description at ${source} is not present. Please verify the path.")
      }
    }

    val packageNameParts = openApiPackageName.split("\\.")
    val generatedInitPath: Path = Paths.get(
      openApiOutputDir,
      packageNameParts.headOption.getOrElse(""),
      "__init__.py"
    )
    removeFile(generatedInitPath, "root __init__.py file")

    val generatedGitPushPath: Path = Paths.get(openApiOutputDir, "git_push.sh")
    removeFile(generatedGitPushPath, "git push script")

    // Compute paths to the custom files
    val buildDir = baseDir.toPath.resolve("build")

    val clientPyproject: Path = buildDir.resolve("pyproject.toml")
    val projectReadme: Path = buildDir.resolve("README.md")
    val clientSetupPy: Path = buildDir.resolve("setup.py")

    // Paths to the generated files
    val generatedPyproject: Path = Paths.get(openApiOutputDir, "pyproject.toml")
    val generatedReadme: Path = Paths.get(openApiOutputDir, "README.md")
    val generatedSetupPy: Path = Paths.get(openApiOutputDir, "setup.py")

    // Replace files
    replaceFileOrFail(clientPyproject, generatedPyproject, "distributable pyproject.toml")
    replaceFileOrFail(projectReadme, generatedReadme, "distributable README.md")
    replaceFileOrFail(clientSetupPy, generatedSetupPy, "distributable setup.py")
  }
}
