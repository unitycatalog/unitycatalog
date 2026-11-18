import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import sbt.Logger

/**
 * Checks PythonClientPostBuild.clientPackageInitProblem, which the build relies on to catch the delta.yaml
 * run overwriting unitycatalog/client/__init__.py. Runs as pythonClient/test, since the build code has no
 * test framework of its own.
 */
object PythonClientPostBuildTest {
  private val allYamlInit =
    """from unitycatalog.client.api.catalogs_api import CatalogsApi
      |from unitycatalog.client.api_client import ApiClient
      |from unitycatalog.client.models.catalog_info import CatalogInfo
      |""".stripMargin

  private val deltaYamlInit =
    """from delta.api.delta_tables_api import DeltaTablesApi
      |from unitycatalog.client.api_client import ApiClient
      |from delta.models.delta_protocol import DeltaProtocol
      |""".stripMargin

  def run(log: Logger, dir: Path): Unit = {
    try check(dir.resolve("__init__.py"))
    catch {
      case e: RuntimeException =>
        log.error(e.getMessage)
        throw e
    }
    log.info("PythonClientPostBuildTest passed.")
  }

  private def check(init: Path): Unit = {
    Files.deleteIfExists(init)
    expect("a missing file", PythonClientPostBuild.clientPackageInitProblem(init), Some(s"$init was not generated."))

    Files.write(init, allYamlInit.getBytes(StandardCharsets.UTF_8))
    expect("the all.yaml init", PythonClientPostBuild.clientPackageInitProblem(init), None)

    Files.write(init, deltaYamlInit.getBytes(StandardCharsets.UTF_8))
    expect(
      "the delta.yaml init",
      PythonClientPostBuild.clientPackageInitProblem(init),
      Some(
        s"$init is not the one from the all.yaml run. The all.yaml run in pythonClient/generate must run last, " +
          "after the delta.yaml run."))

    expectRunOrderProblem(init, "an empty init", "")
    expectRunOrderProblem(
      init, "an init with only the ApiClient import", "from unitycatalog.client.api_client import ApiClient\n")
    expectRunOrderProblem(
      init,
      "the all.yaml init with a bare delta import",
      allYamlInit + "from delta.api.delta_tables_api import DeltaTablesApi\n")
    expectRunOrderProblem(init, "the all.yaml init with an import delta. line", allYamlInit + "import delta.api\n")
    expectRunOrderProblem(
      init,
      "the all.yaml init with an indented bare delta import",
      allYamlInit + "    from delta.api.delta_tables_api import DeltaTablesApi\n")
  }

  private def expectRunOrderProblem(init: Path, name: String, content: String): Unit = {
    Files.write(init, content.getBytes(StandardCharsets.UTF_8))
    expect(
      name,
      PythonClientPostBuild.clientPackageInitProblem(init).map(_.startsWith(s"$init is not the one")),
      Some(true))
  }

  private def expect[T](name: String, actual: T, expected: T): Unit = {
    if (actual != expected) {
      sys.error(s"PythonClientPostBuildTest: for $name expected $expected but got $actual")
    }
  }
}
