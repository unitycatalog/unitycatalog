import org.openapitools.codegen.{CodegenConstants, DefaultGenerator}
import org.openapitools.codegen.config.CodegenConfigurator

case class OpenApiSpec(
  inputSpec: String,
  invokerPackage: String = null,
  apiPackage: String = null,
  modelPackage: String = null,
  packageName: String = null,
  additionalProperties: Map[String, String] = Map.empty,
  globalProperties: Map[String, String] = Map.empty
)

/**
 * Runs the OpenAPI generator programmatically, supporting multiple spec files
 * per sbt project. This calls the same CodegenConfigurator + DefaultGenerator
 * API that the official sbt-openapi-generator plugin uses internally:
 * https://github.com/OpenAPITools/sbt-openapi-generator/blob/v7.9.0/src/main/scala/org/openapitools/generator/sbt/plugin/tasks/OpenApiGenerateTask.scala
 *
 * The plugin's openApiInputSpec is a single SettingKey[String], so it only
 * accepts one spec file per sbt project. To generate from both all.yaml and
 * delta.yaml into the same output directory (so models share the same
 * classpath), we call the generator in a loop over multiple OpenApiSpec
 * entries.
 *
 * Differences from the plugin:
 * - Added: loop over multiple specs, packageName parameter (for Python),
 *   per-instance suppression of test/metadata generation via
 *   setGeneratorPropertyDefault (equivalent to the plugin's
 *   openApiGenerateModelTests := SettingDisabled).
 * - Removed: ~30 config knobs not needed here (verbose, validateSpec,
 *   skipOverwrite, templateDir, auth, gitHost, importMappings,
 *   typeMappings, etc.). These all have sensible defaults in
 *   CodegenConfigurator.
 * - No behavioral change in generated code: the output is identical to
 *   what the plugin would produce with the same inputs.
 *
 * Projects that only generate from a single spec (controlApi,
 * controlModels) still use the plugin directly.
 */
object OpenApiHelper {
  def generate(
    outputDir: String,
    specs: Seq[OpenApiSpec],
    generatorName: String = "java"
  ): Unit = {
    specs.foreach { spec =>
      val config = new CodegenConfigurator()
      config.setInputSpec(spec.inputSpec)
      config.setOutputDir(outputDir)
      config.setGeneratorName(generatorName)
      if (spec.invokerPackage != null) config.setInvokerPackage(spec.invokerPackage)
      if (spec.apiPackage != null) config.setApiPackage(spec.apiPackage)
      if (spec.modelPackage != null) config.setModelPackage(spec.modelPackage)
      if (spec.packageName != null) config.setPackageName(spec.packageName)
      spec.additionalProperties.foreach { case (k, v) => config.addAdditionalProperty(k, v) }
      spec.globalProperties.foreach { case (k, v) => config.addGlobalProperty(k, v) }
      val gen = new DefaultGenerator()
      // Suppress test and metadata generation per-instance
      gen.setGeneratorPropertyDefault(CodegenConstants.API_TESTS, "false")
      gen.setGeneratorPropertyDefault(CodegenConstants.MODEL_TESTS, "false")
      gen.setGenerateMetadata(false)
      gen.opts(config.toClientOptInput()).generate()
    }
  }
}
