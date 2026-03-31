import org.openapitools.codegen.{CodegenConstants, DefaultGenerator}
import org.openapitools.codegen.config.{CodegenConfigurator, GlobalSettings}

case class OpenApiSpec(
  inputSpec: String,
  invokerPackage: String = null,
  apiPackage: String = null,
  modelPackage: String = null,
  packageName: String = null,
  additionalProperties: Map[String, String] = Map.empty,
  globalProperties: Map[String, String] = Map.empty
)

/** Helper to run the OpenAPI generator programmatically with arbitrary config.
 *  Used to generate from multiple spec files within a single sbt project.
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
      // Suppress test and doc generation (matches OpenApiGeneratorPlugin defaults)
      GlobalSettings.setProperty(CodegenConstants.API_TESTS, "false")
      GlobalSettings.setProperty(CodegenConstants.MODEL_TESTS, "false")
      GlobalSettings.setProperty(CodegenConstants.API_DOCS, "false")
      GlobalSettings.setProperty(CodegenConstants.MODEL_DOCS, "false")
      val gen = new DefaultGenerator()
      gen.setGenerateMetadata(false)
      gen.opts(config.toClientOptInput()).generate()
    }
  }
}
