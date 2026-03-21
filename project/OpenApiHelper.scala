import org.openapitools.codegen.DefaultGenerator
import org.openapitools.codegen.config.CodegenConfigurator

case class OpenApiSpec(
  inputSpec: String,
  invokerPackage: String = null,
  apiPackage: String = null,
  modelPackage: String = null,
  additionalProperties: Map[String, String] = Map.empty,
  globalProperties: Map[String, String] = Map.empty
)

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
      spec.additionalProperties.foreach { case (k, v) => config.addAdditionalProperty(k, v) }
      spec.globalProperties.foreach { case (k, v) => config.addGlobalProperty(k, v) }
      new DefaultGenerator().opts(config.toClientOptInput()).generate()
    }
  }
}
