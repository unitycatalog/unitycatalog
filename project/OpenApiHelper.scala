import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.media.Schema
import org.openapitools.codegen.{CodegenConstants, DefaultGenerator}
import org.openapitools.codegen.config.CodegenConfigurator
import scala.collection.JavaConverters._

case class OpenApiSpec(
  inputSpec: String,
  invokerPackage: String = null,
  apiPackage: String = null,
  modelPackage: String = null,
  packageName: String = null,
  templateDir: String = null,
  additionalProperties: Map[String, String] = Map.empty,
  globalProperties: Map[String, String] = Map.empty
)

/**
 * Runs the OpenAPI generator programmatically, supporting multiple spec files
 * per sbt project. This calls the same CodegenConfigurator + DefaultGenerator
 * API that the official sbt-openapi-generator plugin uses internally:
 * https://github.com/OpenAPITools/sbt-openapi-generator/blob/v7.27.0/src/main/scala/org/openapitools/generator/sbt/plugin/tasks/OpenApiGenerateTask.scala
 *
 * The plugin's openApiInputSpec is a single SettingKey[String], so it only
 * accepts one spec file per sbt project. To generate from both all.yaml and
 * delta.yaml into the same output directory (so models share the same
 * classpath), we call the generator in a loop over multiple OpenApiSpec
 * entries.
 *
 * Differences from the plugin:
 * - Added: loop over multiple specs, packageName parameter (for Python),
 *   optional templateDir (for per-spec template overrides),
 *   per-instance suppression of test/metadata generation via
 *   setGeneratorPropertyDefault (equivalent to the plugin's
 *   openApiGenerateModelTests := SettingDisabled).
 * - Removed: ~30 config knobs not needed here (verbose, validateSpec,
 *   skipOverwrite, auth, gitHost, importMappings,
 *   typeMappings, etc.). These all have sensible defaults in
 *   CodegenConfigurator.
 * - Every run moves a property that only adds a description to a `$ref` into
 *   a model of its own (see keepDescribedRefsAsModels). The Java runs also
 *   drop `const` from discriminator properties of subtypes (see
 *   keepDiscriminatorsAsStrings) and `@JsonFormat(shape=OBJECT)` from models
 *   that extend HashMap (see keepMapModelsAsMaps). Apart from that, the
 *   output is identical to what the plugin would produce with the same inputs.
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
      if (spec.templateDir != null) config.setTemplateDir(spec.templateDir)
      spec.additionalProperties.foreach { case (k, v) => config.addAdditionalProperty(k, v) }
      spec.globalProperties.foreach { case (k, v) => config.addGlobalProperty(k, v) }
      val gen = new DefaultGenerator()
      // Suppress test and metadata generation per-instance
      gen.setGeneratorPropertyDefault(CodegenConstants.API_TESTS, "false")
      gen.setGeneratorPropertyDefault(CodegenConstants.MODEL_TESTS, "false")
      gen.setGenerateMetadata(false)
      val input = config.toClientOptInput()
      keepDescribedRefsAsModels(input.getOpenAPI)
      if (generatorName == "java") keepDiscriminatorsAsStrings(input.getOpenAPI)
      val files = gen.opts(input).generate().asScala
      if (generatorName == "java") files.filter(_.getName.endsWith(".java")).foreach(keepMapModelsAsMaps)
    }
  }

  private val objectShape = """(?m)^@JsonFormat\(shape=JsonFormat\.Shape\.OBJECT\)\R""".r

  /**
   * The generator marks every model with additionalProperties `@JsonFormat(shape=OBJECT)`. On a model
   * that extends HashMap (DeltaStructFieldMetadata in the server models), that makes Jackson write it
   * as a bean, which adds an "empty" key from isEmpty() to every column's metadata. Without the
   * annotation it is written as the Map of its metadata keys, as before the upgrade.
   */
  private def keepMapModelsAsMaps(file: File): Unit = {
    val path = file.toPath
    val source = new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
    if (source.contains(" extends HashMap<") && objectShape.findFirstIn(source).isDefined) {
      Files.write(path, objectShape.replaceAllIn(source, "").getBytes(StandardCharsets.UTF_8))
    }
  }

  /**
   * A property written as `allOf: [$ref, {description}]` used to get a model of its own, named
   * `<Schema>_<property>`, such as DeltaStagingTableResponseRequiredProtocol. The generator now
   * types it as the referenced model instead, which breaks callers of the published clients, such
   * as Delta's Unity Catalog client. Moving the property into a component of that name keeps the
   * model, its API docs and the client's binary compatibility.
   */
  private def keepDescribedRefsAsModels(openAPI: OpenAPI): Unit = {
    val schemas = Option(openAPI.getComponents).flatMap(c => Option(c.getSchemas))
    for {
      components <- schemas.toList
      (schemaName, schema) <- components.asScala.toList
      properties <- Option(schema.getProperties).toList
      (propertyName, property) <- properties.asScala.toList
      allOf <- Option(property.getAllOf).map(_.asScala.toList)
      if allOf.count(_.get$ref != null) == 1 && allOf.size == 2
      if allOf.forall(s => s.get$ref != null || (s.getType == null && s.getProperties == null))
    } {
      val name = s"${schemaName}_${propertyName.replace('-', '_')}"
      components.put(name, property)
      properties.put(propertyName, new Schema[AnyRef]().$ref(s"#/components/schemas/$name"))
    }
  }

  /**
   * The Delta subtypes (DeltaAddCommitUpdate, DeltaAssertEtag, ...) pin their parent's discriminator
   * with `const`, which the generator's NORMALIZE_31SPEC rule turns into a one-value enum. A Java
   * subclass cannot narrow the parent's String getter to that enum, so the Java models do not compile.
   * Jackson already picks the subtype from the discriminator mapping, so dropping `const` from the
   * Java models keeps the generated code as it was before the upgrade. The Python client and the docs
   * still see the `const`.
   */
  private def keepDiscriminatorsAsStrings(openAPI: OpenAPI): Unit = {
    val schemas: collection.Map[String, Schema[_]] =
      Option(openAPI.getComponents).flatMap(c => Option(c.getSchemas)).map(_.asScala).getOrElse(Map.empty)
    for {
      schema <- schemas.values
      parentRef <- Option(schema.getAllOf).map(_.asScala).getOrElse(Nil).flatMap(s => Option(s.get$ref))
      parent <- schemas.get(parentRef.stripPrefix("#/components/schemas/"))
      discriminator <- Option(parent.getDiscriminator)
      properties <- Option(schema.getProperties)
      property <- Option[Schema[_]](properties.get(discriminator.getPropertyName))
    } property.setConst(null)
  }
}
