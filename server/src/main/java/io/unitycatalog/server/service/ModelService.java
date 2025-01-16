package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeKeys;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.*;
import io.unitycatalog.server.persist.model.Privileges;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import lombok.SneakyThrows;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.REGISTERED_MODEL;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;

@ExceptionHandler(GlobalExceptionHandler.class)
public class ModelService {

  private final ModelRepository modelRepository;
  private final SchemaRepository schemaRepository;
  private final CatalogRepository catalogRepository;
  private final MetastoreRepository metastoreRepository;
  private final UserRepository userRepository;

  private final UnityCatalogAuthorizer authorizer;
  private final UnityAccessEvaluator evaluator;

  @SneakyThrows
  public ModelService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    this.authorizer = authorizer;
    this.evaluator = new UnityAccessEvaluator(authorizer);
    this.catalogRepository = repositories.getCatalogRepository();
    this.schemaRepository = repositories.getSchemaRepository();
    this.modelRepository = repositories.getModelRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
    this.userRepository = repositories.getUserRepository();
  }

  @Post("")
  @AuthorizeExpression("""
          (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) && #authorize(#principal, #schema, OWNER)) ||
          (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) && #authorizeAll(#principal, #schema, USE_SCHEMA, CREATE_MODEL)) ||
          (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) && #authorizeAll(#principal, #schema, USE_SCHEMA, CREATE_FUNCTION))
          """)
  public HttpResponse createRegisteredModel(
          @AuthorizeKeys({
                  @AuthorizeKey(value = SCHEMA, key = "schema_name"),
                  @AuthorizeKey(value = CATALOG, key = "catalog_name")
          })
          CreateRegisteredModel createRegisteredModel) {
    assert createRegisteredModel != null;
    RegisteredModelInfo createRegisteredModelResponse =
        modelRepository.createRegisteredModel(createRegisteredModel);
    initializeAuthorizations(createRegisteredModelResponse);
    return HttpResponse.ofJson(createRegisteredModelResponse);
  }

  @Get("")
  @AuthorizeExpression("#defer")
  public HttpResponse listRegisteredModels(
      @Param("catalog_name") Optional<String> catalogName,
      @Param("schema_name") Optional<String> schemaName,
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken) {

    ListRegisteredModelsResponse listRegisteredModelsResponse =
            modelRepository.listRegisteredModels(catalogName, schemaName, maxResults, pageToken);
    filterModels("""
            #authorize(#principal, #metastore, OWNER) ||
            #authorize(#principal, #catalog, OWNER) ||
            (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, OWNER)) ||
            (#authorizeAny(#principal, #registered_model, OWNER, EXECUTE) && #authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG))
            """, listRegisteredModelsResponse.getRegisteredModels());

    return HttpResponse.ofJson(listRegisteredModelsResponse);
  }

  @Get("/{full_name}")
  @AuthorizeExpression("""
          #authorize(#principal, #metastore, OWNER) ||
          #authorize(#principal, #catalog, OWNER) ||
          (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, OWNER)) ||
          (#authorizeAny(#principal, #registered_model, OWNER, EXECUTE) && #authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG))
          """)
  @AuthorizeKey(METASTORE)
  public HttpResponse getRegisteredModel(@Param("full_name") @AuthorizeKey(REGISTERED_MODEL) String fullNameArg) {
    assert fullNameArg != null;
    RegisteredModelInfo registeredModelInfo = modelRepository.getRegisteredModel(fullNameArg);
    return HttpResponse.ofJson(registeredModelInfo);
  }

  @Patch("/{full_name}")
  @AuthorizeExpression("""
          (#authorize(#principal, #registered_model, OWNER) && #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) && #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG))
          """)
  @AuthorizeKey(METASTORE)
  public HttpResponse updateRegisteredModel(@Param("full_name") @AuthorizeKey(REGISTERED_MODEL) String fullName, UpdateRegisteredModel updateRegisteredModel) {
    assert updateRegisteredModel != null;
    RegisteredModelInfo updateRegisteredModelResponse =
        modelRepository.updateRegisteredModel(fullName, updateRegisteredModel);
    return HttpResponse.ofJson(updateRegisteredModelResponse);
  }

  @Delete("/{full_name}")
  @AuthorizeExpression("""
          #authorize(#principal, #metastore, OWNER) ||
          #authorize(#principal, #catalog, OWNER) ||
          (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, OWNER)) ||
          (#authorize(#principal, #registered_model, OWNER) && #authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG))
          """)
  @AuthorizeKey(METASTORE)
  public HttpResponse deleteRegisteredModel(
      @Param("full_name") @AuthorizeKey(REGISTERED_MODEL) String fullName, @Param("force") Optional<Boolean> force) {
    RegisteredModelInfo registeredModelInfo = modelRepository.getRegisteredModel(fullName);
    modelRepository.deleteRegisteredModel(fullName, force.orElse(false));
    removeAuthorizations(registeredModelInfo);
    return HttpResponse.of(HttpStatus.OK);
  }

  @Post("/versions")
  @AuthorizeExpression("""
          (#authorize(#principal, #registered_model, OWNER) && #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) && #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG))
          """)
  public HttpResponse createModelVersion(@AuthorizeKeys({
          @AuthorizeKey(value = CATALOG, key = "catalog_name"),
          @AuthorizeKey(value = SCHEMA, key = "schema_name"),
          @AuthorizeKey(value = REGISTERED_MODEL, key = "model_name")})
          CreateModelVersion createModelVersion) {
    assert createModelVersion != null;
    assert createModelVersion.getModelName() != null;
    assert createModelVersion.getCatalogName() != null;
    assert createModelVersion.getSchemaName() != null;
    assert createModelVersion.getSource() != null;
    ModelVersionInfo createModelVersionResponse =
        modelRepository.createModelVersion(createModelVersion);
    return HttpResponse.ofJson(createModelVersionResponse);
  }

  @Get("/{full_name}/versions")
  @AuthorizeExpression("""
          #authorize(#principal, #metastore, OWNER) ||
          #authorize(#principal, #catalog, OWNER) ||
          (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, OWNER)) ||
          (#authorizeAny(#principal, #registered_model, OWNER, EXECUTE) && #authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG))
          """)
  @AuthorizeKey(METASTORE)
  public HttpResponse listModelVersions(
      @Param("full_name") @AuthorizeKey(REGISTERED_MODEL) String fullName,
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken) {
    return HttpResponse.ofJson(modelRepository.listModelVersions(fullName, maxResults, pageToken));
  }

  @Get("/{full_name}/versions/{version}")
  @AuthorizeExpression("""
          #authorize(#principal, #metastore, OWNER) ||
          #authorize(#principal, #catalog, OWNER) ||
          (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, OWNER)) ||
          (#authorizeAny(#principal, #registered_model, OWNER, EXECUTE) && #authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG))
          """)
  @AuthorizeKey(METASTORE)
  public HttpResponse getModelVersion(
      @Param("full_name") @AuthorizeKey(REGISTERED_MODEL) String fullName, @Param("version") Long version) {
    assert fullName != null && version != null;
    ModelVersionInfo modelVersionInfo = modelRepository.getModelVersion(fullName, version);
    return HttpResponse.ofJson(modelVersionInfo);
  }

  @Patch("/{full_name}/versions/{version}")
  @AuthorizeExpression("""
          (#authorize(#principal, #registered_model, OWNER) && #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) && #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG))
          """)
  @AuthorizeKey(METASTORE)
  public HttpResponse updateModelVersion(@Param("full_name") @AuthorizeKey(REGISTERED_MODEL) String fullName, @Param("version") Long version,
                                         UpdateModelVersion updateModelVersion) {
    assert updateModelVersion != null;
    ModelVersionInfo updateModelVersionResponse =
        modelRepository.updateModelVersion(fullName, version, updateModelVersion);
    return HttpResponse.ofJson(updateModelVersionResponse);
  }

  @Delete("/{full_name}/versions/{version}")
  @AuthorizeExpression("""
          #authorize(#principal, #metastore, OWNER) ||
          #authorize(#principal, #catalog, OWNER) ||
          (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, OWNER)) ||
          (#authorize(#principal, #registered_model, OWNER) && #authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG))
          """)
  @AuthorizeKey(METASTORE)
  public HttpResponse deleteModelVersion(
      @Param("full_name") @AuthorizeKey(REGISTERED_MODEL) String fullName, @Param("version") Long version) {
    modelRepository.deleteModelVersion(fullName, version);
    return HttpResponse.of(HttpStatus.OK);
  }

  @Patch("/{full_name}/versions/{version}/finalize")
  @AuthorizeExpression("""
          (#authorize(#principal, #registered_model, OWNER) && #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) && #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG))
          """)
  @AuthorizeKey(METASTORE)
  public HttpResponse finalizeModelVersion(@Param("full_name") @AuthorizeKey(REGISTERED_MODEL) String fullName,
                                           FinalizeModelVersion finalizeModelVersion) {
    assert finalizeModelVersion != null;
    ModelVersionInfo finalizeModelVersionResponse =
        modelRepository.finalizeModelVersion(finalizeModelVersion);
    return HttpResponse.ofJson(finalizeModelVersionResponse);
  }

  private void initializeAuthorizations(RegisteredModelInfo registeredModelInfo) {
    SchemaInfo schemaInfo =
        schemaRepository.getSchema(
            registeredModelInfo.getCatalogName() + "." + registeredModelInfo.getSchemaName());
    UUID principalId = userRepository.findPrincipalId();
    // add owner privilege
    authorizer.grantAuthorization(
        principalId, UUID.fromString(registeredModelInfo.getId()), Privileges.OWNER);
    // make table a child of the schema
    authorizer.addHierarchyChild(
        UUID.fromString(schemaInfo.getSchemaId()), UUID.fromString(registeredModelInfo.getId()));
  }

  public void filterModels(String expression, List<RegisteredModelInfo> entries) {
    // TODO: would be nice to move this to filtering in the Decorator response
    UUID principalId = userRepository.findPrincipalId();

    evaluator.filter(
            principalId,
            expression,
            entries,
            ti -> {
              CatalogInfo catalogInfo = catalogRepository.getCatalog(ti.getCatalogName());
              SchemaInfo schemaInfo =
                      schemaRepository.getSchema(ti.getCatalogName() + "." + ti.getSchemaName());
              return Map.of(
                      METASTORE,
                      metastoreRepository.getMetastoreId(),
                      CATALOG,
                      UUID.fromString(catalogInfo.getId()),
                      SCHEMA,
                      UUID.fromString(schemaInfo.getSchemaId()),
                      REGISTERED_MODEL,
                      UUID.fromString(ti.getId()));
            });
  }

  private void removeAuthorizations(RegisteredModelInfo registeredModelInfo) {
    SchemaInfo schemaInfo =
        schemaRepository.getSchema(
            registeredModelInfo.getCatalogName() + "." + registeredModelInfo.getSchemaName());
    // remove any direct authorizations on the table
    authorizer.clearAuthorizationsForResource(UUID.fromString(registeredModelInfo.getId()));
    // remove link to the parent schema
    authorizer.removeHierarchyChild(
        UUID.fromString(schemaInfo.getSchemaId()), UUID.fromString(registeredModelInfo.getId()));
  }

}
