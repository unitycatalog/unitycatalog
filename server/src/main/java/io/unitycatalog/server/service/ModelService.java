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
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.ModelRepository;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.model.Privileges;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import io.unitycatalog.server.utils.IdentityUtils;
import lombok.SneakyThrows;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.REGISTERED_MODEL;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;

@ExceptionHandler(GlobalExceptionHandler.class)
public class ModelService {

  private static final ModelRepository MODEL_REPOSITORY = ModelRepository.getInstance();

  private static final SchemaRepository SCHEMA_REPOSITORY = SchemaRepository.getInstance();
  private static final CatalogRepository CATALOG_REPOSITORY = CatalogRepository.getInstance();

  private final UnityCatalogAuthorizer authorizer;
  private final UnityAccessEvaluator evaluator;

  @SneakyThrows
  public ModelService(UnityCatalogAuthorizer authorizer) {
    this.authorizer = authorizer;
    evaluator = new UnityAccessEvaluator(authorizer);
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
        MODEL_REPOSITORY.createRegisteredModel(createRegisteredModel);
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
            MODEL_REPOSITORY.listRegisteredModels(catalogName, schemaName, maxResults, pageToken);
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
    RegisteredModelInfo registeredModelInfo = MODEL_REPOSITORY.getRegisteredModel(fullNameArg);
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
        MODEL_REPOSITORY.updateRegisteredModel(fullName, updateRegisteredModel);
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
    RegisteredModelInfo registeredModelInfo = MODEL_REPOSITORY.getRegisteredModel(fullName);
    MODEL_REPOSITORY.deleteRegisteredModel(fullName, force.orElse(false));
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
        MODEL_REPOSITORY.createModelVersion(createModelVersion);
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
    return HttpResponse.ofJson(MODEL_REPOSITORY.listModelVersions(fullName, maxResults, pageToken));
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
    ModelVersionInfo modelVersionInfo = MODEL_REPOSITORY.getModelVersion(fullName, version);
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
        MODEL_REPOSITORY.updateModelVersion(fullName, version, updateModelVersion);
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
    MODEL_REPOSITORY.deleteModelVersion(fullName, version);
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
        MODEL_REPOSITORY.finalizeModelVersion(finalizeModelVersion);
    return HttpResponse.ofJson(finalizeModelVersionResponse);
  }

  private void initializeAuthorizations(RegisteredModelInfo registeredModelInfo) {
    SchemaInfo schemaInfo =
        SCHEMA_REPOSITORY.getSchema(
            registeredModelInfo.getCatalogName() + "." + registeredModelInfo.getSchemaName());
    UUID principalId = IdentityUtils.findPrincipalId();
    // add owner privilege
    authorizer.grantAuthorization(
        principalId, UUID.fromString(registeredModelInfo.getId()), Privileges.OWNER);
    // make table a child of the schema
    authorizer.addHierarchyChild(
        UUID.fromString(schemaInfo.getSchemaId()), UUID.fromString(registeredModelInfo.getId()));
  }

  public void filterModels(String expression, List<RegisteredModelInfo> entries) {
    // TODO: would be nice to move this to filtering in the Decorator response
    UUID principalId = IdentityUtils.findPrincipalId();

    evaluator.filter(
            principalId,
            expression,
            entries,
            ti -> {
              CatalogInfo catalogInfo = CATALOG_REPOSITORY.getCatalog(ti.getCatalogName());
              SchemaInfo schemaInfo =
                      SCHEMA_REPOSITORY.getSchema(ti.getCatalogName() + "." + ti.getSchemaName());
              return Map.of(
                      METASTORE,
                      MetastoreRepository.getInstance().getMetastoreId(),
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
        SCHEMA_REPOSITORY.getSchema(
            registeredModelInfo.getCatalogName() + "." + registeredModelInfo.getSchemaName());
    // remove any direct authorizations on the table
    authorizer.clearAuthorizationsForResource(UUID.fromString(registeredModelInfo.getId()));
    // remove link to the parent schema
    authorizer.removeHierarchyChild(
        UUID.fromString(schemaInfo.getSchemaId()), UUID.fromString(registeredModelInfo.getId()));
  }

}
