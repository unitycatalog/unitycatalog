package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.ResourceType.CATALOG;
import static io.unitycatalog.server.model.ResourceType.METASTORE;
import static io.unitycatalog.server.model.ResourceType.SCHEMA;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.auth.decorator.UnityAccessUtil;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.CreateSchema;
import io.unitycatalog.server.model.ListSchemasResponse;
import io.unitycatalog.server.model.Privilege;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.UpdateSchema;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.SchemaRepository;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.SneakyThrows;

@ExceptionHandler(GlobalExceptionHandler.class)
public class SchemaService {
  private static final SchemaRepository SCHEMA_REPOSITORY = SchemaRepository.getInstance();
  private static final CatalogRepository CATALOG_REPOSITORY = CatalogRepository.getInstance();
  private final UnityCatalogAuthorizer authorizer;
  private final UnityAccessEvaluator evaluator;

  @SneakyThrows
  public SchemaService(UnityCatalogAuthorizer authorizer) {
    this.authorizer = authorizer;
    evaluator = new UnityAccessEvaluator(authorizer);
  }

  @Post("")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, METASTORE_ADMIN) ||
      #authorizeAll(#principal, #catalog, USE_CATALOG, CREATE_SCHEMA)
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse createSchema(
      @AuthorizeKey(value = CATALOG, key = "catalog_name") CreateSchema createSchema) {
    SchemaInfo schemaInfo = SCHEMA_REPOSITORY.createSchema(createSchema);
    createAuthorizations(schemaInfo);
    return HttpResponse.ofJson(schemaInfo);
  }

  @Get("")
  @AuthorizeExpression("#defer")
  public HttpResponse listSchemas(
      @Param("catalog_name") String catalogName,
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken) {
    ListSchemasResponse listSchemasResponse =
        SCHEMA_REPOSITORY.listSchemas(catalogName, maxResults, pageToken);
    filterSchemas("""
        #authorize(#principal, #metastore, METASTORE_ADMIN) ||
        #authorize(#principal, #catalog, OWNER) ||
        (#authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG)) 
        """,
        listSchemasResponse.getSchemas());
    return HttpResponse.ofJson(listSchemasResponse);
  }

  @Get("/{full_name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, METASTORE_ADMIN) ||
      #authorize(#principal, #schema, OWNER) ||
      (#authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG))
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse getSchema(@Param("full_name") @AuthorizeKey(SCHEMA) String fullName) {
    return HttpResponse.ofJson(SCHEMA_REPOSITORY.getSchema(fullName));
  }

  @Patch("/{full_name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, METASTORE_ADMIN) ||
      #authorize(#principal, #schema, OWNER) ||
      #authorizeAll(#principal, #catalog, USE_CATALOG, USE_SCHEMA) ||
      (#authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG))
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse updateSchema(
      @Param("full_name") @AuthorizeKey(SCHEMA) String fullName, UpdateSchema updateSchema) {
    // TODO: This method does not adhere to the complete access control rules of the Databricks
    // Unity Catalog
    return HttpResponse.ofJson(SCHEMA_REPOSITORY.updateSchema(fullName, updateSchema));
  }

  @Delete("/{full_name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, METASTORE_ADMIN) ||
      #authorize(#principal, #schema, OWNER) ||
      (#authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG))
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse deleteSchema(
      @Param("full_name") @AuthorizeKey(SCHEMA) String fullName,
      @Param("force") Optional<Boolean> force) {
    SchemaInfo schemaInfo = SCHEMA_REPOSITORY.getSchema(fullName);
    SCHEMA_REPOSITORY.deleteSchema(fullName, force.orElse(false));
    removeAuthorizations(schemaInfo);
    return HttpResponse.of(HttpStatus.OK);
  }

  public void filterSchemas(String expression, List<SchemaInfo> entries) {
    // TODO: would be nice to move this to filtering in the Decorator response
    UUID principalId = UnityAccessUtil.findPrincipalId();

    evaluator.filter(
        principalId,
        expression,
        entries,
        si -> {
          CatalogInfo catalogInfo = CATALOG_REPOSITORY.getCatalog(si.getCatalogName());
          return Map.of(
              METASTORE,
              MetastoreRepository.getInstance().getMetastoreId(),
              CATALOG,
              UUID.fromString(catalogInfo.getId()),
              SCHEMA,
              UUID.fromString(si.getSchemaId()));
        });
  }

  private void createAuthorizations(SchemaInfo schemaInfo) {
    CatalogInfo catalogInfo = CATALOG_REPOSITORY.getCatalog(schemaInfo.getCatalogName());
    UUID principalId = UnityAccessUtil.findPrincipalId();
    // add owner privilege
    authorizer.grantAuthorization(
        principalId, UUID.fromString(schemaInfo.getSchemaId()), Privilege.OWNER);
    // make schema a child of the catalog
    authorizer.addHierarchyChild(
        UUID.fromString(catalogInfo.getId()), UUID.fromString(schemaInfo.getSchemaId()));
  }

  private void removeAuthorizations(SchemaInfo schemaInfo) {
    CatalogInfo catalogInfo = CATALOG_REPOSITORY.getCatalog(schemaInfo.getCatalogName());
    // remove direct authorizations on the schema
    authorizer.clearAuthorizationsForResource(UUID.fromString(schemaInfo.getSchemaId()));
    // remove any child table links
    authorizer.removeHierarchyChildren(UUID.fromString(schemaInfo.getSchemaId()));
    // remove link to parent catalog
    authorizer.removeHierarchyChild(
        UUID.fromString(catalogInfo.getId()), UUID.fromString(schemaInfo.getSchemaId()));
  }
}
