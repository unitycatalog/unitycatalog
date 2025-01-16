package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Patch;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.CreateSchema;
import io.unitycatalog.server.model.ListSchemasResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.UpdateSchema;
import io.unitycatalog.server.persist.*;
import io.unitycatalog.server.persist.model.Privileges;
import lombok.SneakyThrows;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;

@ExceptionHandler(GlobalExceptionHandler.class)
public class SchemaService {
  private final SchemaRepository schemaRepository;
  private final CatalogRepository catalogRepository;
  private final MetastoreRepository metastoreRepository;
  private final UserRepository userRepository;
  private final UnityCatalogAuthorizer authorizer;
  private final UnityAccessEvaluator evaluator;

  @SneakyThrows
  public SchemaService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    this.authorizer = authorizer;
    this.evaluator = new UnityAccessEvaluator(authorizer);
    this.schemaRepository = repositories.getSchemaRepository();
    this.catalogRepository = repositories.getCatalogRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
    this.userRepository = repositories.getUserRepository();
  }

  @Post("")
  @AuthorizeExpression("""
      #authorize(#principal, #catalog, OWNER) ||
      #authorizeAll(#principal, #catalog, USE_CATALOG, CREATE_SCHEMA)
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse createSchema(
      @AuthorizeKey(value = CATALOG, key = "catalog_name") CreateSchema createSchema) {
    SchemaInfo schemaInfo = schemaRepository.createSchema(createSchema);
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
        schemaRepository.listSchemas(catalogName, maxResults, pageToken);
    filterSchemas("""
        #authorize(#principal, #metastore, OWNER) ||
        #authorize(#principal, #catalog, OWNER) ||
        (#authorize(#principal, #schema, USE_SCHEMA) && #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG))
        """,
        listSchemasResponse.getSchemas());
    return HttpResponse.ofJson(listSchemasResponse);
  }

  @Get("/{full_name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) && #authorizeAny(#principal, #catalog, USE_CATALOG))
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse getSchema(@Param("full_name") @AuthorizeKey(SCHEMA) String fullName) {
    return HttpResponse.ofJson(schemaRepository.getSchema(fullName));
  }

  @Patch("/{full_name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #schema, OWNER) ||
      #authorizeAll(#principal, #catalog, USE_CATALOG, USE_SCHEMA) ||
      (#authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG))
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse updateSchema(
      @Param("full_name") @AuthorizeKey(SCHEMA) String fullName, UpdateSchema updateSchema) {
    // TODO: This method does not adhere to the complete access control rules of the Databricks
    // Unity Catalog
    return HttpResponse.ofJson(schemaRepository.updateSchema(fullName, updateSchema));
  }

  @Delete("/{full_name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) && #authorizeAny(#principal, #catalog, USE_CATALOG))
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse deleteSchema(
      @Param("full_name") @AuthorizeKey(SCHEMA) String fullName,
      @Param("force") Optional<Boolean> force) {
    SchemaInfo schemaInfo = schemaRepository.getSchema(fullName);
    schemaRepository.deleteSchema(fullName, force.orElse(false));
    removeAuthorizations(schemaInfo);
    return HttpResponse.of(HttpStatus.OK);
  }

  public void filterSchemas(String expression, List<SchemaInfo> entries) {
    // TODO: would be nice to move this to filtering in the Decorator response
    UUID principalId = userRepository.findPrincipalId();

    evaluator.filter(
        principalId,
        expression,
        entries,
        si -> {
          CatalogInfo catalogInfo = catalogRepository.getCatalog(si.getCatalogName());
          return Map.of(
              METASTORE,
              metastoreRepository.getMetastoreId(),
              CATALOG,
              UUID.fromString(catalogInfo.getId()),
              SCHEMA,
              UUID.fromString(si.getSchemaId()));
        });
  }

  private void createAuthorizations(SchemaInfo schemaInfo) {
    CatalogInfo catalogInfo = catalogRepository.getCatalog(schemaInfo.getCatalogName());
    UUID principalId = userRepository.findPrincipalId();
    // add owner privilege
    authorizer.grantAuthorization(
        principalId, UUID.fromString(schemaInfo.getSchemaId()), Privileges.OWNER);
    // make schema a child of the catalog
    authorizer.addHierarchyChild(
        UUID.fromString(catalogInfo.getId()), UUID.fromString(schemaInfo.getSchemaId()));
  }

  private void removeAuthorizations(SchemaInfo schemaInfo) {
    CatalogInfo catalogInfo = catalogRepository.getCatalog(schemaInfo.getCatalogName());
    // remove direct authorizations on the schema
    authorizer.clearAuthorizationsForResource(UUID.fromString(schemaInfo.getSchemaId()));
    // remove any child table links
    authorizer.removeHierarchyChildren(UUID.fromString(schemaInfo.getSchemaId()));
    // remove link to parent catalog
    authorizer.removeHierarchyChild(
        UUID.fromString(catalogInfo.getId()), UUID.fromString(schemaInfo.getSchemaId()));
  }
}
