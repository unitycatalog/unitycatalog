package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.EXTERNAL_LOCATION;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;

import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.CreateSchema;
import io.unitycatalog.server.model.ListSchemasResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.UpdateSchema;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Patch;
import com.linecorp.armeria.server.annotation.Post;
import lombok.SneakyThrows;

@ExceptionHandler(GlobalExceptionHandler.class)
public class SchemaService extends AuthorizedService {
  private final SchemaRepository schemaRepository;
  private final CatalogRepository catalogRepository;
  private final MetastoreRepository metastoreRepository;

  @SneakyThrows
  public SchemaService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    super(authorizer, repositories);
    this.schemaRepository = repositories.getSchemaRepository();
    this.catalogRepository = repositories.getCatalogRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
  }

  /**
   * Creating a schema requires OWNER or USE_CATALOG+CREATE_SCHEMA permission on parent catalog.
   * Additionally, if a {@code storage_root} is specified:
   *
   * <ul>
   *   <li>The path has to map to an external_location, not anything else (tables, volumes, models),
   *       to make sure the path isn't under any existing data securables.
   *   <li>User needs to have OWNER or CREATE_MANAGED_STORAGE permission on the external location.
   * </ul>
   *
   * {@code storage_root} is annotated as both a {@link AuthorizeResourceKey} (which maps to owning
   * resource) and {@link AuthorizeKey} (which is simply the raw value of storage_root). This is
   * done so that the expression can check both: 1. if parameter is specified, and 2. which external
   * location (rather than tables etc.) owns the path.
   */
  @Post("")
  @AuthorizeExpression("""
      (#authorize(#principal, #catalog, OWNER) ||
       #authorizeAll(#principal, #catalog, USE_CATALOG, CREATE_SCHEMA)) &&
      (#storage_root == null ||
       (#no_overlap_with_data_securable &&
        #external_location != null &&
        #authorizeAny(#principal, #external_location, OWNER, CREATE_MANAGED_STORAGE)))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse createSchema(
      @AuthorizeResourceKey(value = CATALOG, key = "catalog_name")
      @AuthorizeResourceKey(value = EXTERNAL_LOCATION, key = "storage_root")
      @AuthorizeKey(key = "storage_root")
      CreateSchema createSchema) {
    SchemaInfo schemaInfo = schemaRepository.createSchema(createSchema);

    CatalogInfo catalogInfo = catalogRepository.getCatalog(schemaInfo.getCatalogName());
    initializeHierarchicalAuthorization(schemaInfo.getSchemaId(), catalogInfo.getId());

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
        (#authorize(#principal, #schema, USE_SCHEMA) &&
            #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG))
        """,
        listSchemasResponse.getSchemas());
    return HttpResponse.ofJson(listSchemasResponse);
  }

  @Get("/{full_name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
          #authorizeAny(#principal, #catalog, USE_CATALOG))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse getSchema(@Param("full_name") @AuthorizeResourceKey(SCHEMA) String fullName) {
    return HttpResponse.ofJson(schemaRepository.getSchema(fullName));
  }

  @Patch("/{full_name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #schema, OWNER) ||
      #authorizeAll(#principal, #catalog, USE_CATALOG, USE_SCHEMA) ||
      (#authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #catalog, USE_CATALOG))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse updateSchema(
      @Param("full_name") @AuthorizeResourceKey(SCHEMA) String fullName,
      UpdateSchema updateSchema) {
    // TODO: This method does not adhere to the complete access control rules of the Databricks
    // Unity Catalog
    return HttpResponse.ofJson(schemaRepository.updateSchema(fullName, updateSchema));
  }

  @Delete("/{full_name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) &&
          #authorizeAny(#principal, #catalog, USE_CATALOG))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse deleteSchema(
      @Param("full_name") @AuthorizeResourceKey(SCHEMA) String fullName,
      @Param("force") Optional<Boolean> force) {
    SchemaInfo schemaInfo = schemaRepository.getSchema(fullName);
    schemaRepository.deleteSchema(fullName, force.orElse(false));

    CatalogInfo catalogInfo = catalogRepository.getCatalog(schemaInfo.getCatalogName());

    // First remove any child table links
    authorizer.removeHierarchyChildren(UUID.fromString(schemaInfo.getSchemaId()));
    // Then remove schema from catalog and clear authorizations
    removeHierarchicalAuthorizations(schemaInfo.getSchemaId(), catalogInfo.getId());

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
}

