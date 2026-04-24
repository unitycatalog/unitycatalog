package io.unitycatalog.server.service.delta;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;

import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.ProducesJson;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.delta.model.CatalogConfig;
import io.unitycatalog.server.delta.model.LoadTableResponse;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.DeltaRestExceptionHandler;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.service.AuthorizedService;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.utils.ServerProperties;

import java.util.List;

/**
 * Delta REST Catalog Service -- REST API for Delta tables.
 *
 * <p>Enables Delta clients (e.g., Delta Spark, Delta Kernel) to create, read, write, and manage
 * managed and external Delta tables.
 */
@ExceptionHandler(DeltaRestExceptionHandler.class)
public class DeltaRestCatalogService extends AuthorizedService {

  private static final List<String> ENDPOINTS =
      List.of(
          "POST /v1/catalogs/{catalog}/schemas/{schema}/staging-tables",
          "POST /v1/catalogs/{catalog}/schemas/{schema}/tables",
          "GET /v1/catalogs/{catalog}/schemas/{schema}/tables",
          "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
          "POST /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
          "DELETE /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
          "HEAD /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
          "POST /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/rename",
          "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials",
          "POST /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/metrics",
          "GET /v1/staging-tables/{table_id}/credentials",
          "GET /v1/temporary-path-credentials");

  private final CatalogRepository catalogRepository;
  private final TableRepository tableRepository;

  public DeltaRestCatalogService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      ServerProperties serverProperties,
      StorageCredentialVendor storageCredentialVendor) {
    super(authorizer, repositories, serverProperties);
    this.catalogRepository = repositories.getCatalogRepository();
    this.tableRepository = repositories.getTableRepository();
  }

  // ==================== Configuration API ====================

  @Get("/delta/v1/config")
  @ProducesJson
  @AuthorizeExpression(
      """
      #authorize(#principal, #metastore, OWNER) ||
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
      """)
  @AuthorizeResourceKey(METASTORE)
  public CatalogConfig getConfig(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("protocol-versions") String protocolVersions) {
    if (catalog == null || catalog.isEmpty()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Must supply a proper catalog in catalog parameter.");
    }

    // Verify catalog exists
    catalogRepository.getCatalog(catalog);

    // For now, we only have 1.0 as the first protocol version. Input protocolVersions is ignored.
    return new CatalogConfig().endpoints(ENDPOINTS).protocolVersion("1.0");
  }

  // ==================== Load Table API ====================

  @Get("/delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}")
  @ProducesJson
  @AuthorizeExpression(
      """
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
      (#authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #catalog, USE_CATALOG) &&
          #authorizeAny(#principal, #table, OWNER, SELECT, MODIFY))
      """)
  @AuthorizeResourceKey(METASTORE)
  public LoadTableResponse loadTable(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("schema") @AuthorizeResourceKey(SCHEMA) String schema,
      @Param("table") @AuthorizeResourceKey(TABLE) String table) {
    return tableRepository.loadTableForDelta(catalog, schema, table);
  }
}
