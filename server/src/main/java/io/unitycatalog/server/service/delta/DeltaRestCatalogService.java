package io.unitycatalog.server.service.delta;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;

import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.delta.model.CatalogConfig;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.DeltaRestExceptionHandler;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.service.AuthorizedService;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
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

  public DeltaRestCatalogService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      StorageCredentialVendor storageCredentialVendor) {
    super(authorizer, repositories);
    this.catalogRepository = repositories.getCatalogRepository();
  }

  // ==================== Configuration API ====================

  @Get("/delta/v1/config")
  @AuthorizeExpression(
      """
      #authorize(#principal, #metastore, OWNER) ||
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse getConfig(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("protocol-versions") String protocolVersions) {
    if (catalog == null || catalog.isEmpty()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Must supply a proper catalog in catalog parameter.");
    }

    // Verify catalog exists
    catalogRepository.getCatalog(catalog);

    // For now, we only have 1.0 as the first protocol version. Input protocolVersions is ignored.
    return HttpResponse.ofJson(
        new CatalogConfig().endpoints(ENDPOINTS).protocolVersion("1.0"));
  }
}
