package io.unitycatalog.server.service.delta;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;

import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.ProducesJson;
import io.unitycatalog.server.auth.AuthorizeExpressions;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.delta.model.CatalogConfig;
import io.unitycatalog.server.delta.model.CredentialOperation;
import io.unitycatalog.server.delta.model.CredentialsResponse;
import io.unitycatalog.server.delta.model.LoadTableResponse;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.DeltaRestExceptionHandler;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.service.AuthorizedService;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.List;
import java.util.Set;
import java.util.UUID;

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
  private final StorageCredentialVendor storageCredentialVendor;

  public DeltaRestCatalogService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      StorageCredentialVendor storageCredentialVendor) {
    super(authorizer, repositories);
    this.catalogRepository = repositories.getCatalogRepository();
    this.tableRepository = repositories.getTableRepository();
    this.storageCredentialVendor = storageCredentialVendor;
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
  @AuthorizeExpression(AuthorizeExpressions.GET_TABLE)
  @AuthorizeResourceKey(METASTORE)
  public LoadTableResponse loadTable(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("schema") @AuthorizeResourceKey(SCHEMA) String schema,
      @Param("table") @AuthorizeResourceKey(TABLE) String table) {
    return tableRepository.loadTableForDelta(catalog, schema, table);
  }

  // ==================== Table Credentials API ====================

  /**
   * Vend temporary cloud storage credentials for a table identified by three-part name. The {@code
   * operation} query param scopes the returned credentials: {@code READ} requires {@code SELECT}
   * (or OWNER); {@code READ_WRITE} requires {@code OWNER} or both {@code SELECT} and {@code
   * MODIFY}.
   */
  @Get("/delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials")
  @ProducesJson
  @AuthorizeExpression(AuthorizeExpressions.VEND_TABLE_CREDENTIAL)
  public CredentialsResponse getTableCredentials(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("schema") @AuthorizeResourceKey(SCHEMA) String schema,
      @Param("table") @AuthorizeResourceKey(TABLE) String table,
      @Param("operation") @AuthorizeKey CredentialOperation operation) {
    NormalizedURL storageLocation = tableRepository.getTableStorageLocation(catalog, schema, table);
    TemporaryCredentials credentials =
        storageCredentialVendor.vendCredential(storageLocation, toPrivileges(operation));
    return DeltaCredentialsMapper.toCredentialsResponse(
        storageLocation.toString(), credentials, operation);
  }

  /**
   * Vend temporary cloud storage credentials for a staging table identified by UUID. Only the
   * staging table owner may vend credentials; the returned credentials are always READ_WRITE so the
   * client can write the initial commit.
   */
  @Get("/delta/v1/staging-tables/{table_id}/credentials")
  @ProducesJson
  @AuthorizeExpression("#authorize(#principal, #table, OWNER)")
  public CredentialsResponse getStagingTableCredentials(
      @Param("table_id") @AuthorizeResourceKey(TABLE) String tableId) {
    NormalizedURL storageLocation =
        tableRepository.getStagingTableStorageLocation(UUID.fromString(tableId));
    TemporaryCredentials credentials =
        storageCredentialVendor.vendCredential(
            storageLocation,
            Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE));
    return DeltaCredentialsMapper.toCredentialsResponse(
        storageLocation.toString(), credentials, CredentialOperation.READ_WRITE);
  }

  private static Set<CredentialContext.Privilege> toPrivileges(CredentialOperation operation) {
    return switch (operation) {
      case READ -> Set.of(CredentialContext.Privilege.SELECT);
      case READ_WRITE ->
          Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE);
    };
  }
}
