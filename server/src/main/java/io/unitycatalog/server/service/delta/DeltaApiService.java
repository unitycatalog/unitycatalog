package io.unitycatalog.server.service.delta;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.EXTERNAL_LOCATION;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Head;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.ProducesJson;
import io.unitycatalog.server.auth.AuthorizeExpressions;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.delta.model.DeltaCatalogConfig;
import io.unitycatalog.server.delta.model.DeltaCreateStagingTableRequest;
import io.unitycatalog.server.delta.model.DeltaCreateTableRequest;
import io.unitycatalog.server.delta.model.DeltaCredentialOperation;
import io.unitycatalog.server.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.server.delta.model.DeltaLoadTableResponse;
import io.unitycatalog.server.delta.model.DeltaStagingTableResponse;
import io.unitycatalog.server.delta.model.DeltaTableType;
import io.unitycatalog.server.delta.model.DeltaUpdateTableRequest;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.DeltaApiExceptionHandler;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateStagingTable;
import io.unitycatalog.server.model.StagingTableInfo;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.StagingTableRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.service.AuthorizedService;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * UC Delta API service -- REST API for Delta tables.
 *
 * <p>Enables Delta clients (e.g., Delta Spark, Delta Kernel) to create, read, write, and manage
 * managed and external Delta tables.
 */
@ExceptionHandler(DeltaApiExceptionHandler.class)
public class DeltaApiService extends AuthorizedService {

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
  private final SchemaRepository schemaRepository;
  private final TableRepository tableRepository;
  private final StagingTableRepository stagingTableRepository;
  private final StorageCredentialVendor storageCredentialVendor;

  public DeltaApiService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      ServerProperties serverProperties,
      StorageCredentialVendor storageCredentialVendor) {
    super(authorizer, repositories, serverProperties);
    this.catalogRepository = repositories.getCatalogRepository();
    this.schemaRepository = repositories.getSchemaRepository();
    this.tableRepository = repositories.getTableRepository();
    this.stagingTableRepository = repositories.getStagingTableRepository();
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
  public DeltaCatalogConfig getConfig(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("protocol-versions") String protocolVersions) {
    if (catalog == null || catalog.isEmpty()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Must supply a proper catalog in catalog parameter.");
    }

    // Verify catalog exists
    catalogRepository.getCatalog(catalog);

    // For now, we only have 1.0 as the first protocol version. Input protocolVersions is ignored.
    return new DeltaCatalogConfig().endpoints(ENDPOINTS).protocolVersion("1.0");
  }

  // ==================== Load Table API ====================

  @Get("/delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}")
  @ProducesJson
  @AuthorizeExpression(AuthorizeExpressions.GET_TABLE)
  @AuthorizeResourceKey(METASTORE)
  public DeltaLoadTableResponse loadTable(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("schema") @AuthorizeResourceKey(SCHEMA) String schema,
      @Param("table") @AuthorizeResourceKey(TABLE) String table) {
    return tableRepository.loadTableForDelta(catalog, schema, table);
  }

  @Head("/delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}")
  @AuthorizeExpression(AuthorizeExpressions.GET_TABLE)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse tableExists(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("schema") @AuthorizeResourceKey(SCHEMA) String schema,
      @Param("table") @AuthorizeResourceKey(TABLE) String table) {
    tableRepository.findTableOrThrow(catalog, schema, table);
    return HttpResponse.of(HttpStatus.NO_CONTENT);
  }

  // ==================== Delete Table API ====================

  /**
   * Delete a table by three-part name. Mirrors {@link
   * io.unitycatalog.server.service.TableService#deleteTable}, but returns 204 No Content per {@code
   * delta.yaml} (the UC counterpart returns 200). {@code deleteTable} returns the deleted table's
   * DAO, so the table and schema UUIDs for the authorization cleanup come from the same lookup that
   * removed the table.
   */
  @Delete("/delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}")
  @AuthorizeExpression(AuthorizeExpressions.DELETE_TABLE)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse deleteTable(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("schema") @AuthorizeResourceKey(SCHEMA) String schema,
      @Param("table") @AuthorizeResourceKey(TABLE) String table) {
    TableInfoDAO deleted = tableRepository.deleteTable(catalog, schema, table);
    removeHierarchicalAuthorizations(deleted.getId().toString(), deleted.getSchemaId().toString());
    return HttpResponse.of(HttpStatus.NO_CONTENT);
  }

  // ==================== Staging Table API ====================

  /**
   * Create a staging table for a soon-to-be-created UC catalog-managed Delta table. UC allocates a
   * UUID and a managed storage location; the client then writes the initial Delta commit to that
   * location and calls {@code POST /tables} to finalize the table.
   *
   * <p>The response carries the protocol / properties the client must write in the initial commit
   * for the resulting table to be a valid UC catalog-managed Delta table, plus temporary {@code
   * READ_WRITE} credentials scoped to the staging location.
   */
  @Post("/delta/v1/catalogs/{catalog}/schemas/{schema}/staging-tables")
  @ProducesJson
  @AuthorizeExpression(AuthorizeExpressions.CREATE_STAGING_TABLE)
  public DeltaStagingTableResponse createStagingTable(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("schema") @AuthorizeResourceKey(SCHEMA) String schema,
      DeltaCreateStagingTableRequest request) {
    if (request == null || request.getName() == null || request.getName().isBlank()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Staging table name is required.");
    }
    StagingTableInfo staging =
        stagingTableRepository.createStagingTable(
            new CreateStagingTable()
                .catalogName(catalog)
                .schemaName(schema)
                .name(request.getName()));
    initializeHierarchicalAuthorization(
        staging.getId(), schemaRepository.getSchemaIdOrThrow(catalog, schema).toString());

    NormalizedURL stagingLocation = NormalizedURL.from(staging.getStagingLocation());
    TemporaryCredentials credentials =
        storageCredentialVendor.vendCredential(
            stagingLocation,
            Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE));
    return DeltaStagingTableMapper.toStagingTableResponse(staging, credentials);
  }

  // ==================== Create Table API ====================

  /**
   * Create a Delta table. Both MANAGED and EXTERNAL are supported (at feature parity with the UC
   * {@code TableService.createTable}). For MANAGED, the caller must have previously called {@code
   * POST /staging-tables}, written the initial Delta commit at the returned staging location, and
   * passes that same location back here. For EXTERNAL, the caller supplies any storage location
   * they have rights on.
   *
   * <p>OWNER is the createTable-caller in both branches, matching UC REST {@code
   * TableService.createTable}. EXTERNAL wires it via the {@code
   * initializeHierarchicalAuthorization} call below. MANAGED reuses the staging-table UUID's auth
   * row (already wired by {@code createStagingTable} under the staging-creator); {@code
   * commitStagingTable} additionally enforces {@code callerId == staging.createdBy}, so a
   * different principal cannot finalize someone else's staging — the staging-creator and the
   * createTable-caller are always the same identity. The cross-principal rejection is pinned by
   * {@code SdkStagingTableAccessControlTest#testManagedTableCreationByDifferentUserShouldFail}.
   */
  @Post("/delta/v1/catalogs/{catalog}/schemas/{schema}/tables")
  @ProducesJson
  @AuthorizeExpression(AuthorizeExpressions.CREATE_TABLE)
  public DeltaLoadTableResponse createTable(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("schema") @AuthorizeResourceKey(SCHEMA) String schema,
      @AuthorizeResourceKey(value = EXTERNAL_LOCATION, key = "location")
          @AuthorizeKey(key = "table-type")
          DeltaCreateTableRequest request) {
    DeltaCreateTableMapper.Result mapped =
        DeltaCreateTableMapper.toCreateTable(catalog, schema, request, serverProperties);
    DeltaLoadTableResponse response = tableRepository.createTableForDelta(
        mapped.createTable(), mapped.uniformIcebergFields());
    // Wire the new table into the auth hierarchy under its schema (mirrors
    // TableService.createTable). MANAGED tables reuse the staging-table UUID, whose auth row
    // was already created in createStagingTable, so re-init is unnecessary there.
    if (request.getTableType() == DeltaTableType.EXTERNAL) {
      initializeHierarchicalAuthorization(
          response.getMetadata().getTableUuid().toString(),
          schemaRepository.getSchemaIdOrThrow(catalog, schema).toString());
    }
    return response;
  }

  // ==================== Update Table API ====================

  /**
   * Apply a list of updates (and optional pre-conditions) to an existing Delta table. Covers both
   * pure metadata edits -- set-properties / remove-properties, set-protocol, set-columns, set-
   * partition-columns, set-table-comment, set-domain-metadata / remove-domain-metadata -- and the
   * CCv2 commit flow via add-commit (+ optional uniform for UniForm tables) and
   * set-latest-backfilled-version. External-table-only post-commit-hook updates go through
   * update-metadata-snapshot-version. Authorization mirrors the UC REST commit endpoint
   * ({@link io.unitycatalog.server.service.DeltaCommitsService#postCommit}) so a caller's
   * privileges don't vary by URL.
   */
  @Post("/delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}")
  @ProducesJson
  @AuthorizeExpression(AuthorizeExpressions.UPDATE_TABLE)
  public DeltaLoadTableResponse updateTable(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("schema") @AuthorizeResourceKey(SCHEMA) String schema,
      @Param("table") @AuthorizeResourceKey(TABLE) String table,
      DeltaUpdateTableRequest request) {
    return tableRepository.updateTableForDelta(catalog, schema, table, request);
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
  public DeltaCredentialsResponse getTableCredentials(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("schema") @AuthorizeResourceKey(SCHEMA) String schema,
      @Param("table") @AuthorizeResourceKey(TABLE) String table,
      @Param("operation") @AuthorizeKey DeltaCredentialOperation operation) {
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
  public DeltaCredentialsResponse getStagingTableCredentials(
      @Param("table_id") @AuthorizeResourceKey(TABLE) String tableId) {
    NormalizedURL storageLocation =
        tableRepository.getStagingTableStorageLocation(UUID.fromString(tableId));
    TemporaryCredentials credentials =
        storageCredentialVendor.vendCredential(
            storageLocation,
            Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE));
    return DeltaCredentialsMapper.toCredentialsResponse(
        storageLocation.toString(), credentials, DeltaCredentialOperation.READ_WRITE);
  }

  private static Set<CredentialContext.Privilege> toPrivileges(DeltaCredentialOperation operation) {
    return switch (operation) {
      case READ -> Set.of(CredentialContext.Privilege.SELECT);
      case READ_WRITE ->
          Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE);
    };
  }
}
