package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.ProducesJson;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKeys;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.ColumnInfos;
import io.unitycatalog.server.model.CreateStagingTable;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.DeltaCommit;
import io.unitycatalog.server.model.DeltaCommitInfo;
import io.unitycatalog.server.model.DeltaCommitMetadataProperties;
import io.unitycatalog.server.model.DeltaGetCommits;
import io.unitycatalog.server.model.DeltaGetCommitsResponse;
import io.unitycatalog.server.model.DeltaMetadata;
import io.unitycatalog.server.model.DeltaV1ConfigResponse;
import io.unitycatalog.server.model.DeltaV1CreateStagingTableRequest;
import io.unitycatalog.server.model.DeltaV1CreateTableRequest;
import io.unitycatalog.server.model.DeltaV1LoadTableResponse;
import io.unitycatalog.server.model.DeltaV1ProtocolInfo;
import io.unitycatalog.server.model.DeltaV1StagingTableResponse;
import io.unitycatalog.server.model.DeltaV1UpdateTableRequest;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.StagingTableInfo;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.model.TableOperation;
import io.unitycatalog.server.persist.DeltaCommitRepository;
import io.unitycatalog.server.persist.PropertyRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.StagingTableRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@ExceptionHandler(GlobalExceptionHandler.class)
public class DeltaV1Service extends AuthorizedService {

  private static final List<String> ENDPOINTS =
      List.of(
          "GET /config",
          "POST /catalogs/{catalog}/schemas/{schema}/staging-tables",
          "GET /catalogs/{catalog}/schemas/{schema}/staging-tables/{table_id}/credentials",
          "POST /catalogs/{catalog}/schemas/{schema}/tables",
          "GET /catalogs/{catalog}/schemas/{schema}/tables/{table}",
          "POST /catalogs/{catalog}/schemas/{schema}/tables/{table}",
          "GET /catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials");

  private static final DeltaV1ProtocolInfo REQUIRED_PROTOCOL =
      new DeltaV1ProtocolInfo()
          .minReaderVersion(3)
          .minWriterVersion(7)
          .readerFeatures(List.of("catalogManaged"))
          .writerFeatures(List.of("catalogManaged", "vacuumProtocolCheck", "inCommitTimestamp"));

  private static final DeltaV1ProtocolInfo SUGGESTED_PROTOCOL =
      new DeltaV1ProtocolInfo()
          .minReaderVersion(3)
          .minWriterVersion(7)
          .readerFeatures(Collections.emptyList())
          .writerFeatures(List.of("v2Checkpoint"));

  private static final Map<String, String> REQUIRED_PROPERTIES =
      Map.of(
          "delta.feature.catalogManaged", "supported",
          "delta.feature.vacuumProtocolCheck", "supported");

  private final TableRepository tableRepository;
  private final StagingTableRepository stagingTableRepository;
  private final DeltaCommitRepository deltaCommitRepository;
  private final SchemaRepository schemaRepository;
  private final StorageCredentialVendor storageCredentialVendor;
  private final Repositories repositories;

  public DeltaV1Service(
      UnityCatalogAuthorizer authorizer,
      StorageCredentialVendor storageCredentialVendor,
      Repositories repositories) {
    super(authorizer, repositories);
    this.repositories = repositories;
    this.storageCredentialVendor = storageCredentialVendor;
    this.tableRepository = repositories.getTableRepository();
    this.stagingTableRepository = repositories.getStagingTableRepository();
    this.deltaCommitRepository = repositories.getDeltaCommitRepository();
    this.schemaRepository = repositories.getSchemaRepository();
  }

  @Get("/v1/config")
  @ProducesJson
  public DeltaV1ConfigResponse config() {
    return new DeltaV1ConfigResponse().endpoints(ENDPOINTS).protocolVersion(1);
  }

  @Post("/v1/catalogs/{catalog}/schemas/{schema}/staging-tables")
  @AuthorizeExpression("""
          (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
           && #authorize(#principal, #schema, OWNER)) ||
          (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
           && #authorizeAll(#principal, #schema, USE_SCHEMA, CREATE_TABLE))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse createStagingTable(
      @Param("catalog") String catalog,
      @Param("schema") String schema,
      @AuthorizeResourceKeys({
        @AuthorizeResourceKey(value = SCHEMA, key = "schema"),
        @AuthorizeResourceKey(value = CATALOG, key = "catalog")
      })
      DeltaV1CreateStagingTableRequest request) {
    if (request == null || request.getName() == null || request.getName().isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Field can not be empty: name");
    }
    StagingTableInfo created =
        stagingTableRepository.createStagingTable(
            new CreateStagingTable()
                .catalogName(catalog)
                .schemaName(schema)
                .name(request.getName()));

    SchemaInfo schemaInfo = schemaRepository.getSchema(catalog + "." + schema);
    initializeHierarchicalAuthorization(created.getId(), schemaInfo.getSchemaId());

    DeltaV1StagingTableResponse response =
        new DeltaV1StagingTableResponse()
            .tableId(created.getId())
            .location(created.getStagingLocation())
            .requiredProperties(REQUIRED_PROPERTIES)
            .suggestedProperties(Collections.emptyMap())
            .requiredProtocol(REQUIRED_PROTOCOL)
            .suggestedProtocol(SUGGESTED_PROTOCOL);
    return HttpResponse.ofJson(response);
  }

  @Get("/v1/catalogs/{catalog}/schemas/{schema}/staging-tables/{table_id}/credentials")
  @AuthorizeExpression("""
      #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
      (#operation == 'READ'
        ? #authorizeAny(#principal, #table, OWNER, SELECT)
        : (#authorize(#principal, #table, OWNER) ||
           #authorizeAll(#principal, #table, SELECT, MODIFY)))
      """)
  public HttpResponse getStagingTableCredentials(
      @Param("table_id") @AuthorizeResourceKey(value = TABLE, key = "table_id") String tableId,
      @Param("operation") Optional<String> operation) {
    return HttpResponse.ofJson(
        storageCredentialVendor.vendCredential(
            tableRepository.getStorageLocationForTableOrStagingTable(UUID.fromString(tableId)),
            toPrivileges(operation.orElse(TableOperation.READ_WRITE.name()))));
  }

  @Post("/v1/catalogs/{catalog}/schemas/{schema}/tables")
  @AuthorizeExpression("""
          (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
           && #authorize(#principal, #schema, OWNER)) ||
          (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
           && #authorizeAll(#principal, #schema, USE_SCHEMA, CREATE_TABLE))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse createTable(
      @Param("catalog") String catalog,
      @Param("schema") String schema,
      @AuthorizeResourceKeys({
        @AuthorizeResourceKey(value = SCHEMA, key = "schema"),
        @AuthorizeResourceKey(value = CATALOG, key = "catalog")
      })
      DeltaV1CreateTableRequest request) {
    if (request == null || request.getName() == null || request.getName().isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Field can not be empty: name");
    }
    CreateTable createTable =
        new CreateTable()
            .catalogName(catalog)
            .schemaName(schema)
            .name(request.getName())
            .tableType(TableType.MANAGED)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .storageLocation(request.getStorageLocation())
            .comment(request.getComment())
            .columns(request.getColumns())
            .properties(request.getProperties());
    TableInfo created = tableRepository.createTable(createTable);
    if (request.getTableId() != null && !request.getTableId().equals(created.getTableId())) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          "Created table id "
              + created.getTableId()
              + " does not match expected "
              + request.getTableId());
    }
    return HttpResponse.ofJson(buildLoadTableResponse(created));
  }

  @Get("/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}")
  @ProducesJson
  @AuthorizeExpression("""
        #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
        #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
        #authorizeAny(#principal, #table, OWNER, SELECT)
      """)
  @AuthorizeResourceKey(METASTORE)
  public DeltaV1LoadTableResponse loadTable(
      @Param("catalog") String catalog,
      @Param("schema") String schema,
      @Param("table") @AuthorizeResourceKey(TABLE) String table,
      @Param("start_version") Optional<Long> startVersion,
      @Param("end_version") Optional<Long> endVersion) {
    return buildLoadTableResponse(
        tableRepository.getTable(fullName(catalog, schema, table)), startVersion, endVersion);
  }

  @Post("/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}")
  @AuthorizeExpression("""
        #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
        #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
        #authorizeAny(#principal, #table, OWNER, MODIFY)
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse updateTable(
      @Param("catalog") String catalog,
      @Param("schema") String schema,
      @Param("table") @AuthorizeResourceKey(TABLE) String table,
      DeltaV1UpdateTableRequest request) {
    String fullName = fullName(catalog, schema, table);
    TableInfo current = tableRepository.getTable(fullName);
    DeltaV1LoadTableResponse currentState = buildLoadTableResponse(current);
    validateRequirements(currentState, request);

    boolean hasMetadataUpdate =
        (request.getSetProperties() != null && !request.getSetProperties().isEmpty())
            || (request.getRemoveProperties() != null && !request.getRemoveProperties().isEmpty())
            || request.getComment() != null
            || request.getColumns() != null;
    boolean hasCommitUpdate =
        request.getCommitInfo() != null || request.getLatestBackfilledVersion() != null;
    if (!hasMetadataUpdate && !hasCommitUpdate) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "No updates specified.");
    }

    if (request.getCommitInfo() != null || request.getLatestBackfilledVersion() != null) {
      DeltaCommit commit =
          new DeltaCommit()
              .tableId(current.getTableId())
              .tableUri(current.getStorageLocation())
              .commitInfo(request.getCommitInfo())
              .latestBackfilledVersion(request.getLatestBackfilledVersion());
      if (hasMetadataUpdate) {
        commit.metadata(buildUpdatedMetadata(current, request));
      }
      deltaCommitRepository.postCommit(commit);
    } else {
      applyMetadataOnlyUpdate(current, request);
    }
    return HttpResponse.ofJson(
        buildLoadTableResponse(tableRepository.getTable(fullName(catalog, schema, table))));
  }

  @Get("/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials")
  @AuthorizeExpression("""
      #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
      (#operation == 'READ'
        ? #authorizeAny(#principal, #table, OWNER, SELECT)
        : (#authorize(#principal, #table, OWNER) ||
           #authorizeAll(#principal, #table, SELECT, MODIFY)))
      """)
  public HttpResponse getTableCredentials(
      @Param("catalog") String catalog,
      @Param("schema") String schema,
      @Param("table") @AuthorizeResourceKey(TABLE) String table,
      @Param("operation") Optional<String> operation) {
    TableInfo tableInfo = tableRepository.getTable(fullName(catalog, schema, table));
    return HttpResponse.ofJson(
        storageCredentialVendor.vendCredential(
            NormalizedURL.from(tableInfo.getStorageLocation()),
            toPrivileges(operation.orElse(TableOperation.READ_WRITE.name()))));
  }

  private DeltaV1LoadTableResponse buildLoadTableResponse(TableInfo tableInfo) {
    return buildLoadTableResponse(tableInfo, Optional.of(0L), Optional.empty());
  }

  private DeltaV1LoadTableResponse buildLoadTableResponse(
      TableInfo tableInfo, Optional<Long> startVersion, Optional<Long> endVersion) {
    DeltaGetCommitsResponse commitsResponse =
        deltaCommitRepository.getCommits(
            new DeltaGetCommits()
                .tableId(tableInfo.getTableId())
                .startVersion(startVersion.orElse(0L))
                .endVersion(endVersion.orElse(null)));
    long latestVersion =
        commitsResponse.getLatestTableVersion() != null
            ? commitsResponse.getLatestTableVersion()
            : 0L;
    return new DeltaV1LoadTableResponse()
        .tableInfo(tableInfo)
        .etag(computeEtag(tableInfo, latestVersion))
        .latestTableVersion(latestVersion)
        .lastKnownBackfilledVersion(getLastKnownBackfilledVersion(latestVersion, commitsResponse))
        .commits(commitsResponse.getCommits())
        .protocol(REQUIRED_PROTOCOL);
  }

  private long getLastKnownBackfilledVersion(
      long latestVersion, DeltaGetCommitsResponse commitsResponse) {
    if (commitsResponse.getCommits() == null || commitsResponse.getCommits().isEmpty()) {
      return latestVersion;
    }
    long minTrackedVersion =
        commitsResponse.getCommits().stream()
            .map(DeltaCommitInfo::getVersion)
            .min(Long::compareTo)
            .orElse(latestVersion);
    return Math.max(-1L, minTrackedVersion - 1L);
  }

  private void validateRequirements(
      DeltaV1LoadTableResponse currentState, DeltaV1UpdateTableRequest request) {
    if (request.getAssertTableId() != null
        && !request.getAssertTableId().equals(currentState.getTableInfo().getTableId())) {
      throw new BaseException(ErrorCode.FAILED_PRECONDITION, "Table UUID assertion failed.");
    }
    if (request.getAssertEtag() != null
        && !request.getAssertEtag().equals(currentState.getEtag())) {
      throw new BaseException(ErrorCode.ABORTED, "Etag assertion failed.");
    }
  }

  private DeltaMetadata buildUpdatedMetadata(TableInfo current, DeltaV1UpdateTableRequest request) {
    Map<String, String> updatedProperties = new HashMap<>();
    if (current.getProperties() != null) {
      updatedProperties.putAll(current.getProperties());
    }
    if (request.getSetProperties() != null) {
      updatedProperties.putAll(request.getSetProperties());
    }
    if (request.getRemoveProperties() != null) {
      request.getRemoveProperties().forEach(updatedProperties::remove);
    }
    updatedProperties.putIfAbsent("io.unitycatalog.tableId", current.getTableId());

    List<io.unitycatalog.server.model.ColumnInfo> columns =
        request.getColumns() != null ? request.getColumns() : current.getColumns();
    return new DeltaMetadata()
        .description(request.getComment() != null ? request.getComment() : current.getComment())
        .properties(new DeltaCommitMetadataProperties().properties(updatedProperties))
        .schema(new ColumnInfos().columns(columns));
  }

  private void applyMetadataOnlyUpdate(TableInfo current, DeltaV1UpdateTableRequest request) {
    DeltaMetadata metadata = buildUpdatedMetadata(current, request);
    TransactionManager.executeWithTransaction(
        repositories.getSessionFactory(),
        session -> {
          TableInfoDAO tableInfoDAO =
              session.get(TableInfoDAO.class, UUID.fromString(current.getTableId()));
          if (tableInfoDAO == null) {
            throw new BaseException(
                ErrorCode.NOT_FOUND, "Table not found: " + current.getTableId());
          }
          PropertyRepository.findProperties(session, tableInfoDAO.getId(), Constants.TABLE)
              .forEach(session::remove);
          session.flush();
          PropertyDAO.from(
                  metadata.getProperties().getProperties(),
                  tableInfoDAO.getId(),
                  Constants.TABLE)
              .forEach(session::persist);

          List<ColumnInfoDAO> newColumns =
              ColumnInfoDAO.fromList(metadata.getSchema().getColumns());
          tableInfoDAO.getColumns().clear();
          session.flush();
          newColumns.forEach(
              c -> {
                c.setId(UUID.randomUUID());
                c.setTable(tableInfoDAO);
              });
          tableInfoDAO.getColumns().addAll(newColumns);
          tableInfoDAO.setComment(metadata.getDescription());
          tableInfoDAO.setUpdatedBy(IdentityUtils.findPrincipalEmailAddress());
          tableInfoDAO.setUpdatedAt(new Date());
          session.merge(tableInfoDAO);
          return null;
        },
        "Failed to update table metadata",
        false);
  }

  private String fullName(String catalog, String schema, String table) {
    return catalog + "." + schema + "." + table;
  }

  private String computeEtag(TableInfo tableInfo, long latestVersion) {
    long updatedAt =
        tableInfo.getUpdatedAt() != null ? tableInfo.getUpdatedAt() : tableInfo.getCreatedAt();
    return tableInfo.getTableId() + ":" + updatedAt + ":" + latestVersion;
  }

  private Set<CredentialContext.Privilege> toPrivileges(String operation) {
    TableOperation tableOperation = TableOperation.fromValue(operation);
    return switch (tableOperation) {
      case READ -> Set.of(SELECT);
      case READ_WRITE -> Set.of(SELECT, UPDATE);
      case UNKNOWN_TABLE_OPERATION -> Collections.emptySet();
    };
  }
}
