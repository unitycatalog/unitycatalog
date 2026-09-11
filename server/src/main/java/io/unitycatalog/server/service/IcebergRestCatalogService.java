package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.service.credential.CredentialContext.READ_ONLY;
import static io.unitycatalog.server.service.credential.CredentialContext.READ_WRITE;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Head;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.ProducesJson;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.IcebergRestExceptionHandler;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.CreateSchema;
import io.unitycatalog.server.model.CreateStagingTable;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.ListSchemasResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.StagingTableInfo;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.StagingTableRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.TableRepository.IcebergTablePage;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.model.DeletedResource;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.iceberg.IcebergSchemaConverter;
import io.unitycatalog.server.service.iceberg.MetadataService;
import io.unitycatalog.server.service.iceberg.TableConfigService;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.rest.RESTCatalogProperties.SnapshotMode;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergRestCatalogService extends AuthorizedService implements RegisteredService {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergRestCatalogService.class);

  private static final String PREFIX_BASE = "catalogs/";

  private static final List<Endpoint> READ_ENDPOINTS =
      List.of(
          Endpoint.V1_LIST_NAMESPACES,
          Endpoint.V1_LOAD_NAMESPACE,
          Endpoint.V1_NAMESPACE_EXISTS,
          Endpoint.V1_TABLE_EXISTS,
          Endpoint.V1_LOAD_TABLE,
          Endpoint.V1_LOAD_VIEW,
          Endpoint.V1_REPORT_METRICS,
          Endpoint.V1_LIST_TABLES);

  private static final List<Endpoint> WRITE_ENDPOINTS =
      List.of(
          Endpoint.V1_CREATE_NAMESPACE,
          Endpoint.V1_DELETE_NAMESPACE,
          Endpoint.V1_CREATE_TABLE,
          Endpoint.V1_UPDATE_TABLE,
          Endpoint.V1_DELETE_TABLE,
          Endpoint.V1_RENAME_TABLE);

  private final TableConfigService tableConfigService;
  private final MetadataService metadataService;
  private final CatalogRepository catalogRepository;
  private final SchemaRepository schemaRepository;
  private final StagingTableRepository stagingTableRepository;
  private final TableRepository tableRepository;

  @Override
  public ExceptionHandlerFunction exceptionHandler() {
    return IcebergRestExceptionHandler.INSTANCE;
  }

  public IcebergRestCatalogService(
      UnityCatalogAuthorizer authorizer,
      TableConfigService tableConfigService,
      MetadataService metadataService,
      Repositories repositories,
      ServerProperties serverProperties) {
    super(authorizer, repositories, serverProperties);
    this.tableConfigService = tableConfigService;
    this.metadataService = metadataService;
    this.catalogRepository = repositories.getCatalogRepository();
    this.schemaRepository = repositories.getSchemaRepository();
    this.stagingTableRepository = repositories.getStagingTableRepository();
    this.tableRepository = repositories.getTableRepository();
  }

  // Config APIs

  @Get("/v1/config")
  @ProducesJson
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public ConfigResponse config(@Param("warehouse") Optional<String> catalogOpt) {
    String catalog =
        catalogOpt.orElseThrow(
            () -> new BadRequestException("Must supply a proper catalog in warehouse property."));

    // The prefix below only leads anywhere if the catalog exists. Per the REST spec a warehouse
    // that does not is a 404 here, rather than a config whose every later request fails.
    catalogRepository.getCatalog(catalog);

    // set catalog prefix
    return ConfigResponse.builder()
        .withOverride("prefix", PREFIX_BASE + catalog)
        .withEndpoints(
            serverProperties.isIcebergTableEnabled()
                ? Stream.concat(READ_ENDPOINTS.stream(), WRITE_ENDPOINTS.stream()).toList()
                : READ_ENDPOINTS)
        .build();
  }

  // Namespace APIs

  @Get("/v1/catalogs/{catalog}/namespaces")
  @ProducesJson
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public ListNamespacesResponse listNamespaces(
      @Param("catalog") String catalog, @Param("parent") Optional<String> parent) {
    List<Namespace> namespaces = new ArrayList<>();
    if (parent.isPresent() && !parent.get().isEmpty()) {
      // Nested namespaces are not supported, so a parent yields no child namespaces. The parent is
      // still resolved rather than assumed: per the REST spec, listing under a namespace that does
      // not exist is a 404, which an empty list would hide from the client.
      schemaRepository.getSchema(String.join(".", catalog, parent.get()));
    } else {
      // This endpoint returns the whole listing, so follow the repository's page token to the end.
      Optional<String> pageToken = Optional.empty();
      do {
        ListSchemasResponse resp =
            schemaRepository.listSchemas(catalog, Optional.empty(), pageToken);
        assert resp.getSchemas() != null;
        resp.getSchemas().forEach(schemaInfo -> namespaces.add(Namespace.of(schemaInfo.getName())));
        pageToken = nextPageToken(resp.getNextPageToken());
      } while (pageToken.isPresent());
    }

    return ListNamespacesResponse.builder().addAll(namespaces).build();
  }

  @Head("/v1/catalogs/{catalog}/namespaces/{namespace}")
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse namespaceExists(
      @Param("catalog") String catalog, @Param("namespace") String namespace) {
    // Without a route of its own, this HEAD was served by the GET below, which answered 200 and
    // described a body a HEAD response must not carry. The REST spec answers it with 204.
    schemaRepository.getSchema(String.join(".", catalog, namespace));
    return HttpResponse.of(HttpStatus.NO_CONTENT);
  }

  @Get("/v1/catalogs/{catalog}/namespaces/{namespace}")
  @ProducesJson
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public GetNamespaceResponse getNamespace(
      @Param("catalog") String catalog, @Param("namespace") String namespace) {
    String schemaFullName = String.join(".", catalog, namespace);
    SchemaInfo schemaInfo = schemaRepository.getSchema(schemaFullName);
    return GetNamespaceResponse.builder()
        .withNamespace(Namespace.of(namespace))
        .setProperties(schemaInfo.getProperties())
        .build();
  }

  @Post("/v1/catalogs/{catalog}/namespaces")
  @ProducesJson
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public CreateNamespaceResponse createNamespace(
      @Param("catalog") String catalog, CreateNamespaceRequest request) {
    serverProperties.checkIcebergTableEnabled();
    request.validate();
    if (request.namespace().levels().length != 1) {
      throw new BadRequestException("Nested namespaces are not supported: %s", request.namespace());
    }
    String schemaName = request.namespace().level(0);
    CreateSchema createSchema =
        new CreateSchema().name(schemaName).catalogName(catalog).properties(request.properties());
    SchemaInfo schemaInfo = schemaRepository.createSchema(createSchema);
    CatalogInfo catalogInfo = catalogRepository.getCatalog(catalog);
    initializeHierarchicalAuthorization(schemaInfo.getSchemaId(), catalogInfo.getId());
    Map<String, String> properties =
        schemaInfo.getProperties() == null ? Map.of() : schemaInfo.getProperties();
    return CreateNamespaceResponse.builder()
        .withNamespace(request.namespace())
        .setProperties(properties)
        .build();
  }

  @Delete("/v1/catalogs/{catalog}/namespaces/{namespace}")
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse dropNamespace(
      @Param("catalog") String catalog, @Param("namespace") String namespace) {
    serverProperties.checkIcebergTableEnabled();
    List<DeletedResource> deleted;
    try {
      deleted = schemaRepository.deleteSchema(String.join(".", catalog, namespace), false);
    } catch (BaseException failure) {
      if (failure.getErrorCode() == ErrorCode.FAILED_PRECONDITION) {
        // A schema that still holds objects is a failed precondition to the repository, which is a
        // 400; the REST spec answers a namespace that is not empty with 409.
        throw new NamespaceNotEmptyException("Namespace is not empty: %s", namespace);
      }
      throw failure;
    }
    clearDeletedResourceAuthorizations(deleted);
    return HttpResponse.of(HttpStatus.NO_CONTENT);
  }

  // Table APIs

  @Head("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}")
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse tableExists(
      @Param("catalog") String catalog,
      @Param("namespace") String namespace,
      @Param("table") String table) {
    TableRepository.IcebergTableState state =
        tableRepository.getIcebergTableState(catalog, namespace, table);
    if (state.metadataLocation() == null) {
      throw new NoSuchTableException("Table does not exist: %s", namespace + "." + table);
    }
    // The REST spec answers this HEAD with 204 and no content. Answering 200 also had Armeria
    // describe a body ("200 OK", Content-Length: 6) that a HEAD response must not carry.
    return HttpResponse.of(HttpStatus.NO_CONTENT);
  }

  @Get("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}")
  @ProducesJson
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public LoadTableResponse loadTable(
      @Param("catalog") String catalog,
      @Param("namespace") String namespace,
      @Param("table") String table,
      @Param(RESTCatalogProperties.SNAPSHOTS_QUERY_PARAMETER) Optional<String> snapshots) {
    TableRepository.IcebergTableState state =
        tableRepository.getIcebergTableState(catalog, namespace, table);
    if (state.metadataLocation() == null) {
      throw new NoSuchTableException("Table does not exist: %s", namespace + "." + table);
    }

    NormalizedURL tableLocation = NormalizedURL.from(state.storageLocation());
    TableMetadata tableMetadata =
        metadataService.readTableMetadata(
            NormalizedURL.from(state.metadataLocation()), tableLocation);
    if (refsOnly(snapshots)) {
      tableMetadata =
          TableMetadata.buildFrom(tableMetadata)
              .withMetadataLocation(tableMetadata.metadataFileLocation())
              .suppressHistoricalSnapshots()
              .build();
    }
    Map<String, String> config =
        tableConfigService.getTableConfig(tableLocation, getLoadCredentialPrivileges(state));

    return LoadTableResponse.builder()
        .withTableMetadata(tableMetadata)
        .addAllConfig(config)
        .build();
  }

  Set<CredentialContext.Privilege> getLoadCredentialPrivileges(
      TableRepository.IcebergTableState state) {
    if (state.dataSourceFormat() != DataSourceFormat.ICEBERG) {
      return READ_ONLY;
    }
    UUID principalId = userRepository.findPrincipalId();
    if (principalId == null) {
      return READ_ONLY;
    }
    boolean canWrite =
        authorizer.authorize(principalId, state.tableId(), Privileges.OWNER)
            || authorizer.authorizeAll(
                principalId, state.tableId(), Privileges.SELECT, Privileges.MODIFY);
    return canWrite ? READ_WRITE : READ_ONLY;
  }

  @Post("/v1/catalogs/{catalog}/namespaces/{namespace}/tables")
  @ProducesJson
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public LoadTableResponse createTable(
      @Param("catalog") String catalog,
      @Param("namespace") String namespace,
      CreateTableRequest request) {
    serverProperties.checkIcebergTableEnabled();
    request.validate();
    NormalizedURL location;
    TableType tableType;
    if (request.location() == null || request.location().isEmpty()) {
      StagingTableInfo stagingTable =
          stagingTableRepository.createStagingTable(
              new CreateStagingTable()
                  .name(request.name())
                  .catalogName(catalog)
                  .schemaName(namespace));
      initializeHierarchicalAuthorization(
          stagingTable.getId(), schemaRepository.getSchemaIdOrThrow(catalog, namespace).toString());
      tableType = TableType.MANAGED;
      location = NormalizedURL.from(stagingTable.getStagingLocation());
    } else {
      tableType = TableType.EXTERNAL;
      location = NormalizedURL.from(request.location());
    }
    Map<String, String> properties = request.properties() == null ? Map.of() : request.properties();
    PartitionSpec spec = request.spec() == null ? PartitionSpec.unpartitioned() : request.spec();
    SortOrder writeOrder =
        request.writeOrder() == null ? SortOrder.unsorted() : request.writeOrder();
    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(
            request.schema(), spec, writeOrder, location.toString(), properties);

    if (request.stageCreate()) {
      // The permanent table is not registered and no metadata file is written. Managed creates
      // retain their staging row so duplicate validation, temporary credentials, and later
      // lifecycle management use the same path as other managed tables.
      if (tableType == TableType.EXTERNAL && ucTableExists(catalog, namespace, request.name())) {
        throw new AlreadyExistsException("Table already exists: %s.%s", namespace, request.name());
      }
      metadataService.prepareTableLocation(tableMetadata, location);
      return LoadTableResponse.builder()
          .withTableMetadata(tableMetadata)
          .addAllConfig(tableConfigService.getTableConfig(location, READ_WRITE))
          .build();
    }

    return finalizeIcebergTableCreation(
        catalog, namespace, request.name(), tableType, tableMetadata, false);
  }

  /**
   * Writes a new Iceberg table's first metadata file and atomically registers the UC row, columns,
   * properties, committed staging row, and metadata pointer. Shared by direct creates and commits
   * of staged creates; {@code fromCommit} only changes the already-exists error shape (409 {@link
   * CommitFailedException} for commits, 409 {@link AlreadyExistsException} for creates).
   */
  private LoadTableResponse finalizeIcebergTableCreation(
      String catalog,
      String namespace,
      String name,
      TableType tableType,
      TableMetadata tableMetadata,
      boolean fromCommit) {
    NormalizedURL tableLocation = NormalizedURL.from(tableMetadata.location());
    NormalizedURL metadataLocation =
        MetadataService.newMetadataLocation(tableMetadata, 0, tableLocation);
    TableMetadata committed =
        TableMetadata.buildFrom(tableMetadata)
            // Discard builder-only changes before assigning the authoritative metadata pointer.
            .discardChanges()
            .withMetadataLocation(MetadataService.toIcebergMetadataLocation(metadataLocation))
            .build();

    CreateTable createTable =
        new CreateTable()
            .name(name)
            .catalogName(catalog)
            .schemaName(namespace)
            .tableType(tableType)
            .dataSourceFormat(DataSourceFormat.ICEBERG)
            .columns(IcebergSchemaConverter.toColumnInfos(committed.schema()))
            .storageLocation(committed.location())
            .properties(committed.properties());
    metadataService.writeTableMetadata(committed, metadataLocation, tableLocation);
    TableInfo tableInfo;
    try {
      tableInfo = tableRepository.createTableForIceberg(createTable, metadataLocation);
    } catch (RuntimeException e) {
      metadataService.deleteTableMetadata(metadataLocation, tableLocation);
      if (fromCommit
          && e instanceof BaseException baseException
          && baseException.getErrorCode() == ErrorCode.TABLE_ALREADY_EXISTS) {
        throw new CommitFailedException(
            "Requirement failed: table already exists: %s.%s", namespace, name);
      }
      throw e;
    }
    SchemaInfo schemaInfo =
        schemaRepository.getSchema(tableInfo.getCatalogName() + "." + tableInfo.getSchemaName());
    if (tableType == TableType.EXTERNAL) {
      initializeHierarchicalAuthorization(tableInfo.getTableId(), schemaInfo.getSchemaId());
    }

    // Use the location returned from the persisted UC row for credential vending, not the
    // client-supplied metadata object used to create the row.
    NormalizedURL persistedTableLocation = NormalizedURL.from(tableInfo.getStorageLocation());
    return LoadTableResponse.builder()
        .withTableMetadata(committed)
        .addAllConfig(tableConfigService.getTableConfig(persistedTableLocation, READ_WRITE))
        .build();
  }

  @Post("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}")
  @ProducesJson
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public LoadTableResponse updateTable(
      @Param("catalog") String catalog,
      @Param("namespace") String namespace,
      @Param("table") String table,
      UpdateTableRequest request) {
    serverProperties.checkIcebergTableEnabled();
    boolean isCreateCommit =
        request.requirements().stream()
            .anyMatch(r -> r instanceof UpdateRequirement.AssertTableDoesNotExist);
    if (isCreateCommit) {
      return commitStagedCreate(catalog, namespace, table, request);
    }
    TableRepository.IcebergTableState state =
        tableRepository.getNativeIcebergTableState(catalog, namespace, table);
    if (state.metadataLocation() == null) {
      throw new NoSuchTableException("Table does not exist: %s", namespace + "." + table);
    }

    NormalizedURL tableLocation = NormalizedURL.from(state.storageLocation());
    NormalizedURL metadataLocation = NormalizedURL.from(state.metadataLocation());
    TableMetadata base = metadataService.readTableMetadata(metadataLocation, tableLocation);
    request.requirements().forEach(requirement -> requirement.validate(base));

    TableMetadata.Builder builder = TableMetadata.buildFrom(base);
    request.updates().forEach(update -> update.applyTo(builder));
    TableMetadata updatedWithoutLocation = builder.build();
    if (updatedWithoutLocation.changes().isEmpty()) {
      // No-op update: requirements were validated, but there is no new metadata file or DAO write.
      return LoadTableResponse.builder()
          .withTableMetadata(base)
          .addAllConfig(tableConfigService.getTableConfig(tableLocation, READ_WRITE))
          .build();
    }

    NormalizedURL newMetadataLocation =
        MetadataService.newMetadataLocation(
            updatedWithoutLocation,
            MetadataService.parseMetadataVersion(metadataLocation) + 1,
            tableLocation);
    TableMetadata updated =
        TableMetadata.buildFrom(updatedWithoutLocation)
            // Iceberg update builders retain pending changes; persist a clean snapshot with the
            // newly assigned metadata location.
            .discardChanges()
            .withMetadataLocation(MetadataService.toIcebergMetadataLocation(newMetadataLocation))
            .build();
    metadataService.writeTableMetadata(updated, newMetadataLocation, tableLocation);
    try {
      tableRepository.commitIcebergTable(
          catalog,
          namespace,
          table,
          metadataLocation.toString(),
          newMetadataLocation,
          IcebergSchemaConverter.toColumnInfos(updated.schema()),
          updated.properties());
    } catch (RuntimeException e) {
      // The metadata file was written before the swap, so any failure (a lost commit race or an
      // unexpected error) leaves it orphaned unless we clean it up.
      metadataService.deleteTableMetadata(newMetadataLocation, tableLocation);
      throw e;
    }

    return LoadTableResponse.builder()
        .withTableMetadata(updated)
        .addAllConfig(tableConfigService.getTableConfig(tableLocation, READ_WRITE))
        .build();
  }

  @Delete("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}")
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse dropTable(
      @Param("catalog") String catalog,
      @Param("namespace") String namespace,
      @Param("table") String table,
      @Param("purgeRequested") Optional<Boolean> purgeRequested) {
    serverProperties.checkIcebergTableEnabled();
    String fullName = catalog + "." + namespace + "." + table;
    TableRepository.IcebergTableState state =
        tableRepository.getIcebergTableState(catalog, namespace, table);
    if (state.dataSourceFormat() != DataSourceFormat.ICEBERG) {
      throw new BadRequestException(
          "Table %s was not created through the Iceberg REST catalog; drop it through the Unity"
              + " Catalog API instead.",
          fullName);
    }
    // EXTERNAL tables leave data and metadata files in place (purgeRequested is accepted for
    // spec compatibility but does not delete files); MANAGED tables have their storage directory
    // removed by the repository's delete path.
    TableInfoDAO deleted = tableRepository.deleteTable(catalog, namespace, table);
    removeHierarchicalAuthorizations(deleted.getId().toString(), deleted.getSchemaId().toString());
    return HttpResponse.of(HttpStatus.NO_CONTENT);
  }

  /**
   * Materializes a staged create: a commit whose requirements carry {@code assert-create}
   * (AssertTableDoesNotExist). Mirroring Iceberg's reference CatalogHandlers, the staged metadata
   * is rebuilt from the commit's updates against an empty base, the first metadata file is written,
   * and the table is registered in UC. The table is MANAGED when its location matches an
   * uncommitted staging row, EXTERNAL otherwise.
   */
  private LoadTableResponse commitStagedCreate(
      String catalog, String namespace, String table, UpdateTableRequest request) {
    String fullName = catalog + "." + namespace + "." + table;
    for (UpdateRequirement requirement : request.requirements()) {
      if (!(requirement instanceof UpdateRequirement.AssertTableDoesNotExist)) {
        throw new BadRequestException(
            "Invalid requirement for a create commit: %s", requirement.getClass().getSimpleName());
      }
    }
    // Pick the format version out of the updates before building, like CatalogHandlers.create:
    // buildFromEmpty defaults to v2, and applying an upgrade-format-version update for a lower
    // version would otherwise fail as a downgrade.
    Optional<Integer> formatVersion =
        request.updates().stream()
            .filter(update -> update instanceof MetadataUpdate.UpgradeFormatVersion)
            .map(update -> ((MetadataUpdate.UpgradeFormatVersion) update).formatVersion())
            .findFirst();
    // buildFromEmpty(int) seeds the requested version; the no-arg overload uses the default.
    TableMetadata.Builder builder =
        formatVersion.map(TableMetadata::buildFromEmpty).orElseGet(TableMetadata::buildFromEmpty);
    request.updates().forEach(update -> update.applyTo(builder));
    TableMetadata tableMetadata = builder.build();
    if (tableMetadata.location() == null || tableMetadata.location().isEmpty()) {
      throw new BadRequestException(
          "Create commit for %s must include a set-location update", fullName);
    }

    // Managed locations carry UC's reserved marker. The final repository transaction performs the
    // authoritative staging-row ownership and committed-state validation.
    TableType tableType =
        tableMetadata.location().contains(Constants.MANAGED_STORAGE_PREFIX)
            ? TableType.MANAGED
            : TableType.EXTERNAL;
    return finalizeIcebergTableCreation(catalog, namespace, table, tableType, tableMetadata, true);
  }

  private boolean ucTableExists(String catalog, String namespace, String table) {
    try {
      tableRepository.getTable(catalog + "." + namespace + "." + table);
      return true;
    } catch (BaseException e) {
      if (e.getErrorCode() == ErrorCode.TABLE_NOT_FOUND) {
        return false;
      }
      throw e;
    }
  }

  @Post("/v1/catalogs/{catalog}/tables/rename")
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse renameTable(@Param("catalog") String catalog, RenameTableRequest request) {
    serverProperties.checkIcebergTableEnabled();
    // A request missing either identifier is a bad request, not the NPE reading it would raise.
    request.validate();
    Namespace source = request.source().namespace();
    Namespace destination = request.destination().namespace();
    if (!source.equals(destination)) {
      // Unity Catalog has no way to move a table between schemas, so a rename that asks for one is
      // reported as an operation this server does not implement rather than half-applied.
      throw new BaseException(
          ErrorCode.UNIMPLEMENTED,
          "Renaming a table into another namespace is not supported: "
              + source
              + " to "
              + destination);
    }
    String namespace = source.toString();
    String fullName = catalog + "." + namespace + "." + request.source().name();
    // As with dropTable, only a table created through this API may be changed through it: a UniForm
    // table is a Delta table that these endpoints read, and renaming it here would rename the Delta
    // table under its own API.
    TableRepository.IcebergTableState state =
        tableRepository.getIcebergTableState(catalog, namespace, request.source().name());
    if (state.dataSourceFormat() != DataSourceFormat.ICEBERG) {
      throw new BadRequestException(
          "Table %s was not created through the Iceberg REST catalog; rename it through the Unity"
              + " Catalog API instead.",
          fullName);
    }
    tableRepository.renameTable(
        catalog, namespace, request.source().name(), request.destination().name());
    return HttpResponse.of(HttpStatus.NO_CONTENT);
  }

  @Get("/v1/catalogs/{catalog}/namespaces/{namespace}/views/{view}")
  @ProducesJson
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public LoadViewResponse loadView(
      @Param("namespace") String namespace, @Param("view") String view) {
    // this is not supported yet, but Iceberg REST client tries to load
    // a table with given path name and then tries to load a view with that
    // name if it didn't find a table, so for now, let's just return a 404
    // as that should be expected since it didn't find a table with the name
    throw new NoSuchViewException("View does not exist: %s", namespace + "." + view);
  }

  /**
   * Accept a scan or commit report from an Iceberg client. Clients report after every scan and
   * commit as long as the server advertises {@code V1_REPORT_METRICS}, which this service does. The
   * report is acknowledged and discarded -- UC does not persist client telemetry today -- but the
   * table is still resolved so that a report naming a table UC doesn't serve is answered with a
   * 404, as the REST spec requires.
   */
  @Post("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/metrics")
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse reportMetrics(
      @Param("catalog") String catalog,
      @Param("namespace") String namespace,
      @Param("table") String table,
      ReportMetricsRequest request) {
    TableRepository.IcebergTableState state =
        tableRepository.getIcebergTableState(catalog, namespace, table);
    if (state.metadataLocation() == null) {
      throw new NoSuchTableException("Table does not exist: %s", namespace + "." + table);
    }

    LOGGER.debug("Received {} for table {}.{}.{}", request.reportType(), catalog, namespace, table);
    return HttpResponse.of(HttpStatus.NO_CONTENT);
  }

  @Get("/v1/catalogs/{catalog}/namespaces/{namespace}/tables")
  @ProducesJson
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public org.apache.iceberg.rest.responses.ListTablesResponse listTables(
      @Param("catalog") String catalog, @Param("namespace") String namespace) {
    // This endpoint returns the whole listing, so follow the repository's page token to the end.
    // Each page already says which of its tables carry Iceberg metadata, so no listed table is
    // resolved a second time: doing that made a table dropped mid-listing fail the entire request.
    org.apache.iceberg.rest.responses.ListTablesResponse.Builder listed =
        org.apache.iceberg.rest.responses.ListTablesResponse.builder();
    Optional<String> pageToken = Optional.empty();
    do {
      IcebergTablePage page = tableRepository.listIcebergTables(catalog, namespace, pageToken);
      page.tableNames()
          .forEach(table -> listed.add(TableIdentifier.of(Namespace.of(namespace), table)));
      pageToken = page.nextPageToken();
    } while (pageToken.isPresent());

    return listed.build();
  }

  /**
   * Whether the request asked for only the snapshots the table's refs point at. The REST spec's
   * {@code snapshots} parameter takes "all", which is also the default, or "refs"; anything else is
   * a bad request rather than a silently full response.
   */
  private static boolean refsOnly(Optional<String> snapshots) {
    return snapshots.map(IcebergRestCatalogService::snapshotMode).orElse(SnapshotMode.ALL)
        == SnapshotMode.REFS;
  }

  /** Reads the parameter as the mode Iceberg's client names it with, rejecting anything else. */
  private static SnapshotMode snapshotMode(String snapshots) {
    try {
      return SnapshotMode.valueOf(snapshots.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(
          "Invalid snapshots parameter: %s. Valid values are %s and %s.",
          snapshots, modeName(SnapshotMode.ALL), modeName(SnapshotMode.REFS));
    }
  }

  /** The name the mode travels under on the wire, which the client lowercases. */
  private static String modeName(SnapshotMode mode) {
    return mode.name().toLowerCase(Locale.ROOT);
  }

  /**
   * Wraps the page token a listing returned, treating a missing or empty token as "no more pages".
   */
  private static Optional<String> nextPageToken(String token) {
    return Optional.ofNullable(token).filter(t -> !t.isEmpty());
  }
}
