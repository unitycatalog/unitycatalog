package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.service.credential.CredentialContext.READ_ONLY;
import static io.unitycatalog.server.service.credential.CredentialContext.READ_WRITE;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
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
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.StagingTableInfo;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.StagingTableRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.iceberg.IcebergSchemaConverter;
import io.unitycatalog.server.service.iceberg.MetadataService;
import io.unitycatalog.server.service.iceberg.TableConfigService;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
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
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

@ExceptionHandler(IcebergRestExceptionHandler.class)
public class IcebergRestCatalogService extends AuthorizedService {

  private static final String PREFIX_BASE = "catalogs/";

  private static final List<Endpoint> ENDPOINTS =
      List.of(
          Endpoint.V1_LIST_NAMESPACES,
          Endpoint.V1_LOAD_NAMESPACE,
          Endpoint.V1_TABLE_EXISTS,
          Endpoint.V1_LOAD_TABLE,
          Endpoint.V1_LOAD_VIEW,
          Endpoint.V1_REPORT_METRICS,
          Endpoint.V1_LIST_TABLES,
          Endpoint.V1_CREATE_NAMESPACE,
          Endpoint.V1_CREATE_TABLE,
          Endpoint.V1_UPDATE_TABLE,
          Endpoint.V1_DELETE_TABLE);

  private final TableConfigService tableConfigService;
  private final MetadataService metadataService;
  private final CatalogRepository catalogRepository;
  private final SchemaRepository schemaRepository;
  private final StagingTableRepository stagingTableRepository;
  private final TableRepository tableRepository;
  private final SessionFactory sessionFactory;

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
    this.sessionFactory = repositories.getSessionFactory();
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

    // TODO: check catalog exists
    // set catalog prefix
    return ConfigResponse.builder()
        .withOverride("prefix", PREFIX_BASE + catalog)
        .withEndpoints(ENDPOINTS)
        .build();
  }

  // Namespace APIs

  @Get("/v1/catalogs/{catalog}/namespaces")
  @ProducesJson
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public ListNamespacesResponse listNamespaces(
      @Param("catalog") String catalog, @Param("parent") Optional<String> parent) {
    List<Namespace> namespaces;
    if (parent.isPresent() && !parent.get().isEmpty()) {
      // nested namespaces is not supported, so child namespaces will be empty
      namespaces = Collections.emptyList();
    } else {
      ListSchemasResponse resp =
          schemaRepository.listSchemas(catalog, Optional.of(Integer.MAX_VALUE), Optional.empty());
      assert resp.getSchemas() != null;
      namespaces =
          resp.getSchemas().stream()
              .map(schemaInfo -> Namespace.of(schemaInfo.getName()))
              .collect(Collectors.toList());
    }

    return ListNamespacesResponse.builder().addAll(namespaces).build();
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
    return HttpResponse.of(HttpStatus.OK);
  }

  @Get("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}")
  @ProducesJson
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public LoadTableResponse loadTable(
      @Param("catalog") String catalog,
      @Param("namespace") String namespace,
      @Param("table") String table) {
    TableRepository.IcebergTableState state =
        tableRepository.getIcebergTableState(catalog, namespace, table);
    if (state.metadataLocation() == null) {
      throw new NoSuchTableException("Table does not exist: %s", namespace + "." + table);
    }

    TableMetadata tableMetadata =
        metadataService.readTableMetadata(NormalizedURL.from(state.metadataLocation()));
    Map<String, String> config =
        tableConfigService.getTableConfig(tableMetadata, getLoadCredentialPrivileges(state));

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
      if (ucTableExists(catalog, namespace, request.name())) {
        throw new AlreadyExistsException("Table already exists: %s.%s", namespace, request.name());
      }
      metadataService.prepareTableLocation(tableMetadata);
      return LoadTableResponse.builder()
          .withTableMetadata(tableMetadata)
          .addAllConfig(tableConfigService.getTableConfig(tableMetadata, READ_WRITE))
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
    NormalizedURL metadataLocation = MetadataService.newMetadataLocation(tableMetadata, 0);
    TableMetadata committed =
        TableMetadata.buildFrom(tableMetadata)
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
    metadataService.writeTableMetadata(committed, metadataLocation);
    TableInfo tableInfo;
    try {
      tableInfo = tableRepository.createTableForIceberg(createTable, metadataLocation);
    } catch (RuntimeException e) {
      metadataService.deleteTableMetadata(metadataLocation);
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

    return LoadTableResponse.builder()
        .withTableMetadata(committed)
        .addAllConfig(tableConfigService.getTableConfig(committed, READ_WRITE))
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

    NormalizedURL metadataLocation = NormalizedURL.from(state.metadataLocation());
    TableMetadata base = metadataService.readTableMetadata(metadataLocation);
    request.requirements().forEach(requirement -> requirement.validate(base));

    TableMetadata.Builder builder = TableMetadata.buildFrom(base);
    request.updates().forEach(update -> update.applyTo(builder));
    TableMetadata updatedWithoutLocation = builder.build();
    if (updatedWithoutLocation.changes().isEmpty()) {
      return LoadTableResponse.builder()
          .withTableMetadata(base)
          .addAllConfig(tableConfigService.getTableConfig(base, READ_WRITE))
          .build();
    }

    NormalizedURL newMetadataLocation =
        MetadataService.newMetadataLocation(
            updatedWithoutLocation, MetadataService.parseMetadataVersion(metadataLocation) + 1);
    TableMetadata updated =
        TableMetadata.buildFrom(updatedWithoutLocation)
            .discardChanges()
            .withMetadataLocation(MetadataService.toIcebergMetadataLocation(newMetadataLocation))
            .build();
    metadataService.writeTableMetadata(updated, newMetadataLocation);
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
      metadataService.deleteTableMetadata(newMetadataLocation);
      throw e;
    }

    return LoadTableResponse.builder()
        .withTableMetadata(updated)
        .addAllConfig(tableConfigService.getTableConfig(updated, READ_WRITE))
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
    if (ucTableExists(catalog, namespace, table)) {
      throw new CommitFailedException("Requirement failed: table already exists: %s", fullName);
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

    TableType tableType =
        stagingTableRepository.isUncommittedStagingLocation(
                NormalizedURL.from(tableMetadata.location()))
            ? TableType.MANAGED
            : TableType.EXTERNAL;
    return finalizeIcebergTableCreation(
        catalog, namespace, table, tableType, tableMetadata, true);
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

  @Post("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/metrics")
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse reportMetrics(
      @Param("namespace") String namespace, @Param("table") String table) {
    return HttpResponse.of(HttpStatus.OK);
  }

  @Get("/v1/catalogs/{catalog}/namespaces/{namespace}/tables")
  @ProducesJson
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public org.apache.iceberg.rest.responses.ListTablesResponse listTables(
      @Param("catalog") String catalog, @Param("namespace") String namespace) {
    ListTablesResponse tables =
        tableRepository.listTables(
            catalog, namespace, Optional.of(Integer.MAX_VALUE), Optional.empty(), false, false);
    List<TableIdentifier> filteredTables;
    try (Session session = sessionFactory.openSession()) {
      filteredTables =
          Objects.requireNonNull(tables.getTables()).stream()
              .filter(
                  tableInfo -> {
                    String metadataLocation =
                        tableRepository.getTableUniformMetadataLocation(
                            session, catalog, namespace, tableInfo.getName());
                    return metadataLocation != null;
                  })
              .map(
                  tableInfo ->
                      TableIdentifier.of(
                          Namespace.of(tableInfo.getSchemaName()), tableInfo.getName()))
              .collect(Collectors.toList());
    }

    return org.apache.iceberg.rest.responses.ListTablesResponse.builder()
        .addAll(filteredTables)
        .build();
  }
}
