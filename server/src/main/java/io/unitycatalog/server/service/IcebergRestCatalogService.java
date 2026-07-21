package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.METASTORE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Head;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.ProducesJson;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.IcebergRestExceptionHandler;
import io.unitycatalog.server.model.CreateSchema;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.ListSchemasResponse;
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.iceberg.IcebergSchemaConverter;
import io.unitycatalog.server.service.iceberg.MetadataService;
import io.unitycatalog.server.service.iceberg.TableConfigService;
import io.unitycatalog.server.utils.JsonUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
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
public class IcebergRestCatalogService {

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

  // All Iceberg REST endpoints currently authorize as metastore owner (see the
  // @AuthorizeExpression on each route), so write-capable storage credentials are only handed to
  // principals that could already write through the UC API.
  private static final Set<CredentialContext.Privilege> READ_WRITE = CredentialContext.READ_WRITE;

  private final SchemaService schemaService;
  private final TableService tableService;
  private final TableConfigService tableConfigService;
  private final MetadataService metadataService;
  private final TableRepository tableRepository;
  private final SessionFactory sessionFactory;

  public IcebergRestCatalogService(
      SchemaService schemaService,
      TableService tableService,
      TableConfigService tableConfigService,
      MetadataService metadataService,
      Repositories repositories) {
    this.schemaService = schemaService;
    this.tableService = tableService;
    this.tableConfigService = tableConfigService;
    this.metadataService = metadataService;
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
      @Param("catalog") String catalog, @Param("parent") Optional<String> parent)
      throws JsonProcessingException {
    List<Namespace> namespaces;
    if (parent.isPresent() && !parent.get().isEmpty()) {
      // nested namespaces is not supported, so child namespaces will be empty
      namespaces = Collections.emptyList();
    } else {
      String respContent =
          schemaService
              .listSchemas(catalog, Optional.of(Integer.MAX_VALUE), Optional.empty())
              .aggregate()
              .join()
              .contentUtf8();
      ListSchemasResponse resp =
          JsonUtils.getInstance().readValue(respContent, ListSchemasResponse.class);
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
      @Param("catalog") String catalog, @Param("namespace") String namespace)
      throws JsonProcessingException {
    String schemaFullName = String.join(".", catalog, namespace);
    String resp = schemaService.getSchema(schemaFullName).aggregate().join().contentUtf8();
    return GetNamespaceResponse.builder()
        .withNamespace(Namespace.of(namespace))
        .setProperties(JsonUtils.getInstance().readValue(resp, SchemaInfo.class).getProperties())
        .build();
  }

  @Post("/v1/catalogs/{catalog}/namespaces")
  @ProducesJson
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public CreateNamespaceResponse createNamespace(
      @Param("catalog") String catalog, CreateNamespaceRequest request)
      throws JsonProcessingException {
    request.validate();
    if (request.namespace().levels().length != 1) {
      throw new BadRequestException("Nested namespaces are not supported: %s", request.namespace());
    }
    String schemaName = request.namespace().level(0);
    CreateSchema createSchema =
        new CreateSchema().name(schemaName).catalogName(catalog).properties(request.properties());
    String resp;
    try {
      resp = schemaService.createSchema(createSchema).aggregate().join().contentUtf8();
    } catch (BaseException e) {
      if (e.getErrorCode() == ErrorCode.SCHEMA_ALREADY_EXISTS) {
        throw new AlreadyExistsException("Namespace already exists: %s", request.namespace());
      }
      throw e;
    }
    SchemaInfo schemaInfo = JsonUtils.getInstance().readValue(resp, SchemaInfo.class);
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
    try (Session session = sessionFactory.openSession()) {
      tableRepository.getTable(catalog + "." + namespace + "." + table);
      String metadataLocation =
          tableRepository.getTableUniformMetadataLocation(session, catalog, namespace, table);
      if (metadataLocation == null) {
        throw new NoSuchTableException("Table does not exist: %s", namespace + "." + table);
      } else {
        return HttpResponse.of(HttpStatus.OK);
      }
    }
  }

  @Get("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}")
  @ProducesJson
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public LoadTableResponse loadTable(
      @Param("catalog") String catalog,
      @Param("namespace") String namespace,
      @Param("table") String table) {
    TableInfo tableInfo = getIcebergTableInfo(catalog, namespace, table);
    String metadataLocation;
    try (Session session = sessionFactory.openSession()) {
      metadataLocation =
          tableRepository.getTableUniformMetadataLocation(session, catalog, namespace, table);
    }

    if (metadataLocation == null) {
      throw new NoSuchTableException("Table does not exist: %s", namespace + "." + table);
    }

    TableMetadata tableMetadata = metadataService.readTableMetadata(metadataLocation);
    // Native Iceberg tables are writable through this catalog, so vend write-capable storage
    // credentials; UniForm-derived metadata stays read-only.
    Map<String, String> config =
        tableInfo.getDataSourceFormat() == DataSourceFormat.ICEBERG
            ? tableConfigService.getTableConfig(tableMetadata, READ_WRITE)
            : tableConfigService.getTableConfig(tableMetadata);

    return LoadTableResponse.builder()
        .withTableMetadata(tableMetadata)
        .addAllConfig(config)
        .build();
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
    if (request.stageCreate()) {
      throw new BadRequestException(
          "Staged table creation is not supported yet. Clients should create tables directly,"
              + " e.g. DuckDB with `STAGE_CREATE_TABLES false` or PyIceberg/Spark createTable.");
    }
    String location = request.location();
    if (location == null || location.isEmpty()) {
      throw new BadRequestException(
          "Table location is required: Unity Catalog does not assign storage locations to"
              + " Iceberg REST tables yet.");
    }
    location = location.replaceAll("/+$", "");
    Map<String, String> properties = request.properties() == null ? Map.of() : request.properties();
    PartitionSpec spec = request.spec() == null ? PartitionSpec.unpartitioned() : request.spec();
    SortOrder writeOrder =
        request.writeOrder() == null ? SortOrder.unsorted() : request.writeOrder();
    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(request.schema(), spec, writeOrder, location, properties);
    String metadataLocation = MetadataService.newMetadataLocation(tableMetadata, 0);

    CreateTable createTable =
        new CreateTable()
            .name(request.name())
            .catalogName(catalog)
            .schemaName(namespace)
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.ICEBERG)
            .columns(IcebergSchemaConverter.toColumnInfos(tableMetadata.schema()))
            .storageLocation(location)
            .properties(properties);
    try {
      tableService.createTable(createTable);
    } catch (BaseException e) {
      if (e.getErrorCode() == ErrorCode.TABLE_ALREADY_EXISTS) {
        throw new AlreadyExistsException("Table already exists: %s.%s", namespace, request.name());
      }
      throw e;
    }
    try {
      metadataService.writeTableMetadata(tableMetadata, metadataLocation);
      tableRepository.setIcebergMetadataLocation(
          catalog, namespace, request.name(), null, metadataLocation);
    } catch (RuntimeException e) {
      // Roll the UC entry back so a failed create doesn't leave behind a table that can never be
      // loaded through this catalog.
      try {
        tableService.deleteTable(catalog + "." + namespace + "." + request.name());
      } catch (RuntimeException cleanupFailure) {
        e.addSuppressed(cleanupFailure);
      }
      metadataService.deleteTableMetadata(metadataLocation);
      throw e;
    }

    TableMetadata committed = metadataService.readTableMetadata(metadataLocation);
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
    String fullName = catalog + "." + namespace + "." + table;
    TableInfo tableInfo = getIcebergTableInfo(catalog, namespace, table);
    String metadataLocation;
    try (Session session = sessionFactory.openSession()) {
      metadataLocation =
          tableRepository.getTableUniformMetadataLocation(session, catalog, namespace, table);
    }
    if (metadataLocation == null) {
      throw new NoSuchTableException("Table does not exist: %s", namespace + "." + table);
    }
    if (tableInfo.getDataSourceFormat() != DataSourceFormat.ICEBERG) {
      throw new BadRequestException(
          "Table %s is a UniForm-enabled Delta table; its Iceberg metadata is derived from the"
              + " Delta log and is read-only through the Iceberg REST catalog.",
          fullName);
    }

    TableMetadata base = metadataService.readTableMetadata(metadataLocation);
    request.requirements().forEach(requirement -> requirement.validate(base));

    TableMetadata.Builder builder = TableMetadata.buildFrom(base);
    request.updates().forEach(update -> update.applyTo(builder));
    TableMetadata updated = builder.build();
    if (updated.changes().isEmpty()) {
      return LoadTableResponse.builder()
          .withTableMetadata(base)
          .addAllConfig(tableConfigService.getTableConfig(base, READ_WRITE))
          .build();
    }

    String newMetadataLocation =
        MetadataService.newMetadataLocation(
            updated, MetadataService.parseMetadataVersion(metadataLocation) + 1);
    metadataService.writeTableMetadata(updated, newMetadataLocation);
    try {
      tableRepository.setIcebergMetadataLocation(
          catalog, namespace, table, metadataLocation, newMetadataLocation);
    } catch (BaseException e) {
      metadataService.deleteTableMetadata(newMetadataLocation);
      if (e.getErrorCode() == ErrorCode.UPDATE_REQUIREMENT_CONFLICT) {
        throw new CommitFailedException("Commit conflict on %s: %s", fullName, e.getErrorMessage());
      }
      throw e;
    }

    TableMetadata committed = metadataService.readTableMetadata(newMetadataLocation);
    return LoadTableResponse.builder()
        .withTableMetadata(committed)
        .addAllConfig(tableConfigService.getTableConfig(committed, READ_WRITE))
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
    TableInfo tableInfo = getIcebergTableInfo(catalog, namespace, table);
    if (tableInfo.getDataSourceFormat() != DataSourceFormat.ICEBERG) {
      throw new BadRequestException(
          "Table %s was not created through the Iceberg REST catalog; drop it through the Unity"
              + " Catalog API instead.",
          fullName);
    }
    // Tables created through this API are EXTERNAL: data and metadata files are left in place,
    // so purgeRequested is accepted for spec compatibility but does not delete files.
    tableService.deleteTable(fullName);
    return HttpResponse.of(HttpStatus.NO_CONTENT);
  }

  /** Looks up the UC table entry, translating UC's not-found error into the Iceberg shape. */
  private TableInfo getIcebergTableInfo(String catalog, String namespace, String table) {
    try {
      return tableRepository.getTable(catalog + "." + namespace + "." + table);
    } catch (BaseException e) {
      if (e.getErrorCode() == ErrorCode.TABLE_NOT_FOUND) {
        throw new NoSuchTableException("Table does not exist: %s", namespace + "." + table);
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
      @Param("catalog") String catalog, @Param("namespace") String namespace)
      throws JsonProcessingException {
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
