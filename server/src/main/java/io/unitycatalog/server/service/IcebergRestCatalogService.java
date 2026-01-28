package io.unitycatalog.server.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Head;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.ProducesJson;
import io.unitycatalog.server.exception.IcebergRestExceptionHandler;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.ColumnTypeName;
import io.unitycatalog.server.model.ListSchemasResponse;
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.ViewInfo;
import io.unitycatalog.server.model.ViewRepresentation;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.ViewRepository;
import io.unitycatalog.server.service.iceberg.MetadataService;
import io.unitycatalog.server.service.iceberg.TableConfigService;
import io.unitycatalog.server.utils.JsonUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.responses.ConfigResponse;
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
          Endpoint.V1_LIST_VIEWS);

  private final CatalogService catalogService;
  private final SchemaService schemaService;
  private final TableService tableService;
  private final TableConfigService tableConfigService;
  private final MetadataService metadataService;
  private final TableRepository tableRepository;
  private final ViewRepository viewRepository;
  private final SessionFactory sessionFactory;

  public IcebergRestCatalogService(
      CatalogService catalogService,
      SchemaService schemaService,
      TableService tableService,
      TableConfigService tableConfigService,
      MetadataService metadataService,
      Repositories repositories) {
    this.catalogService = catalogService;
    this.schemaService = schemaService;
    this.tableService = tableService;
    this.tableConfigService = tableConfigService;
    this.metadataService = metadataService;
    this.tableRepository = repositories.getTableRepository();
    this.viewRepository = repositories.getViewRepository();
    this.sessionFactory = repositories.getSessionFactory();
  }

  // Config APIs

  @Get("/v1/config")
  @ProducesJson
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

  // Table APIs

  @Head("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}")
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
  public LoadTableResponse loadTable(
      @Param("catalog") String catalog,
      @Param("namespace") String namespace,
      @Param("table") String table) {
    String metadataLocation;
    try (Session session = sessionFactory.openSession()) {
      tableRepository.getTable(catalog + "." + namespace + "." + table);
      metadataLocation =
          tableRepository.getTableUniformMetadataLocation(session, catalog, namespace, table);
    }

    if (metadataLocation == null) {
      throw new NoSuchTableException("Table does not exist: %s", namespace + "." + table);
    }

    TableMetadata tableMetadata = metadataService.readTableMetadata(metadataLocation);
    Map<String, String> config = tableConfigService.getTableConfig(tableMetadata);

    return LoadTableResponse.builder()
        .withTableMetadata(tableMetadata)
        .addAllConfig(config)
        .build();
  }

  @Get("/v1/catalogs/{catalog}/namespaces/{namespace}/views/{view}")
  @ProducesJson
  public LoadViewResponse loadView(
      @Param("catalog") String catalog,
      @Param("namespace") String namespace,
      @Param("view") String view) {
    String fullName = catalog + "." + namespace + "." + view;
    ViewInfo viewInfo = viewRepository.getView(fullName);
    ViewMetadata viewMetadata = buildViewMetadata(catalog, namespace, viewInfo);
    return createLoadViewResponse(viewMetadata);
  }

  private LoadViewResponse createLoadViewResponse(ViewMetadata metadata) {
    return new LoadViewResponse() {
      @Override
      public String metadataLocation() {
        return ""; // Views in UC don't have a persisted metadata location
      }

      @Override
      public ViewMetadata metadata() {
        return metadata;
      }

      @Override
      public Map<String, String> config() {
        return Collections.emptyMap();
      }
    };
  }

  private ViewMetadata buildViewMetadata(String catalog, String namespace, ViewInfo viewInfo) {
    Schema icebergSchema = buildIcebergSchema(viewInfo);
    org.apache.iceberg.view.ViewVersion currentVersion =
        buildIcebergVersion(viewInfo, namespace, catalog);

    return ViewMetadata.builder()
        .setLocation("uc://" + catalog + "/" + namespace + "/" + viewInfo.getName())
        .addSchema(icebergSchema)
        .setCurrentVersion(currentVersion, icebergSchema)
        .setProperties(Collections.emptyMap())
        .build();
  }

  private Schema buildIcebergSchema(ViewInfo viewInfo) {
    if (viewInfo.getColumns() == null || viewInfo.getColumns().isEmpty()) {
      return new Schema(Collections.emptyList());
    }

    List<Types.NestedField> fields = new ArrayList<>();
    int fieldId = 1;
    for (ColumnInfo col : viewInfo.getColumns()) {
      fields.add(
          Types.NestedField.optional(fieldId++, col.getName(), convertToIcebergType(col)));
    }

    return new Schema(1, fields);
  }

  private org.apache.iceberg.view.ViewVersion buildIcebergVersion(
      ViewInfo viewInfo, String namespace, String catalog) {
    List<org.apache.iceberg.view.ViewRepresentation> representations = new ArrayList<>();
    for (ViewRepresentation ucRep : viewInfo.getRepresentations()) {
      representations.add(
          ImmutableSQLViewRepresentation.builder()
              .sql(ucRep.getSql())
              .dialect(ucRep.getDialect())
              .build());
    }

    return ImmutableViewVersion.builder()
        .versionId(1)
        .schemaId(1)
        .timestampMillis(viewInfo.getCreatedAt())
        .summary(Collections.emptyMap())
        .representations(representations)
        .defaultNamespace(Namespace.of(namespace))
        .defaultCatalog(catalog)
        .build();
  }

  private Type convertToIcebergType(ColumnInfo col) {
    ColumnTypeName typeName = col.getTypeName();
    if (typeName == null) {
      return Types.StringType.get();
    }
    return switch (typeName) {
      case BOOLEAN -> Types.BooleanType.get();
      case BYTE -> Types.IntegerType.get();
      case SHORT -> Types.IntegerType.get();
      case INT -> Types.IntegerType.get();
      case LONG -> Types.LongType.get();
      case FLOAT -> Types.FloatType.get();
      case DOUBLE -> Types.DoubleType.get();
      case DATE -> Types.DateType.get();
      case TIMESTAMP -> Types.TimestampType.withZone();
      case TIMESTAMP_NTZ -> Types.TimestampType.withoutZone();
      case STRING, CHAR -> Types.StringType.get();
      case BINARY -> Types.BinaryType.get();
      case DECIMAL -> Types.DecimalType.of(
          col.getTypePrecision() != null ? col.getTypePrecision() : 38,
          col.getTypeScale() != null ? col.getTypeScale() : 18);
      case INTERVAL, ARRAY, STRUCT, MAP, NULL, USER_DEFINED_TYPE, TABLE_TYPE ->
          Types.StringType.get(); // Fallback for complex/unsupported types
    };
  }

  @Post("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/metrics")
  public HttpResponse reportMetrics(
      @Param("namespace") String namespace, @Param("table") String table) {
    return HttpResponse.of(HttpStatus.OK);
  }

  @Get("/v1/catalogs/{catalog}/namespaces/{namespace}/tables")
  @ProducesJson
  public org.apache.iceberg.rest.responses.ListTablesResponse listTables(
      @Param("catalog") String catalog, @Param("namespace") String namespace)
      throws JsonProcessingException {
    AggregatedHttpResponse resp =
        tableService
            .listTables(
                catalog,
                namespace,
                Optional.of(Integer.MAX_VALUE),
                Optional.empty(),
                Optional.empty(),
                Optional.empty())
            .aggregate()
            .join();
    List<TableIdentifier> filteredTables;
    try (Session session = sessionFactory.openSession()) {
      filteredTables =
          Objects.requireNonNull(
                  JsonUtils.getInstance()
                      .readValue(resp.contentUtf8(), ListTablesResponse.class)
                      .getTables())
              .stream()
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

  @Get("/v1/catalogs/{catalog}/namespaces/{namespace}/views")
  @ProducesJson
  public org.apache.iceberg.rest.responses.ListTablesResponse listViews(
      @Param("catalog") String catalog, @Param("namespace") String namespace) {
    List<TableIdentifier> viewIdentifiers =
        viewRepository.listViews(catalog, namespace).stream()
            .map(
                viewInfo ->
                    TableIdentifier.of(Namespace.of(viewInfo.getSchemaName()), viewInfo.getName()))
            .collect(Collectors.toList());

    return org.apache.iceberg.rest.responses.ListTablesResponse.builder()
        .addAll(viewIdentifiers)
        .build();
  }
}
