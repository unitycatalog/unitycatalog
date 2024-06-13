package io.unitycatalog.server.service;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.HibernateUtil;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.utils.JsonUtils;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

@ExceptionHandler(GlobalExceptionHandler.class)
public class IcebergRestCatalogService {

  private final CatalogService catalogService;
  private final SchemaService schemaService;
  private final TableService tableService;
  private final TableRepository tableRepository = TableRepository.getInstance();
  private static final SessionFactory sessionFactory = HibernateUtil.getSessionFactory();

  public IcebergRestCatalogService(CatalogService catalogService,
                                   SchemaService schemaService,
                                   TableService tableService) {
    this.catalogService = catalogService;
    this.schemaService = schemaService;
    this.tableService = tableService;
  }

  // Config APIs

  @Get("/v1/config")
  @ProducesJson
  public ConfigResponse config() {
    return ConfigResponse.builder().build();
  }

  // Namespace APIs

  @Get("/v1/namespaces")
  @ProducesJson
  public ListNamespacesResponse listNamespaces(@Param("parent") Optional<String> parent)
    throws JsonProcessingException {
    // List catalogs if the parent is not present
    if (!parent.isPresent()) {
      String respContent = catalogService.listCatalogs().aggregate().join().contentUtf8();
      ListCatalogsResponse resp =
        JsonUtils.getInstance().readValue(respContent, ListCatalogsResponse.class);
      assert resp.getCatalogs() != null;
      List<Namespace> namespaces = resp.getCatalogs().stream()
        .map(catalogInfo -> Namespace.of(catalogInfo.getName()))
        .collect(Collectors.toList());
      return ListNamespacesResponse.builder()
        .addAll(namespaces)
        .build();
    }

    List<String> parentParts = Splitter.on(".").splitToList(parent.get());

    // If parent is a catalog, then list the schemas
    if (parentParts.size() == 1) {
      String catalogName = parentParts.get(0);
      String respContent =
        schemaService.listSchemas(
          catalogName, Optional.of(Integer.MAX_VALUE), Optional.empty())
                .aggregate().join().contentUtf8();
      ListSchemasResponse resp =
        JsonUtils.getInstance().readValue(respContent, ListSchemasResponse.class);
      assert resp.getSchemas() != null;
      List<Namespace> namespaces = resp.getSchemas().stream()
        .map(schemaInfo -> Namespace.of(schemaInfo.getCatalogName(), schemaInfo.getName()))
        .collect(Collectors.toList());
      return ListNamespacesResponse.builder()
        .addAll(namespaces)
        .build();
    }

    // If the parent is a schema, then return an empty list of namespaces
    if (parentParts.size() == 2) {
      // make sure the schema exists
      schemaService.getSchema(parent.get());
      return ListNamespacesResponse.builder().build();
    }

    throw new IllegalArgumentException("invalid parent " + parent.get());
  }

  @Get("/v1/namespaces/{namespace}")
  @ProducesJson
  public GetNamespaceResponse getNamespace(@Param("namespace") String namespace)
    throws JsonProcessingException {
    List<String> namespaceParts = Splitter.on(".").splitToList(namespace);

    // If namespace length is 1, then it is a catalog
    if (namespaceParts.size() == 1) {
      String catalogName = namespaceParts.get(0);
      String resp = catalogService.getCatalog(catalogName).aggregate().join().contentUtf8();
      CatalogInfo catalog = JsonUtils.getInstance().readValue(resp, CatalogInfo.class);
      return GetNamespaceResponse.builder()
        .withNamespace(Namespace.of(catalogName))
        .setProperties(catalog.getProperties())
        .build();
    }

    // If namespace length is 2, then it is a schema
    if (namespaceParts.size() == 2) {
      String catalogName = namespaceParts.get(0);
      String schemaName = namespaceParts.get(1);
      String schemaFullName = String.join(".", catalogName, schemaName);
      String resp =
        schemaService.getSchema(schemaFullName).aggregate().join().contentUtf8();
      return GetNamespaceResponse.builder()
        .withNamespace(Namespace.of(catalogName, schemaName))
        .setProperties(JsonUtils.getInstance().readValue(resp, SchemaInfo.class).getProperties())
        .build();
    }

    // Else it's an invalid parameter
    throw new IllegalArgumentException();
  }

  // Table APIs

  @Head("/v1/namespaces/{namespace}/tables/{table}")
  public HttpResponse tableExists(@Param("namespace") String namespace,
                                  @Param("table") String table) {
    List<String> namespaceParts = Splitter.on(".").splitToList(namespace);
    if (namespaceParts.size() != 2) {
      throw new IllegalArgumentException("invalid namespace " + namespace);
    }
    String catalog = namespaceParts.get(0);
    String schema = namespaceParts.get(1);
    try (Session session = sessionFactory.openSession()) {
      tableRepository.getTable(namespace + "." + table);
      String metadataLocation =
        tableRepository.getTableUniformMetadataLocation(session, catalog, schema, table);
      if (metadataLocation == null) {
        return HttpResponse.of(HttpStatus.NOT_FOUND);
      } else {
        return HttpResponse.of(HttpStatus.OK);
      }
    }
  }

  @Get("/v1/namespaces/{namespace}/tables/{table}")
  @ProducesJson
  public LoadTableResponse loadTable(@Param("namespace") String namespace,
                                     @Param("table") String table) throws IOException {
    List<String> namespaceParts = Splitter.on(".").splitToList(namespace);
    if (namespaceParts.size() != 2) {
      throw new IllegalArgumentException("invalid namespace " + namespace);
    }
    String catalog = namespaceParts.get(0);
    String schema = namespaceParts.get(1);
    String metadataLocation;
    try (Session session = sessionFactory.openSession()) {
      tableRepository.getTable(namespace + "." + table);
      metadataLocation =
        tableRepository.getTableUniformMetadataLocation(session, catalog, schema, table);
    }

    if (metadataLocation == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Table not found: " + namespace + "." + table);
    }

    String metadataJson = new String(Files.readAllBytes(Paths.get(URI.create(metadataLocation))));
    TableMetadata tableMetadata = TableMetadataParser.fromJson(metadataLocation, metadataJson);

    return LoadTableResponse.builder()
      .withTableMetadata(tableMetadata)
      .build();
  }

  @Post("/v1/namespaces/{namespace}/tables/{table}/metrics")
  public HttpResponse reportMetrics(@Param("namespace") String namespace,
                                    @Param("table") String table) {
    return HttpResponse.of(HttpStatus.OK);
  }

  @Get("/v1/namespaces/{namespace}/tables")
  @ProducesJson
  public org.apache.iceberg.rest.responses.ListTablesResponse listTables(
    @Param("namespace") String namespace)
    throws JsonProcessingException {
    List<String> namespaceParts = Splitter.on(".").splitToList(namespace);
    if (namespaceParts.size() != 2) {
      throw new IllegalArgumentException("invalid namespace " + namespace);
    }
    String catalog = namespaceParts.get(0);
    String schema = namespaceParts.get(1);
    AggregatedHttpResponse resp =
      tableService.listTables(catalog, schema, Optional.of(Integer.MAX_VALUE), Optional.empty(),
        Optional.empty(), Optional.empty()).aggregate().join();
    List<TableIdentifier> filteredTables;
    try (Session session = sessionFactory.openSession()) {
      filteredTables = Objects.requireNonNull(JsonUtils.getInstance()
          .readValue(resp.contentUtf8(), ListTablesResponse.class)
          .getTables())
        .stream()
        .filter(tableInfo -> {
          String metadataLocation =
            tableRepository.getTableUniformMetadataLocation(session, catalog, schema,
              tableInfo.getName());
          return metadataLocation != null;
        })
        .map(tableInfo -> TableIdentifier.of(tableInfo.getCatalogName(), tableInfo.getSchemaName(),
          tableInfo.getName()))
        .collect(Collectors.toList());
    }

    return org.apache.iceberg.rest.responses.ListTablesResponse
      .builder()
      .addAll(filteredTables)
      .build();
  }

}
