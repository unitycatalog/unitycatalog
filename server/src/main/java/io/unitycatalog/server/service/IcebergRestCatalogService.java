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
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.ListCatalogsResponse;
import io.unitycatalog.server.model.ListSchemasResponse;
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import io.unitycatalog.server.service.iceberg.MetadataService;
import io.unitycatalog.server.utils.JsonUtils;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

@ExceptionHandler(IcebergRestExceptionHandler.class)
public class IcebergRestCatalogService {

  private final CatalogService catalogService;
  private final SchemaService schemaService;
  private final TableService tableService;
  private final MetadataService metadataService;
  private final TableRepository tableRepository = TableRepository.getInstance();
  private static final SessionFactory sessionFactory = HibernateUtils.getSessionFactory();

  public IcebergRestCatalogService(
      CatalogService catalogService,
      SchemaService schemaService,
      TableService tableService,
      MetadataService metadataService) {
    this.catalogService = catalogService;
    this.schemaService = schemaService;
    this.tableService = tableService;
    this.metadataService = metadataService;
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
      List<Namespace> namespaces =
          resp.getCatalogs().stream()
              .map(catalogInfo -> Namespace.of(catalogInfo.getName()))
              .collect(Collectors.toList());
      return ListNamespacesResponse.builder().addAll(namespaces).build();
    }

    List<String> parentParts = Splitter.on(".").splitToList(parent.get());

    // If parent is a catalog, then list the schemas
    if (parentParts.size() == 1) {
      String catalogName = parentParts.get(0);
      String respContent =
          schemaService
              .listSchemas(catalogName, Optional.of(Integer.MAX_VALUE), Optional.empty())
              .aggregate()
              .join()
              .contentUtf8();
      ListSchemasResponse resp =
          JsonUtils.getInstance().readValue(respContent, ListSchemasResponse.class);
      assert resp.getSchemas() != null;
      List<Namespace> namespaces =
          resp.getSchemas().stream()
              .map(schemaInfo -> Namespace.of(schemaInfo.getCatalogName(), schemaInfo.getName()))
              .collect(Collectors.toList());
      return ListNamespacesResponse.builder().addAll(namespaces).build();
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
      String resp = schemaService.getSchema(schemaFullName).aggregate().join().contentUtf8();
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
  public HttpResponse tableExists(
      @Param("namespace") String namespace, @Param("table") String table) {
    List<String> namespaceParts = splitTwoPartNamespace(namespace);
    String catalog = namespaceParts.get(0);
    String schema = namespaceParts.get(1);
    try (Session session = sessionFactory.openSession()) {
      tableRepository.getTable(namespace + "." + table);
      String metadataLocation =
          tableRepository.getTableUniformMetadataLocation(session, catalog, schema, table);
      if (metadataLocation == null) {
        throw new NoSuchTableException("Table does not exist: %s", namespace + "." + table);
      } else {
        return HttpResponse.of(HttpStatus.OK);
      }
    }
  }

  @Get("/v1/namespaces/{namespace}/tables/{table}")
  @ProducesJson
  public LoadTableResponse loadTable(
      @Param("namespace") String namespace, @Param("table") String table) throws IOException {
    List<String> namespaceParts = splitTwoPartNamespace(namespace);
    String catalog = namespaceParts.get(0);
    String schema = namespaceParts.get(1);
    String metadataLocation;
    try (Session session = sessionFactory.openSession()) {
      tableRepository.getTable(namespace + "." + table);
      metadataLocation =
          tableRepository.getTableUniformMetadataLocation(session, catalog, schema, table);
    }

    if (metadataLocation == null) {
      throw new NoSuchTableException("Table does not exist: %s", namespace + "." + table);
    }

    TableMetadata tableMetadata = metadataService.readTableMetadata(metadataLocation);

    return LoadTableResponse.builder().withTableMetadata(tableMetadata).build();
  }

  @Get("/v1/namespaces/{namespace}/views/{view}")
  @ProducesJson
  public LoadViewResponse loadView(
      @Param("namespace") String namespace, @Param("view") String view) {
    // this is not supported yet, but Iceberg REST client tries to load
    // a table with given path name and then tries to load a view with that
    // name if it didn't find a table, so for now, let's just return a 404
    // as that should be expected since it didn't find a table with the name
    throw new NoSuchViewException("View does not exist: %s", namespace + "." + view);
  }

  @Post("/v1/namespaces/{namespace}/tables/{table}/metrics")
  public HttpResponse reportMetrics(
      @Param("namespace") String namespace, @Param("table") String table) {
    return HttpResponse.of(HttpStatus.OK);
  }

  @Get("/v1/namespaces/{namespace}/tables")
  @ProducesJson
  public org.apache.iceberg.rest.responses.ListTablesResponse listTables(
      @Param("namespace") String namespace) throws JsonProcessingException {
    List<String> namespaceParts = splitTwoPartNamespace(namespace);
    String catalog = namespaceParts.get(0);
    String schema = namespaceParts.get(1);
    AggregatedHttpResponse resp =
        tableService
            .listTables(
                catalog,
                schema,
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
                            session, catalog, schema, tableInfo.getName());
                    return metadataLocation != null;
                  })
              .map(
                  tableInfo ->
                      TableIdentifier.of(
                          tableInfo.getCatalogName(),
                          tableInfo.getSchemaName(),
                          tableInfo.getName()))
              .collect(Collectors.toList());
    }

    return org.apache.iceberg.rest.responses.ListTablesResponse.builder()
        .addAll(filteredTables)
        .build();
  }

  private List<String> splitTwoPartNamespace(String namespace) {
    List<String> namespaceParts = Splitter.on(".").splitToList(namespace);
    if (namespaceParts.size() != 2) {
      String errMsg = "Invalid two-part namespace " + namespace;
      throw new IllegalArgumentException(errMsg);
    }

    return namespaceParts;
  }
}
