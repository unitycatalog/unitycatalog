package io.unitycatalog.server.service;

import static io.unitycatalog.server.utils.TestUtils.assertIcebergApiException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.VolumesApi;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.cleanup.StorageCleanupTestSupport;
import io.unitycatalog.server.cleanup.StorageCleanupWorker;
import io.unitycatalog.server.observability.UnityCatalogMetrics;
import io.unitycatalog.server.persist.ManagedResourceType;
import io.unitycatalog.server.persist.StorageCleanupTaskRepository;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.types.Types;
import org.hibernate.Session;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** In-place registration, persisted state, overlap exclusion, and managed storage cleanup. */
public class IcebergRegisterTableTest extends BaseServerTest {
  private static final String CATALOG = TestUtils.CATALOG_NAME;
  private static final String NAMESPACE = TestUtils.SCHEMA_NAME;
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  private IcebergRestClient client;
  private CatalogsApi catalogs;
  private SchemasApi schemas;
  private TablesApi tables;
  private Path externalRoot;
  private FileOperations fileOperations;

  @Override
  protected FileOperations decorateFileOperations(FileOperations operations) {
    fileOperations = operations;
    return operations;
  }

  @BeforeEach
  @Override
  @SneakyThrows
  public void setUp() {
    super.setUp();
    var api = TestUtils.createApiClient(serverConfig);
    catalogs = new CatalogsApi(api);
    schemas = new SchemasApi(api);
    tables = new TablesApi(api);
    client = new IcebergRestClient(serverConfig);
    catalogs.createCatalog(new CreateCatalog().name(CATALOG));
    schemas.createSchema(new CreateSchema().catalogName(CATALOG).name(NAMESPACE));
    externalRoot = testDirectoryRoot.resolve("external");
    new CredentialsApi(api)
        .createCredential(
            new CreateCredentialRequest()
                .name("storage")
                .purpose(CredentialPurpose.STORAGE)
                .awsIamRole(
                    new AwsIamRoleRequest().roleArn("arn:aws:iam::123456789012:role/storage")));
    new ExternalLocationsApi(api)
        .createExternalLocation(
            new CreateExternalLocation()
                .name("warehouse")
                .url(externalRoot.toUri().toString())
                .credentialName("storage"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"external", "catalog", "schema", "default"})
  public void registersAndDropsManagedTableInPlace(String storage) throws Exception {
    String catalog = CATALOG;
    String namespace = NAMESPACE;
    Path root = externalRoot;
    if (storage.equals("catalog")) {
      catalog = "managed_catalog";
      var created =
          catalogs.createCatalog(
              new CreateCatalog().name(catalog).storageRoot(root.toUri().toString()));
      root = Path.of(NormalizedURL.from(created.getStorageLocation()).toUri());
      schemas.createSchema(new CreateSchema().catalogName(catalog).name(namespace));
    } else if (storage.equals("schema")) {
      namespace = "managed_schema";
      var created =
          schemas.createSchema(
              new CreateSchema()
                  .catalogName(catalog)
                  .name(namespace)
                  .storageRoot(root.toUri().toString()));
      root = Path.of(NormalizedURL.from(created.getStorageLocation()).toUri());
    } else if (storage.equals("default")) {
      root = testDirectoryRoot.resolve("__unitystorage");
    }
    Path directory = root.resolve("orders");
    Path file = directory.resolve("metadata/v1.metadata.json");
    Files.createDirectories(file.getParent());
    String json;
    try (var fixture =
        Objects.requireNonNull(
            getClass().getResourceAsStream("/iceberg.metadata.two-snapshots.json"))) {
      json =
          new String(fixture.readAllBytes(), StandardCharsets.UTF_8)
              .replace(
                  "file:/tmp/uniform_iceberg_table",
                  NormalizedURL.from(directory.toUri()).toString());
    }
    Files.writeString(file, json);
    Path sibling = root.resolve("orders-sibling/keep");
    Files.createDirectories(sibling.getParent());
    Files.writeString(sibling, "keep");

    var response =
        client.registerTable(
            catalog,
            namespace,
            request(
                "orders", storage.equals("external") ? file.toString() : file.toUri().toString()));
    assertThat(response.metadataLocation()).isEqualTo(file.toString());
    assertThat(response.tableMetadata().snapshots()).hasSize(2);
    assertThat(response.tableMetadata().previousFiles()).hasSize(2);
    assertThat(response.tableMetadata().uuid()).isEqualTo("55d4dc69-5b14-4483-bfc8-f33b80f99f99");
    assertThat(Files.readString(file)).isEqualTo(json);
    try (var files = Files.list(file.getParent())) {
      assertThat(files.toList()).containsExactly(file);
    }
    var table = tables.getTable(catalog + "." + namespace + ".orders", null, null, null);
    assertThat(table.getTableType()).isEqualTo(TableType.MANAGED);
    assertThat(table.getDataSourceFormat()).isEqualTo(DataSourceFormat.ICEBERG);
    assertThat(table.getColumns()).extracting(column -> column.getName()).containsExactly("x", "y");
    assertThat(table.getProperties()).containsAllEntriesOf(response.tableMetadata().properties());
    UUID id = UUID.fromString(table.getTableId());
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      TableInfoDAO row = session.get(TableInfoDAO.class, id);
      assertThat(row.getIcebergMetadataLocation())
          .isEqualTo(NormalizedURL.from(file.toUri()).toString());
      assertThat(row.getUrl()).isEqualTo(NormalizedURL.from(directory.toUri()).toString());
    }
    assertThat(client.listTables(catalog, namespace).identifiers())
        .containsExactly(TableIdentifier.of(namespace, "orders"));
    assertThat(client.loadTable(catalog, namespace, "orders").metadataLocation())
        .isEqualTo(file.toString());
    client.dropTable(catalog, namespace, "orders");
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      var task = session.get(StorageCleanupTaskDAO.class, id);
      assertThat(task.getResourceType()).isEqualTo(ManagedResourceType.REGISTERED_TABLE);
      assertThat(task.getStorageLocation())
          .isEqualTo(NormalizedURL.from(directory.toUri()).toString());
    }
    Properties cleanupProperties = new Properties();
    cleanupProperties.putAll(serverProperties);
    StorageCleanupTestSupport.configureFastCleanup(cleanupProperties);
    try (var worker =
        new StorageCleanupWorker(
            new StorageCleanupTaskRepository(hibernateConfigurator.getSessionFactory()),
            fileOperations,
            Clock.systemUTC(),
            new ServerProperties(cleanupProperties),
            UnityCatalogMetrics.noop())) {
      worker.start();
      StorageCleanupTestSupport.awaitCleanup(
          hibernateConfigurator.getSessionFactory(), id, Duration.ofSeconds(5), directory);
    }
    assertThat(directory).doesNotExist();
    assertThat(sibling).hasContent("keep");
  }

  @Test
  public void icebergClientCanRegisterAndCommit() throws Exception {
    Path file = writeMetadata(externalRoot.resolve("client_table"));
    try (RESTCatalog catalog = new RESTCatalog()) {
      catalog.initialize(
          "uc",
          Map.of(
              CatalogProperties.URI,
              serverConfig.getServerUrl() + "/api/2.1/unity-catalog/iceberg",
              CatalogProperties.WAREHOUSE_LOCATION,
              CATALOG,
              OAuth2Properties.TOKEN,
              serverConfig.getAuthToken()));
      var table =
          catalog.registerTable(TableIdentifier.of(NAMESPACE, "client_table"), file.toString());
      table.updateProperties().set("registered", "true").commit();
      assertThat(client.loadTable(CATALOG, NAMESPACE, "client_table").tableMetadata().properties())
          .containsEntry("registered", "true");
    }
    assertThat(file).exists();
  }

  @Test
  public void rejectsDuplicateNamesOverlapsAndOverwrite() throws Exception {
    Path directory = externalRoot.resolve("orders");
    Path file = writeMetadata(directory);
    RegisterTableRequest request = request("orders", file.toString());
    client.registerTable(CATALOG, NAMESPACE, request);
    assertIcebergApiException(
        () -> client.registerTable(CATALOG, NAMESPACE, request), 409, "already exists");
    assertIcebergApiException(
        () ->
            client.registerTable(
                CATALOG,
                NAMESPACE,
                ImmutableRegisterTableRequest.builder().from(request).overwrite(true).build()),
        400,
        "overwrite=true");
    for (Path overlap : List.of(directory, directory.resolve("child"), externalRoot)) {
      Path overlapFile = writeMetadata(overlap);
      assertIcebergApiException(
          () ->
              client.registerTable(CATALOG, NAMESPACE, request("overlap", overlapFile.toString())),
          400,
          overlap.equals(externalRoot) ? "must be a subpath" : "overlaps");
    }
    assertIcebergApiException(
        () ->
            client.createTable(
                CATALOG,
                NAMESPACE,
                CreateTableRequest.builder()
                    .withName("external_alias")
                    .withSchema(SCHEMA)
                    .withLocation(directory.toString())
                    .build()),
        400,
        "overlaps");
    client.dropTable(CATALOG, NAMESPACE, "orders");
    assertIcebergApiException(
        () -> client.registerTable(CATALOG, NAMESPACE, request), 400, "pending storage cleanup");
    assertIcebergApiException(
        () ->
            client.createTable(
                CATALOG,
                NAMESPACE,
                CreateTableRequest.builder()
                    .withName("pending_alias")
                    .withSchema(SCHEMA)
                    .withLocation(directory.toString())
                    .build()),
        400,
        "pending storage cleanup");
    assertThat(file).exists();
  }

  @Test
  public void rejectsVolumeOverlapBeforeAndAfterRegistration() throws Exception {
    VolumesApi volumes = new VolumesApi(TestUtils.createApiClient(serverConfig));
    Path directory = externalRoot.resolve("volume");
    var volume =
        new CreateVolumeRequestContent()
            .catalogName(CATALOG)
            .schemaName(NAMESPACE)
            .name("volume")
            .volumeType(VolumeType.EXTERNAL)
            .storageLocation(directory.toString());
    volumes.createVolume(volume);
    Path file = writeMetadata(directory);
    assertIcebergApiException(
        () -> client.registerTable(CATALOG, NAMESPACE, request("table", file.toString())),
        400,
        "overlaps");
    volumes.deleteVolume(CATALOG + "." + NAMESPACE + ".volume");
    client.registerTable(CATALOG, NAMESPACE, request("table", file.toString()));
    assertThatThrownBy(() -> volumes.createVolume(volume))
        .isInstanceOfSatisfying(
            ApiException.class,
            e -> {
              assertThat(e.getCode()).isEqualTo(400);
              assertThat(e.getResponseBody()).contains("INVALID_ARGUMENT", "overlaps");
            });
  }

  @Test
  public void rejectsUnknownStorageAndOtherManagedNamespaces() throws Exception {
    Path unknown = writeMetadata(testDirectoryRoot.resolve("unregistered/orders"));
    assertIcebergApiException(
        () -> client.registerTable(CATALOG, NAMESPACE, request("unknown", unknown.toString())),
        403,
        "must be inside an external location");
    var other =
        schemas.createSchema(
            new CreateSchema()
                .catalogName(CATALOG)
                .name("other")
                .storageRoot(externalRoot.toUri().toString()));
    Path file =
        writeMetadata(
            Path.of(NormalizedURL.from(other.getStorageLocation()).toUri()).resolve("orders"));
    assertIcebergApiException(
        () -> client.registerTable(CATALOG, NAMESPACE, request("other", file.toString())),
        403,
        "another catalog or schema");
    assertIcebergApiException(
        () -> client.registerTable(CATALOG, "missing", request("other", file.toString())),
        404,
        "Schema not found");
    assertThat(client.listTables(CATALOG, NAMESPACE).identifiers()).isEmpty();
  }

  @Test
  public void rejectsExistingExternalTableAndSharedManagedDirectories() throws Exception {
    Path parent = externalRoot.resolve("existing");
    Path occupied = parent.resolve("table");
    client.createTable(
        CATALOG,
        NAMESPACE,
        CreateTableRequest.builder()
            .withName("existing")
            .withSchema(SCHEMA)
            .withLocation(occupied.toString())
            .build());
    for (Path overlap : List.of(parent, occupied, occupied.resolve("child"))) {
      Path file = writeMetadata(overlap);
      assertIcebergApiException(
          () -> client.registerTable(CATALOG, NAMESPACE, request("overlap", file.toString())),
          400,
          "overlaps");
    }
    for (String segment : List.of("tables", "volumes", "models")) {
      Path file = writeMetadata(testDirectoryRoot.resolve("__unitystorage").resolve(segment));
      assertIcebergApiException(
          () -> client.registerTable(CATALOG, NAMESPACE, request("shared", file.toString())),
          400,
          "shared managed storage directory");
    }
    assertThat(client.listTables(CATALOG, NAMESPACE).identifiers())
        .containsExactly(TableIdentifier.of(NAMESPACE, "existing"));
  }

  @Test
  public void rejectsMissingInvalidAndOutOfBoundaryMetadata() throws Exception {
    Path directory = externalRoot.resolve("orders");
    Path file = directory.resolve("metadata/v1.metadata.json");
    Files.createDirectories(file.getParent());
    RegisterTableRequest request = request("orders", file.toString());
    assertIcebergApiException(
        () -> client.registerTable(CATALOG, NAMESPACE, request),
        400,
        "Cannot read Iceberg metadata");
    for (String json : List.of("{", "{}")) {
      Files.writeString(file, json);
      assertIcebergApiException(
          () -> client.registerTable(CATALOG, NAMESPACE, request),
          400,
          "Cannot read Iceberg metadata");
    }
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            testDirectoryRoot.resolve("outside").toUri().toString(),
            Map.of());
    Files.writeString(file, TableMetadataParser.toJson(metadata));
    assertIcebergApiException(
        () -> client.registerTable(CATALOG, NAMESPACE, request), 400, "must be a subpath");
    assertThat(client.listTables(CATALOG, NAMESPACE).identifiers()).isEmpty();
    writeMetadata(directory);
    assertIcebergApiException(
        () ->
            client.registerTable(
                CATALOG,
                NAMESPACE,
                request("alias", file.toUri().toString().replace("v1.metadata", "%761.metadata"))),
        400,
        "unneeded escapes");
    client.registerTable(CATALOG, NAMESPACE, request);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void concurrentRegistrationClaimsOneDirectory(boolean createExternalTable)
      throws Exception {
    Path file = writeMetadata(externalRoot.resolve("orders"));
    var barrier = new CyclicBarrier(2);
    var executor = Executors.newFixedThreadPool(2);
    try {
      var results =
          List.of("first", "second").stream()
              .map(
                  name ->
                      executor.submit(
                          () -> {
                            barrier.await(30, TimeUnit.SECONDS);
                            try {
                              if (createExternalTable && name.equals("second")) {
                                client.createTable(
                                    CATALOG,
                                    NAMESPACE,
                                    CreateTableRequest.builder()
                                        .withName(name)
                                        .withSchema(SCHEMA)
                                        .withLocation(file.getParent().getParent().toString())
                                        .build());
                              } else {
                                client.registerTable(
                                    CATALOG, NAMESPACE, request(name, file.toString()));
                              }
                              return 200;
                            } catch (ApiException e) {
                              return e.getCode();
                            }
                          }))
              .toList();
      assertThat(
              List.of(
                  results.get(0).get(30, TimeUnit.SECONDS),
                  results.get(1).get(30, TimeUnit.SECONDS)))
          .containsExactlyInAnyOrder(200, 400);
    } finally {
      executor.shutdownNow();
    }
    assertThat(client.listTables(CATALOG, NAMESPACE).identifiers()).hasSize(1);
    assertThat(file).exists();
  }

  private static RegisterTableRequest request(String name, String location) {
    return ImmutableRegisterTableRequest.builder().name(name).metadataLocation(location).build();
  }

  private static Path writeMetadata(Path directory) throws Exception {
    Path file = directory.resolve("metadata/v1.metadata.json");
    Files.createDirectories(file.getParent());
    Files.writeString(
        file,
        TableMetadataParser.toJson(
            TableMetadata.newTableMetadata(
                SCHEMA,
                PartitionSpec.unpartitioned(),
                NormalizedURL.from(directory.toUri()).toString(),
                Map.of("test", "value"))));
    return file;
  }
}
