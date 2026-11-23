package io.unitycatalog.server.service;

import static io.unitycatalog.server.utils.TestUtils.assertIcebergApiException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.api.RegisteredModelsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.VolumesApi;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CreateRegisteredModel;
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
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.SimpleLocalFileIO;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.IcebergTestUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.SneakyThrows;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTSessionCatalog;
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
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(
        ServerProperties.Property.MODEL_STORAGE_ROOT.getKey(),
        testDirectoryRoot.resolve("external/model_root").toString());
  }

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
    var source = IcebergTestUtils.createTable(directory, SCHEMA);
    TableMetadata original;
    try (var ignored = source.io()) {
      for (long id : List.of(10L, 20L)) {
        GenericRecord record = GenericRecord.create(SCHEMA);
        record.setField("id", id);
        source
            .newFastAppend()
            .appendFile(IcebergTestUtils.writeDataFile(source, "record" + id, record))
            .commit();
      }
      original = source.operations().current();
    }
    Path file = Path.of(NormalizedURL.from(original.metadataFileLocation()).toUri());
    String json = Files.readString(file);
    List<Path> originalFiles;
    try (var files = Files.list(file.getParent())) {
      originalFiles = files.toList();
    }
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
    assertThat(response.tableMetadata().uuid()).isEqualTo(original.uuid());
    assertThat(Files.readString(file)).isEqualTo(json);
    try (var files = Files.list(file.getParent())) {
      assertThat(files.toList()).containsExactlyInAnyOrderElementsOf(originalFiles);
    }
    var table = tables.getTable(catalog + "." + namespace + ".orders", null, null, null);
    assertThat(table.getTableType()).isEqualTo(TableType.MANAGED);
    assertThat(table.getDataSourceFormat()).isEqualTo(DataSourceFormat.ICEBERG);
    assertThat(table.getColumns()).extracting(column -> column.getName()).containsExactly("id");
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
    var source = IcebergTestUtils.createTable(externalRoot.resolve("client_table"), SCHEMA);
    Path file;
    try (var ignored = source.io()) {
      GenericRecord record = GenericRecord.create(SCHEMA);
      record.setField("id", 42L);
      source
          .newFastAppend()
          .appendFile(IcebergTestUtils.writeDataFile(source, "record", record))
          .commit();
      file =
          Path.of(NormalizedURL.from(source.operations().current().metadataFileLocation()).toUri());
    }
    try (RESTCatalog catalog =
        new RESTCatalog() {
          @Override
          protected RESTSessionCatalog newSessionCatalog(
              Function<Map<String, String>, RESTClient> clientBuilder) {
            return new RESTSessionCatalog(
                clientBuilder, (context, properties) -> new SimpleLocalFileIO(externalRoot));
          }
        }) {
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
      try (var tasks = table.newScan().planFiles()) {
        var files = tasks.iterator();
        assertThat(files.hasNext()).isTrue();
        var task = files.next();
        try (var rows =
            Avro.read(table.io().newInputFile(task.file().location()))
                .project(table.schema())
                .createReaderFunc(DataReader::create)
                .<GenericRecord>build()) {
          assertThat(rows).extracting(row -> row.getField("id")).containsExactly(42L);
        }
        assertThat(files.hasNext()).isFalse();
      }
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

  @ParameterizedTest
  @ValueSource(strings = {"catalog", "schema"})
  public void rejectsNamespaceAllocationInRegisteredStorage(String kind) throws Exception {
    Path directory = externalRoot.resolve("orders");
    Path file = writeMetadata(directory);
    client.registerTable(CATALOG, NAMESPACE, request("orders", file.toString()));
    for (boolean dropped : List.of(false, true)) {
      if (dropped) {
        client.dropTable(CATALOG, NAMESPACE, "orders");
      }
      assertThatThrownBy(
              () -> {
                if (kind.equals("catalog")) {
                  catalogs.createCatalog(
                      new CreateCatalog().name("nested").storageRoot(directory.toString()));
                } else {
                  schemas.createSchema(
                      new CreateSchema()
                          .catalogName(CATALOG)
                          .name("nested")
                          .storageRoot(directory.toString()));
                }
              })
          .isInstanceOfSatisfying(
              ApiException.class,
              e -> {
                assertThat(e.getCode()).isEqualTo(400);
                assertThat(e.getResponseBody()).contains("INVALID_ARGUMENT", "overlaps");
              });
      try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
        assertThat(
                session
                    .createSelectionQuery(
                        "FROM CatalogInfoDAO WHERE name='nested'", CatalogInfoDAO.class)
                    .getResultList())
            .isEmpty();
        assertThat(
                session
                    .createSelectionQuery(
                        "FROM SchemaInfoDAO WHERE name='nested'", SchemaInfoDAO.class)
                    .getResultList())
            .isEmpty();
      }
      assertThat(file).exists();
    }
    // A shared raw root is valid when the assigned namespace directory is disjoint.
    schemas.createSchema(
        new CreateSchema()
            .catalogName(CATALOG)
            .name("sibling")
            .storageRoot(externalRoot.toString()));
    new RegisteredModelsApi(TestUtils.createApiClient(serverConfig))
        .createRegisteredModel(
            new CreateRegisteredModel().catalogName(CATALOG).schemaName("sibling").name("model"));
  }

  @Test
  public void rejectsModelAllocationInRegisteredStorage() throws Exception {
    Path directory = externalRoot.resolve("model_root");
    Path file = writeMetadata(directory);
    client.registerTable(CATALOG, NAMESPACE, request("orders", file.toString()));
    RegisteredModelsApi models = new RegisteredModelsApi(TestUtils.createApiClient(serverConfig));
    for (boolean dropped : List.of(false, true)) {
      if (dropped) {
        client.dropTable(CATALOG, NAMESPACE, "orders");
      }
      assertThatThrownBy(
              () ->
                  models.createRegisteredModel(
                      new CreateRegisteredModel()
                          .catalogName(CATALOG)
                          .schemaName(NAMESPACE)
                          .name("model")))
          .isInstanceOfSatisfying(
              ApiException.class,
              e -> {
                assertThat(e.getCode()).isEqualTo(400);
                assertThat(e.getResponseBody()).contains("INVALID_ARGUMENT", "overlaps");
              });
      assertThat(directory.resolve("__unitystorage")).doesNotExist();
      assertThat(file).exists();
    }
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
  @ValueSource(strings = {"register", "table", "catalog", "schema", "model"})
  public void concurrentRegistrationClaimsOneDirectory(String operation) throws Exception {
    Path file =
        writeMetadata(externalRoot.resolve(operation.equals("model") ? "model_root" : "orders"));
    var models = new RegisteredModelsApi(TestUtils.createApiClient(serverConfig));
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
                              if (operation.equals("table") && name.equals("second")) {
                                client.createTable(
                                    CATALOG,
                                    NAMESPACE,
                                    CreateTableRequest.builder()
                                        .withName(name)
                                        .withSchema(SCHEMA)
                                        .withLocation(file.getParent().getParent().toString())
                                        .build());
                              } else if (operation.equals("catalog") && name.equals("second")) {
                                catalogs.createCatalog(
                                    new CreateCatalog()
                                        .name(name)
                                        .storageRoot(file.getParent().getParent().toString()));
                              } else if (operation.equals("schema") && name.equals("second")) {
                                schemas.createSchema(
                                    new CreateSchema()
                                        .catalogName(CATALOG)
                                        .name(name)
                                        .storageRoot(file.getParent().getParent().toString()));
                              } else if (operation.equals("model") && name.equals("second")) {
                                models.createRegisteredModel(
                                    new CreateRegisteredModel()
                                        .catalogName(CATALOG)
                                        .schemaName(NAMESPACE)
                                        .name(name));
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
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      long count = 0;
      for (String entity :
          List.of("TableInfoDAO", "SchemaInfoDAO", "CatalogInfoDAO", "RegisteredModelInfoDAO")) {
        count +=
            session
                .createSelectionQuery(
                    "SELECT count(*) FROM " + entity + " WHERE name IN ('first', 'second')",
                    Long.class)
                .getSingleResult();
      }
      assertThat(count).isEqualTo(1);
    }
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
