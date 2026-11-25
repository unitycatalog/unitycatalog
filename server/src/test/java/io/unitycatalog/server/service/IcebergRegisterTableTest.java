package io.unitycatalog.server.service;

import static io.unitycatalog.server.utils.TestUtils.assertIcebergApiException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import io.unitycatalog.client.model.CreateTable;
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
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.dao.VolumeInfoDAO;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lombok.SneakyThrows;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
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
    fileOperations = spy(operations);
    return fileOperations;
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
    awaitStorageCleanup(id, directory);
    assertThat(directory).doesNotExist();
    assertThat(sibling).hasContent("keep");
    assertThat(
            StorageCleanupTestSupport.findTask(hibernateConfigurator.getSessionFactory(), id)
                .getCompletedAt())
        .isNotNull();
    Path recreated = writeMetadata(directory);
    assertIcebergApiException(
        () -> client.registerTable(CATALOG, NAMESPACE, request("reused", recreated.toString())),
        400,
        "retired registered directory");
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void icebergClientCanRegisterAndCommit(int formatVersion) throws Exception {
    var source =
        IcebergTestUtils.createTable(
            externalRoot.resolve("client_table"),
            SCHEMA,
            Map.of(TableProperties.FORMAT_VERSION, Integer.toString(formatVersion)));
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
    if (formatVersion == 1) {
      String original = Files.readString(file);
      ObjectNode json = (ObjectNode) new ObjectMapper().readTree(original);
      json.remove("table-uuid");
      String withoutUuid = json.toString();
      Files.writeString(file, withoutUuid);
      assertIcebergApiException(
          () -> client.registerTable(CATALOG, NAMESPACE, request("client_table", file.toString())),
          400,
          "must contain table-uuid");
      assertThat(file).hasContent(withoutUuid);
      try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
        assertThat(session.createSelectionQuery("FROM TableInfoDAO", TableInfoDAO.class).list())
            .isEmpty();
      }
      Files.writeString(file, original);
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
      TableMetadata committed =
          client.loadTable(CATALOG, NAMESPACE, "client_table").tableMetadata();
      assertThat(committed.formatVersion()).isEqualTo(formatVersion);
      assertThat(committed.properties()).containsEntry("registered", "true");
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
  public void rejectsRegistrationContainingDefaultModelStorage() throws Exception {
    Path directory = externalRoot.resolve("model_root");
    Path file = writeMetadata(directory);
    assertIcebergApiException(
        () -> client.registerTable(CATALOG, NAMESPACE, request("orders", file.toString())),
        400,
        "must not contain or equal a shared managed storage directory");
    assertThat(client.listTables(CATALOG, NAMESPACE).identifiers()).isEmpty();
    assertThat(file).exists();
    RegisteredModelsApi models = new RegisteredModelsApi(TestUtils.createApiClient(serverConfig));
    var model =
        models.createRegisteredModel(
            new CreateRegisteredModel().catalogName(CATALOG).schemaName(NAMESPACE).name("model"));
    assertThat(model.getStorageLocation())
        .startsWith(NormalizedURL.from(directory.toUri()) + "/__unitystorage/models/");
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
    for (String segment : List.of("tables", "volumes", "models", "catalogs", "schemas")) {
      Path shared = testDirectoryRoot.resolve("__unitystorage").resolve(segment);
      for (Path directory : List.of(shared, shared.resolve(UUID.randomUUID().toString()))) {
        Path file = writeMetadata(directory);
        assertIcebergApiException(
            () -> client.registerTable(CATALOG, NAMESPACE, request("shared", file.toString())),
            400,
            "shared managed storage directory");
      }
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
  @ValueSource(strings = {"table", "volume"})
  public void nativeAllocationRejectsNoncanonicalLocalUrls(String kind) throws Exception {
    String alias = externalRoot.resolve("alias").toUri().toString().replace("alias", "%61lias");
    assertThatThrownBy(() -> createNativeExternal(kind, alias))
        .isInstanceOfSatisfying(
            ApiException.class,
            e -> {
              assertThat(e.getCode()).isEqualTo(400);
              assertThat(e.getResponseBody()).contains("INVALID_ARGUMENT", "unneeded escapes");
            });
    assertThat(client.listTables(CATALOG, NAMESPACE).identifiers()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"table", "volume"})
  public void registrationRejectsLegacyLocalAliases(String kind) throws Exception {
    Path directory = externalRoot.resolve("legacy");
    String canonical = NormalizedURL.from(directory.toUri()).toString();
    UUID id = createNativeExternal(kind, canonical);
    // Model a URL accepted by an earlier server; new allocations reject this spelling.
    String stored = canonical.replace("legacy", "l%65gacy");
    hibernateConfigurator
        .getSessionFactory()
        .inTransaction(
            session -> {
              if (kind.equals("table")) {
                session.get(TableInfoDAO.class, id).setUrl(stored);
              } else {
                session.get(VolumeInfoDAO.class, id).setStorageLocation(stored);
              }
            });
    Path file = writeMetadata(directory);
    assertIcebergApiException(
        () -> client.registerTable(CATALOG, NAMESPACE, request("imported", file.toString())),
        400,
        "stored local URL");
    Path sibling = writeMetadata(externalRoot.resolve("legacy-sibling"));
    client.registerTable(CATALOG, NAMESPACE, request("sibling", sibling.toString()));
    assertThat(file).exists();
  }

  private UUID createNativeExternal(String kind, String location) throws ApiException {
    if (kind.equals("table")) {
      return UUID.fromString(
          tables
              .createTable(
                  new CreateTable()
                      .catalogName(CATALOG)
                      .schemaName(NAMESPACE)
                      .name("native")
                      .tableType(TableType.EXTERNAL)
                      .dataSourceFormat(DataSourceFormat.PARQUET)
                      .storageLocation(location))
              .getTableId());
    }
    return UUID.fromString(
        new VolumesApi(TestUtils.createApiClient(serverConfig))
            .createVolume(
                new CreateVolumeRequestContent()
                    .catalogName(CATALOG)
                    .schemaName(NAMESPACE)
                    .name("native")
                    .volumeType(VolumeType.EXTERNAL)
                    .storageLocation(location))
            .getVolumeId());
  }

  @ParameterizedTest
  @ValueSource(strings = {"table", "volume"})
  public void registrationRejectsCaseAliasesOfOccupiedLocalStorage(String kind) throws Exception {
    Path directory = externalRoot.resolve("CaseTable");
    Path file = writeMetadata(directory);
    Path alias = externalRoot.resolve("casetable");
    assumeTrue(
        Files.exists(alias) && Files.isSameFile(directory, alias),
        "Requires a case-insensitive filesystem");
    createNativeExternal(kind, directory.toUri().toString());
    byte[] original = Files.readAllBytes(file);
    assertIcebergApiException(
        () ->
            client.registerTable(
                CATALOG,
                NAMESPACE,
                request("imported", alias.resolve("metadata/v1.metadata.json").toString())),
        400,
        "stored local URL");
    assertThat(Files.readAllBytes(file)).isEqualTo(original);
    assertThat(client.listTables(CATALOG, NAMESPACE).identifiers())
        .doesNotContain(TableIdentifier.of(NAMESPACE, "imported"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "register",
        "table",
        "staging",
        "external-staging",
        "volume",
        "catalog",
        "schema",
        "model"
      })
  public void allocationWaitsForRegistrationStorageLock(String operation) throws Exception {
    Path original = writeMetadata(externalRoot.resolve("first_table"));
    client.registerTable(CATALOG, NAMESPACE, request("first", original.toString()));
    UUID firstId =
        UUID.fromString(
            tables.getTable(CATALOG + "." + NAMESPACE + ".first", null, null, null).getTableId());
    Path directory =
        operation.equals("staging")
            ? testDirectoryRoot.resolve("__unitystorage")
            : externalRoot.resolve(operation.equals("model") ? "model_root" : "orders");
    Path file = writeMetadata(directory);
    var api = TestUtils.createApiClient(serverConfig);
    int status =
        runBlockedByHeldWrite(
            session -> {
              ExternalLocationUtils.lockStorageAllocation(session);
              TableInfoDAO table = session.get(TableInfoDAO.class, firstId);
              table.setUrl(NormalizedURL.from(directory.toUri()).toString());
              table.setIcebergMetadataLocation(NormalizedURL.from(file.toUri()).toString());
            },
            () -> {
              try {
                switch (operation) {
                  case "table" ->
                      client.createTable(
                          CATALOG,
                          NAMESPACE,
                          CreateTableRequest.builder()
                              .withName("second")
                              .withSchema(SCHEMA)
                              .withLocation(directory.toString())
                              .build());
                  case "staging", "external-staging" ->
                      client.createTable(
                          CATALOG,
                          NAMESPACE,
                          CreateTableRequest.builder()
                              .withName("second")
                              .withSchema(SCHEMA)
                              .withLocation(
                                  operation.equals("staging") ? null : directory.toString())
                              .stageCreate()
                              .build());
                  case "volume" ->
                      new VolumesApi(api)
                          .createVolume(
                              new CreateVolumeRequestContent()
                                  .catalogName(CATALOG)
                                  .schemaName(NAMESPACE)
                                  .name("second")
                                  .volumeType(VolumeType.EXTERNAL)
                                  .storageLocation(directory.toString()));
                  case "catalog" ->
                      catalogs.createCatalog(
                          new CreateCatalog().name("second").storageRoot(directory.toString()));
                  case "schema" ->
                      schemas.createSchema(
                          new CreateSchema()
                              .catalogName(CATALOG)
                              .name("second")
                              .storageRoot(directory.toString()));
                  case "model" ->
                      new RegisteredModelsApi(api)
                          .createRegisteredModel(
                              new CreateRegisteredModel()
                                  .catalogName(CATALOG)
                                  .schemaName(NAMESPACE)
                                  .name("second"));
                  default ->
                      client.registerTable(CATALOG, NAMESPACE, request("second", file.toString()));
                }
                return 200;
              } catch (ApiException e) {
                assertThat(e.getResponseBody()).contains("overlap");
                return e.getCode();
              }
            });
    assertThat(status).isEqualTo(400);
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      long count = 0;
      for (String entity :
          List.of(
              "TableInfoDAO",
              "VolumeInfoDAO",
              "SchemaInfoDAO",
              "CatalogInfoDAO",
              "RegisteredModelInfoDAO",
              "StagingTableDAO")) {
        count +=
            session
                .createSelectionQuery(
                    "SELECT count(*) FROM " + entity + " WHERE name IN ('first', 'second')",
                    Long.class)
                .getSingleResult();
      }
      assertThat(count).isEqualTo(1);
      assertThat(session.get(TableInfoDAO.class, firstId).getUrl())
          .isEqualTo(NormalizedURL.from(directory.toUri()).toString());
    }
    assertThat(file).exists();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void externalCreatesRejectRegisteredStorageBeforeWriting(boolean retired)
      throws Exception {
    Path registered = externalRoot.resolve("registered");
    Path original = writeMetadata(registered);
    client.registerTable(CATALOG, NAMESPACE, request("registered", original.toString()));
    if (retired) {
      UUID id =
          UUID.fromString(
              tables
                  .getTable(CATALOG + "." + NAMESPACE + ".registered", null, null, null)
                  .getTableId());
      client.dropTable(CATALOG, NAMESPACE, "registered");
      awaitStorageCleanup(id, registered);
      original = writeMetadata(registered);
    }
    byte[] contents = Files.readAllBytes(original);
    AtomicInteger outputOpens = new AtomicInteger();
    doAnswer(
            invocation -> {
              FileIO fileIO = spy((FileIO) invocation.callRealMethod());
              doAnswer(
                      output -> {
                        outputOpens.incrementAndGet();
                        return output.callRealMethod();
                      })
                  .when(fileIO)
                  .newOutputFile(any(String.class));
              return fileIO;
            })
        .when(fileOperations)
        .getFileIO(any(), any());
    for (boolean stage : List.of(false, true)) {
      var builder =
          CreateTableRequest.builder()
              .withName("overlap")
              .withSchema(SCHEMA)
              .withLocation(registered.toString());
      if (stage) {
        builder.stageCreate();
      }
      assertIcebergApiException(
          () -> client.createTable(CATALOG, NAMESPACE, builder.build()), 400, "overlap");
      assertThat(Files.readAllBytes(original)).isEqualTo(contents);
      assertThat(registered.resolve("data")).doesNotExist();
      assertThat(outputOpens.get()).as("metadata output files opened").isZero();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"staging", "model"})
  public void registrationRejectsExistingStagingAndModelDirectories(String kind) throws Exception {
    Path directory;
    if (kind.equals("staging")) {
      directory =
          Path.of(
              NormalizedURL.from(
                      client
                          .createTable(
                              CATALOG,
                              NAMESPACE,
                              CreateTableRequest.builder()
                                  .withName("staged")
                                  .withSchema(SCHEMA)
                                  .stageCreate()
                                  .build())
                          .tableMetadata()
                          .location())
                  .toUri());
    } else {
      var model =
          new RegisteredModelsApi(TestUtils.createApiClient(serverConfig))
              .createRegisteredModel(
                  new CreateRegisteredModel()
                      .catalogName(CATALOG)
                      .schemaName(NAMESPACE)
                      .name("model"));
      directory = Path.of(NormalizedURL.from(model.getStorageLocation()).toUri());
    }
    Path file = writeMetadata(directory);
    byte[] contents = Files.readAllBytes(file);
    assertIcebergApiException(
        () -> client.registerTable(CATALOG, NAMESPACE, request("imported", file.toString())),
        400,
        "overlap");
    assertThat(Files.readAllBytes(file)).isEqualTo(contents);
  }

  @ParameterizedTest
  @ValueSource(strings = {"catalog", "schema"})
  public void forcedNamespaceDeleteQueuesRegisteredDirectoryCleanup(String kind) throws Exception {
    Path directory = externalRoot.resolve("orders");
    Path file = writeMetadata(directory);
    client.registerTable(CATALOG, NAMESPACE, request("orders", file.toString()));
    UUID id =
        UUID.fromString(
            tables.getTable(CATALOG + "." + NAMESPACE + ".orders", null, null, null).getTableId());
    if (kind.equals("catalog")) {
      catalogs.deleteCatalog(CATALOG, true);
    } else {
      schemas.deleteSchema(CATALOG + "." + NAMESPACE, true);
    }
    var task = StorageCleanupTestSupport.findTask(hibernateConfigurator.getSessionFactory(), id);
    assertThat(task.getResourceType()).isEqualTo(ManagedResourceType.REGISTERED_TABLE);
    assertThat(task.getStorageLocation())
        .isEqualTo(NormalizedURL.from(directory.toUri()).toString());
    assertThat(file).exists();
  }

  @Test
  public void externalDropPreservesFilesForRegistration() throws Exception {
    Path directory = externalRoot.resolve("external_table");
    client.createTable(
        CATALOG,
        NAMESPACE,
        CreateTableRequest.builder()
            .withName("external_table")
            .withSchema(SCHEMA)
            .withLocation(directory.toString())
            .build());
    String pointer = client.loadTable(CATALOG, NAMESPACE, "external_table").metadataLocation();
    Path file = Path.of(NormalizedURL.from(pointer).toUri());
    byte[] contents = Files.readAllBytes(file);
    client.dropTable(CATALOG, NAMESPACE, "external_table");
    client.registerTable(CATALOG, NAMESPACE, request("imported", pointer));
    assertThat(Files.readAllBytes(file)).isEqualTo(contents);
    assertThat(
            tables
                .getTable(CATALOG + "." + NAMESPACE + ".imported", null, null, null)
                .getTableType())
        .isEqualTo(TableType.MANAGED);
  }

  @Test
  public void externalLocationDeleteWaitsForRegistrationAndSeesItsUsage() throws Exception {
    Path directory = externalRoot.resolve("orders");
    Path file = writeMetadata(directory);
    ExternalLocationsApi locations =
        new ExternalLocationsApi(TestUtils.createApiClient(serverConfig));
    UUID locationId = UUID.fromString(locations.getExternalLocation("warehouse", null).getId());
    UUID schemaId =
        UUID.fromString(schemas.getSchema(CATALOG + "." + NAMESPACE, null).getSchemaId());
    int status =
        runBlockedByHeldWrite(
            session -> {
              io.unitycatalog.server.persist.utils.RepositoryUtils.getForUpdate(
                  session, ExternalLocationDAO.class, locationId);
              TableInfoDAO table =
                  TableInfoDAO.builder()
                      .id(UUID.randomUUID())
                      .name("registered")
                      .schemaId(schemaId)
                      .type("MANAGED")
                      .dataSourceFormat("ICEBERG")
                      .url(NormalizedURL.from(directory.toUri()).toString())
                      .icebergMetadataLocation(NormalizedURL.from(file.toUri()).toString())
                      .build();
              session.persist(table);
            },
            () -> {
              try {
                locations.deleteExternalLocation("warehouse", false);
                return 200;
              } catch (ApiException e) {
                assertThat(e.getResponseBody()).contains("still used by");
                return e.getCode();
              }
            });
    assertThat(status).isEqualTo(400);
    assertThat(locations.getExternalLocation("warehouse", null).getId())
        .isEqualTo(locationId.toString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void nativeManagedIcebergDropRemovesCleanupTask(boolean staged) throws Exception {
    var builder = CreateTableRequest.builder().withName("native_table").withSchema(SCHEMA);
    if (staged) {
      builder.stageCreate();
    }
    TableMetadata metadata =
        client.createTable(CATALOG, NAMESPACE, builder.build()).tableMetadata();
    if (staged) {
      client.updateTable(
          CATALOG,
          NAMESPACE,
          "native_table",
          new UpdateTableRequest(
              List.of(new UpdateRequirement.AssertTableDoesNotExist()),
              List.of(
                  new MetadataUpdate.AssignUUID(metadata.uuid()),
                  new MetadataUpdate.UpgradeFormatVersion(metadata.formatVersion()),
                  new MetadataUpdate.AddSchema(metadata.schema()),
                  new MetadataUpdate.SetCurrentSchema(-1),
                  new MetadataUpdate.AddPartitionSpec(metadata.spec()),
                  new MetadataUpdate.SetDefaultPartitionSpec(-1),
                  new MetadataUpdate.AddSortOrder(metadata.sortOrder()),
                  new MetadataUpdate.SetDefaultSortOrder(-1),
                  new MetadataUpdate.SetLocation(metadata.location()))));
    }
    UUID id =
        UUID.fromString(
            tables
                .getTable(CATALOG + "." + NAMESPACE + ".native_table", null, null, null)
                .getTableId());
    client.dropTable(CATALOG, NAMESPACE, "native_table");
    var task = StorageCleanupTestSupport.findTask(hibernateConfigurator.getSessionFactory(), id);
    assertThat(task.getResourceType()).isEqualTo(ManagedResourceType.TABLE);
    awaitStorageCleanup(id, Path.of(NormalizedURL.from(metadata.location()).toUri()));
    assertThat(StorageCleanupTestSupport.findTask(hibernateConfigurator.getSessionFactory(), id))
        .isNull();
  }

  private static RegisterTableRequest request(String name, String location) {
    return ImmutableRegisterTableRequest.builder().name(name).metadataLocation(location).build();
  }

  private void awaitStorageCleanup(UUID id, Path directory) throws Exception {
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
          hibernateConfigurator.getSessionFactory(), id, Duration.ofSeconds(10), directory);
    }
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
