package io.unitycatalog.server.cleanup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.model.VolumeType;
import io.unitycatalog.server.persist.ManagedResourceType;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.dao.VolumeInfoDAO;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.persist.utils.InterruptiblePrefixOperations;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.CooperativeDeadline;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Integration coverage for the storage cleanup worker's claim/delete/finish cycle: dropping a
 * managed table or volume queues a cleanup task, and the worker reclaims the object prefix while
 * leaving siblings untouched. The local case runs against a real temp directory through the
 * server's own {@link FileOperations}; the S3 cases run against an in-process S3-compatible server
 * (s3mock), stubbing only the {@code getCleanupFileIO} seam to hand {@link S3FileIO} the s3mock
 * client (credential vending has no storage-endpoint override yet). The repositories, the DB task,
 * and the worker itself are real throughout.
 */
@ExtendWith(S3MockExtension.class)
class StorageCleanupWorkerE2ETest {
  private static final String CATALOG = "catalog";
  private static final String SCHEMA = "schema";

  @RegisterExtension
  public static final S3MockExtension S3_MOCK = S3MockExtension.builder().silent().build();

  @TempDir Path tempDir;

  private final S3Client s3 = S3_MOCK.createS3ClientV2();
  private final String bucket = "cleanup-e2e-" + UUID.randomUUID();

  private SessionFactory sessionFactory;
  private Repositories repositories;
  private UUID schemaId;

  @BeforeEach
  void setUp() {
    ServerProperties serverProperties = testServerProperties();
    Properties hibernateProperties =
        HibernateConfigurator.setupHibernateProperties(serverProperties);
    hibernateProperties.setProperty(
        "hibernate.connection.url", "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
    sessionFactory = new HibernateConfigurator(hibernateProperties).getSessionFactory();
    repositories = new Repositories(sessionFactory, serverProperties, null);
    schemaId = seedNamespace();
  }

  @AfterEach
  void tearDown() {
    sessionFactory.close();
  }

  @Test
  void workerDeletesDroppedManagedVolumeFromLocalStorage() throws Exception {
    UUID volumeId = UUID.randomUUID();
    Path volumeDir = tempDir.resolve("root/volumes").resolve(volumeId.toString());
    Path marker = volumeDir.resolve("data/marker.txt");
    Files.createDirectories(marker.getParent());
    Files.writeString(marker, "x");
    seedManagedVolume("vol", volumeId, volumeDir.toString());

    repositories.getVolumeRepository().deleteVolume(CATALOG + "." + SCHEMA + ".vol");

    assertThat(localWorker().runOnce()).isTrue();
    assertThat(Files.exists(volumeDir)).isFalse();
    assertThat(findTask(volumeId)).isNull();
  }

  @Test
  void workerDeletesDroppedManagedVolumeFromS3() {
    s3.createBucket(b -> b.bucket(bucket));
    UUID volumeId = UUID.randomUUID();
    String location = "s3://" + bucket + "/root/volumes/" + volumeId;
    seedManagedVolume("vol", volumeId, location);
    putObjects("root/volumes/" + volumeId, "data/part-0", "data/part-1", "_meta/index");
    s3.putObject(b -> b.bucket(bucket).key("root/volumes/other/keep"), RequestBody.fromString("x"));
    // Shares the volume id as a raw key prefix but is NOT under "<id>/": guards against a
    // regression that deletes on ".../volumes/<id>" without the trailing slash and so over-deletes
    // an adjacent volume's storage.
    s3.putObject(
        b -> b.bucket(bucket).key("root/volumes/" + volumeId + "-sibling/keep"),
        RequestBody.fromString("x"));

    repositories.getVolumeRepository().deleteVolume(CATALOG + "." + SCHEMA + ".vol");

    assertThat(s3BackedWorker().runOnce()).isTrue();
    assertThat(remainingKeys())
        .containsExactlyInAnyOrder(
            "root/volumes/other/keep", "root/volumes/" + volumeId + "-sibling/keep");
    assertThat(findTask(volumeId)).isNull();
  }

  @Test
  void workerCompletesWhenManagedVolumeStorageIsAlreadyEmpty() {
    s3.createBucket(b -> b.bucket(bucket));
    UUID volumeId = UUID.randomUUID();
    String location = "s3://" + bucket + "/root/volumes/" + volumeId;
    seedManagedVolume("vol", volumeId, location);
    // No objects under the volume prefix: the storage was already reclaimed (e.g. a retried or
    // duplicate task), so cleanup must still finish cleanly rather than fail on an empty prefix.
    s3.putObject(b -> b.bucket(bucket).key("root/volumes/other/keep"), RequestBody.fromString("x"));

    repositories.getVolumeRepository().deleteVolume(CATALOG + "." + SCHEMA + ".vol");

    assertThat(s3BackedWorker().runOnce()).isTrue();
    assertThat(remainingKeys()).containsExactly("root/volumes/other/keep");
    assertThat(findTask(volumeId)).isNull();
  }

  @Test
  void workerDeletesDroppedManagedTableFromLocalStorage() throws Exception {
    UUID tableId = UUID.randomUUID();
    Path tableDir = tempDir.resolve("root/tables").resolve(tableId.toString());
    Path marker = tableDir.resolve("data/marker.txt");
    Files.createDirectories(marker.getParent());
    Files.writeString(marker, "x");
    seedManagedTable("tbl", tableId, tableDir.toString());

    repositories.getTableRepository().deleteTable(CATALOG, SCHEMA, "tbl");

    assertThat(localWorker().runOnce()).isTrue();
    assertThat(Files.exists(tableDir)).isFalse();
    assertThat(findTask(tableId)).isNull();
  }

  @Test
  void workerCompletesWhenManagedTableStorageIsAlreadyEmpty() {
    s3.createBucket(b -> b.bucket(bucket));
    UUID tableId = UUID.randomUUID();
    String location = "s3://" + bucket + "/root/tables/" + tableId;
    seedManagedTable("tbl", tableId, location);
    // No objects under the table prefix: the storage was already reclaimed (e.g. a retried or
    // duplicate task), so cleanup must still finish cleanly rather than fail on an empty prefix.
    s3.putObject(b -> b.bucket(bucket).key("root/tables/other/keep"), RequestBody.fromString("x"));

    repositories.getTableRepository().deleteTable(CATALOG, SCHEMA, "tbl");

    assertThat(s3BackedWorker().runOnce()).isTrue();
    assertThat(remainingKeys()).containsExactly("root/tables/other/keep");
    assertThat(findTask(tableId)).isNull();
  }

  @Test
  void workerDeletesDroppedManagedTableFromS3() {
    s3.createBucket(b -> b.bucket(bucket));
    UUID tableId = UUID.randomUUID();
    String location = "s3://" + bucket + "/root/tables/" + tableId;
    seedManagedTable("tbl", tableId, location);
    putObjects("root/tables/" + tableId, "_delta_log/00000000000000000000.json", "part-0.parquet");
    s3.putObject(b -> b.bucket(bucket).key("root/tables/other/keep"), RequestBody.fromString("x"));
    // Shares the table id as a raw key prefix but is NOT under "<id>/": guards against a
    // regression that deletes on ".../tables/<id>" without the trailing slash and so over-deletes
    // an adjacent table's storage.
    s3.putObject(
        b -> b.bucket(bucket).key("root/tables/" + tableId + "-sibling/keep"),
        RequestBody.fromString("x"));

    repositories.getTableRepository().deleteTable(CATALOG, SCHEMA, "tbl");

    assertThat(s3BackedWorker().runOnce()).isTrue();
    assertThat(remainingKeys())
        .containsExactlyInAnyOrder(
            "root/tables/other/keep", "root/tables/" + tableId + "-sibling/keep");
    assertThat(findTask(tableId)).isNull();
  }

  @ParameterizedTest(name = "nested model cleanup: modelFirst={0}")
  @ValueSource(booleans = {true, false})
  void workerCompletesNestedModelCleanupInEitherOrder(boolean modelFirst) throws Exception {
    UUID modelId = UUID.randomUUID();
    UUID versionId = UUID.randomUUID();
    Path modelsDir =
        tempDir.resolve("root").resolve(ManagedResourceType.REGISTERED_MODEL.pathSegment());
    Path modelDir = modelsDir.resolve(modelId.toString());
    Path versionDir =
        modelDir
            .resolve(ManagedResourceType.MODEL_VERSION.pathSegment())
            .resolve(versionId.toString());
    Path siblingDir = modelsDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(versionDir);
    Files.createDirectories(siblingDir);
    Path modelMarker = Files.writeString(modelDir.resolve("model.txt"), "model data");
    Files.writeString(versionDir.resolve("version.txt"), "version data");
    Path siblingMarker = Files.writeString(siblingDir.resolve("keep.txt"), "sibling data");

    // Distinct, eligible timestamps make claim order deterministic without sleeps.
    persist(
        StorageCleanupTaskDAO.builder()
            .id(modelId)
            .name("model")
            .resourceType(ManagedResourceType.REGISTERED_MODEL)
            .storageLocation(modelDir.toUri().toString())
            .deletedAt(new Date(modelFirst ? 1 : 2))
            .build());
    persist(
        StorageCleanupTaskDAO.builder()
            .id(versionId)
            .name("1")
            .resourceType(ManagedResourceType.MODEL_VERSION)
            .storageLocation(versionDir.toUri().toString())
            .deletedAt(new Date(modelFirst ? 2 : 1))
            .build());

    try (StorageCleanupWorker worker = localWorker()) {
      assertThat(worker.runOnce()).isTrue();
      assertThat(findTask(modelFirst ? modelId : versionId)).isNull();
      StorageCleanupTaskDAO pending = findTask(modelFirst ? versionId : modelId);
      assertThat(pending).isNotNull();
      assertThat(pending.getFailureCount()).isZero();
      assertThat(pending.getLeaseToken()).isNull();
      assertThat(versionDir).doesNotExist();
      if (modelFirst) {
        assertThat(modelDir).doesNotExist();
      } else {
        assertThat(Files.readString(modelMarker)).isEqualTo("model data");
      }
      assertThat(Files.readString(siblingMarker)).isEqualTo("sibling data");

      // Each task must finish on its first attempt, even if its directory is already gone.
      assertThat(worker.runOnce()).isTrue();
      assertThat(findTask(modelId)).isNull();
      assertThat(findTask(versionId)).isNull();
      assertThat(modelDir).doesNotExist();
      assertThat(Files.readString(siblingMarker)).isEqualTo("sibling data");
      assertThat(worker.runOnce()).isFalse();
    }
  }

  /** Real FileOperations: local (file://) cleanup resolves to SimpleLocalFileIO, no credentials. */
  private StorageCleanupWorker localWorker() {
    return new StorageCleanupWorker(
        repositories.getStorageCleanupTaskRepository(),
        repositories.getFileOperations(),
        Clock.systemUTC(),
        workerProperties());
  }

  /** Stubbed FileOperations that points the cleanup S3FileIO at the s3mock client. */
  private StorageCleanupWorker s3BackedWorker() {
    FileOperations fileOperations = mock(FileOperations.class);
    when(fileOperations.getCleanupFileIO(any(), any()))
        .thenAnswer(
            invocation -> {
              NormalizedURL location = invocation.getArgument(0);
              return new InterruptiblePrefixOperations(
                  new S3FileIO(() -> s3), location + "/", CooperativeDeadline.NO_DEADLINE);
            });
    return new StorageCleanupWorker(
        repositories.getStorageCleanupTaskRepository(),
        fileOperations,
        Clock.systemUTC(),
        workerProperties());
  }

  private void putObjects(String prefix, String... suffixes) {
    for (String suffix : suffixes) {
      String key = prefix + "/" + suffix;
      s3.putObject(b -> b.bucket(bucket).key(key), RequestBody.fromString("x"));
    }
  }

  private List<String> remainingKeys() {
    return s3.listObjectsV2(b -> b.bucket(bucket)).contents().stream().map(o -> o.key()).toList();
  }

  private StorageCleanupTaskDAO findTask(UUID resourceId) {
    return StorageCleanupTestSupport.findTask(sessionFactory, resourceId);
  }

  private UUID seedNamespace() {
    UUID catalogId = UUID.randomUUID();
    UUID newSchemaId = UUID.randomUUID();
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          session.persist(
              CatalogInfoDAO.builder().id(catalogId).name(CATALOG).createdAt(new Date()).build());
          session.persist(
              SchemaInfoDAO.builder()
                  .id(newSchemaId)
                  .catalogId(catalogId)
                  .name(SCHEMA)
                  .createdAt(new Date())
                  .build());
          return null;
        },
        "seed namespace",
        /* readOnly= */ false);
    return newSchemaId;
  }

  private void seedManagedVolume(String name, UUID volumeId, String location) {
    persist(
        VolumeInfoDAO.builder()
            .id(volumeId)
            .schemaId(schemaId)
            .name(name)
            .volumeType(VolumeType.MANAGED.getValue())
            .storageLocation(location)
            .createdAt(new Date())
            .build());
  }

  private void seedManagedTable(String name, UUID tableId, String location) {
    persist(
        TableInfoDAO.builder()
            .id(tableId)
            .schemaId(schemaId)
            .name(name)
            .type(TableType.MANAGED.getValue())
            .dataSourceFormat(DataSourceFormat.DELTA.getValue())
            .url(location)
            .createdAt(new Date())
            .build());
  }

  private void persist(Object dao) {
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          session.persist(dao);
          return null;
        },
        "seed resource",
        /* readOnly= */ false);
  }

  private static ServerProperties testServerProperties() {
    Properties properties = new Properties();
    properties.setProperty("server.env", "test");
    return new ServerProperties(properties);
  }

  private static ServerProperties workerProperties() {
    ServerProperties serverProperties = mock(ServerProperties.class);
    when(serverProperties.getStorageCleanupPollInterval()).thenReturn(Duration.ofMinutes(1));
    when(serverProperties.getStorageCleanupLeaseDuration()).thenReturn(Duration.ofMinutes(5));
    when(serverProperties.getStorageCleanupAttemptTimeout()).thenReturn(Duration.ofSeconds(20));
    when(serverProperties.getStorageCleanupInitialDelay()).thenReturn(Duration.ZERO);
    when(serverProperties.getStorageCleanupRetryBackoff()).thenReturn(Duration.ofMinutes(1));
    return serverProperties;
  }
}
