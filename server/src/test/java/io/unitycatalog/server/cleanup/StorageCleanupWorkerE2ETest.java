package io.unitycatalog.server.cleanup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.model.VolumeType;
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

    repositories.getVolumeRepository().deleteVolume(CATALOG + "." + SCHEMA + ".vol");

    assertThat(s3BackedWorker().runOnce()).isTrue();
    assertThat(remainingKeys()).containsExactly("root/volumes/other/keep");
    assertThat(findTask(volumeId)).isNull();
  }

  @Test
  void workerDeletesDroppedManagedTableFromS3() {
    s3.createBucket(b -> b.bucket(bucket));
    UUID tableId = UUID.randomUUID();
    String location = "s3://" + bucket + "/root/tables/" + tableId;
    seedManagedTable("tbl", tableId, location);
    putObjects("root/tables/" + tableId, "_delta_log/00000000000000000000.json", "part-0.parquet");
    s3.putObject(b -> b.bucket(bucket).key("root/tables/other/keep"), RequestBody.fromString("x"));

    repositories.getTableRepository().deleteTable(CATALOG, SCHEMA, "tbl");

    assertThat(s3BackedWorker().runOnce()).isTrue();
    assertThat(remainingKeys()).containsExactly("root/tables/other/keep");
    assertThat(findTask(tableId)).isNull();
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
    try (var session = sessionFactory.openSession()) {
      return session.get(StorageCleanupTaskDAO.class, resourceId);
    }
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
