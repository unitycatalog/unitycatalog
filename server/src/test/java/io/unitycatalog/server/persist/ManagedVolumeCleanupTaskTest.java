package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.cleanup.StorageCleanupTestSupport;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.model.VolumeType;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
import io.unitycatalog.server.persist.dao.VolumeInfoDAO;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ManagedVolumeCleanupTaskTest {
  private static final String CATALOG = "catalog";
  private static final String SCHEMA = "schema";

  @TempDir Path tempDir;

  private SessionFactory sessionFactory;
  private Repositories repositories;
  private UUID schemaId;

  @BeforeEach
  void setUp() {
    Properties properties = new Properties();
    properties.setProperty("server.env", "test");
    ServerProperties serverProperties = new ServerProperties(properties);
    Properties hibernateProperties =
        HibernateConfigurator.setupHibernateProperties(serverProperties);
    hibernateProperties.setProperty(
        "hibernate.connection.url", "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
    sessionFactory = new HibernateConfigurator(hibernateProperties).getSessionFactory();
    repositories = new Repositories(sessionFactory, serverProperties);

    UUID catalogId = UUID.randomUUID();
    schemaId = UUID.randomUUID();
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          session.persist(
              CatalogInfoDAO.builder().id(catalogId).name(CATALOG).createdAt(new Date()).build());
          session.persist(
              SchemaInfoDAO.builder()
                  .id(schemaId)
                  .catalogId(catalogId)
                  .name(SCHEMA)
                  .createdAt(new Date())
                  .build());
          return null;
        },
        "Failed to create test namespace",
        /* readOnly= */ false);
  }

  @AfterEach
  void tearDown() {
    sessionFactory.close();
  }

  @Test
  void managedVolumeDropsCreateCleanupTasksForAllSchemes() {
    VolumeInfoDAO local =
        createVolume(
            "local_volume",
            VolumeType.MANAGED,
            id -> tempDir.resolve("__unitystorage/volumes").resolve(id.toString()).toString());
    Date beforeDrop = new Date();
    repositories.getVolumeRepository().deleteVolume(CATALOG + "." + SCHEMA + ".local_volume");
    assertDroppedWithTask(local, beforeDrop);

    VolumeInfoDAO s3 =
        createVolume(
            "s3_volume", VolumeType.MANAGED, id -> "s3://bucket/root/volumes/" + id + "///");
    beforeDrop = new Date();
    repositories.getVolumeRepository().deleteVolume(CATALOG + "." + SCHEMA + ".s3_volume");
    assertDroppedWithTask(s3, beforeDrop);

    VolumeInfoDAO gcs =
        createVolume("gcs_volume", VolumeType.MANAGED, id -> "gs://bucket/root/volumes/" + id);
    beforeDrop = new Date();
    repositories.getVolumeRepository().deleteVolume(CATALOG + "." + SCHEMA + ".gcs_volume");
    assertDroppedWithTask(gcs, beforeDrop);

    for (String scheme : List.of("abfs", "abfss")) {
      VolumeInfoDAO adls =
          createVolume(
              scheme + "_volume",
              VolumeType.MANAGED,
              id -> scheme + "://container@account.dfs.core.windows.net/root/volumes/" + id);
      beforeDrop = new Date();
      repositories
          .getVolumeRepository()
          .deleteVolume(CATALOG + "." + SCHEMA + "." + adls.getName());
      assertDroppedWithTask(adls, beforeDrop);
    }
  }

  @Test
  void externalVolumeDropsDoNotCreateTasks() {
    VolumeInfoDAO external =
        createVolume(
            "external_volume",
            VolumeType.EXTERNAL,
            id -> tempDir.resolve("external").resolve(id.toString()).toString());

    repositories.getVolumeRepository().deleteVolume(CATALOG + "." + SCHEMA + ".external_volume");

    // findTask == null is the real guard: an external drop queues no cleanup task, so the worker
    // never touches its files. (No synchronous delete happens for any drop, managed or external.)
    assertThat(findVolume(external.getId())).isNull();
    assertThat(findTask(external.getId())).isNull();
  }

  @Test
  void cascadingSchemaDropQueuesCleanupForManagedVolumesOnly() {
    VolumeInfoDAO managed =
        createVolume(
            "managed_cascade",
            VolumeType.MANAGED,
            id -> tempDir.resolve("__unitystorage/volumes").resolve(id.toString()).toString());
    VolumeInfoDAO external =
        createVolume(
            "external_cascade",
            VolumeType.EXTERNAL,
            id -> tempDir.resolve("external").resolve(id.toString()).toString());
    Date beforeDrop = new Date();

    // A force schema drop cascades each child through VolumeRepository.deleteVolume(session, ...),
    // the same entry point a direct drop uses, so managed children still queue a cleanup task and
    // external children still queue none.
    repositories.getSchemaRepository().deleteSchema(CATALOG + "." + SCHEMA, /* force= */ true);

    assertDroppedWithTask(managed, beforeDrop);
    assertThat(findVolume(external.getId())).isNull();
    assertThat(findTask(external.getId())).isNull();
    // Exactly one task: the managed child queued one, the external child queued none. Guards
    // against a future change queueing a second task with a different id (the resource_id primary
    // key only blocks a duplicate id).
    assertThat(allTasks()).hasSize(1);
  }

  @Test
  void taskInsertFailureRollsBackVolumeDeletion() {
    VolumeInfoDAO volume =
        createVolume("rollback_volume", VolumeType.MANAGED, id -> "s3://bucket/root/volumes/" + id);
    String existingTaskLocation = "s3://bucket/existing/volumes/" + volume.getId();
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          repositories
              .getStorageCleanupTaskRepository()
              .create(
                  session,
                  ResourceType.VOLUME,
                  volume.getId(),
                  volume.getName(),
                  existingTaskLocation);
          return null;
        },
        "Failed to create existing cleanup task",
        /* readOnly= */ false);

    assertThatThrownBy(
            () ->
                repositories
                    .getVolumeRepository()
                    .deleteVolume(CATALOG + "." + SCHEMA + ".rollback_volume"))
        .isInstanceOf(BaseException.class);

    // The rollback is what these assertions prove: the volume delete and the duplicate task insert
    // share one transaction, so the insert failure must undo both writes. The volume row is still
    // present, and the pre-existing task keeps its original location (the failed insert, which
    // would
    // have used the volume's own location, left no trace).
    assertThat(findVolume(volume.getId())).isNotNull();
    assertThat(findTask(volume.getId()).getStorageLocation()).isEqualTo(existingTaskLocation);
  }

  private VolumeInfoDAO createVolume(
      String name, VolumeType volumeType, Function<UUID, String> location) {
    UUID volumeId = UUID.randomUUID();
    VolumeInfoDAO volume =
        VolumeInfoDAO.builder()
            .id(volumeId)
            .schemaId(schemaId)
            .name(name)
            .volumeType(volumeType.getValue())
            .storageLocation(location.apply(volumeId))
            .createdAt(new Date())
            .build();
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          session.persist(volume);
          return volume;
        },
        "Failed to create test volume",
        /* readOnly= */ false);
  }

  private void assertDroppedWithTask(VolumeInfoDAO volume, Date beforeDrop) {
    StorageCleanupTaskDAO task = findTask(volume.getId());
    assertThat(findVolume(volume.getId())).isNull();
    assertThat(task).isNotNull();
    assertThat(task.getId()).isEqualTo(volume.getId());
    assertThat(task.getName()).isEqualTo(volume.getName());
    assertThat(task.getResourceType()).isEqualTo(ResourceType.VOLUME);
    assertThat(task.getStorageLocation())
        .isEqualTo(NormalizedURL.normalize(volume.getStorageLocation()));
    assertThat(task.getDeletedAt().getTime())
        .isBetween(beforeDrop.getTime(), System.currentTimeMillis());
  }

  private VolumeInfoDAO findVolume(UUID id) {
    try (var session = sessionFactory.openSession()) {
      return session.get(VolumeInfoDAO.class, id);
    }
  }

  private StorageCleanupTaskDAO findTask(UUID id) {
    return StorageCleanupTestSupport.findTask(sessionFactory, id);
  }

  private List<StorageCleanupTaskDAO> allTasks() {
    return StorageCleanupTestSupport.allTasks(sessionFactory);
  }
}
