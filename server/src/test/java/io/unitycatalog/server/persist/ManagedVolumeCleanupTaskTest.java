package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.util.SafeCloseable;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateCatalog;
import io.unitycatalog.server.model.CreateSchema;
import io.unitycatalog.server.model.CreateVolumeRequestContent;
import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.model.VolumeType;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
import io.unitycatalog.server.persist.dao.VolumeInfoDAO;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.ServerProperties;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ManagedVolumeCleanupTaskTest {
  @TempDir Path tempDir;
  private SessionFactory sessionFactory;
  private Repositories repositories;
  private SafeCloseable requestContext;

  @BeforeEach
  void setUp() {
    requestContext = ServiceRequestContext.of(HttpRequest.of(HttpMethod.POST, "/")).push();
    Properties properties = new Properties();
    properties.setProperty("server.env", "test");
    ServerProperties serverProperties = new ServerProperties(properties);
    Properties hibernateProperties =
        HibernateConfigurator.setupHibernateProperties(serverProperties);
    hibernateProperties.setProperty("hibernate.connection.url", "jdbc:h2:mem:" + UUID.randomUUID());
    sessionFactory = new HibernateConfigurator(hibernateProperties).getSessionFactory();
    repositories = new Repositories(sessionFactory, serverProperties);
    repositories.getCatalogRepository().addCatalog(new CreateCatalog().name("catalog"));
  }

  @AfterEach
  void tearDown() {
    try {
      sessionFactory.close();
    } finally {
      requestContext.close();
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "local",
        "s3://bucket/root",
        "gs://bucket/root",
        "abfs://container@account.dfs.core.windows.net/root",
        "abfss://container@account.dfs.core.windows.net/root"
      })
  void managedDropQueuesStorageWithoutDeletingIt(String root) throws Exception {
    createSchema(root.equals("local") ? tempDir.toUri().toString() : root);
    VolumeInfo volume = createVolume("managed", VolumeType.MANAGED, null);
    Path marker = null;
    if (root.equals("local")) {
      marker = Path.of(URI.create(volume.getStorageLocation())).resolve("data.txt");
      Files.createDirectories(marker.getParent());
      Files.writeString(marker, "retained data");
    }

    repositories.getVolumeRepository().deleteVolume(volume.getFullName());

    assertDroppedWithTask(volume);
    assertThat(
            repositories
                .getStorageCleanupTaskRepository()
                .claim(Duration.ofHours(2), Duration.ofDays(7)))
        .isEmpty();
    if (marker != null) {
      assertThat(Files.readString(marker)).isEqualTo("retained data");
    }
  }

  @Test
  void externalDropKeepsFilesAndDoesNotQueueCleanup() throws Exception {
    createSchema(tempDir.resolve("managed").toUri().toString());
    Path external = Files.createDirectories(tempDir.resolve("external"));
    Path marker = Files.writeString(external.resolve("data.txt"), "external data");
    VolumeInfo volume = createVolume("external", VolumeType.EXTERNAL, external.toUri().toString());

    repositories.getVolumeRepository().deleteVolume(volume.getFullName());

    assertThat(findVolume(volume)).isNull();
    assertThat(findTask(volume)).isNull();
    assertThat(Files.readString(marker)).isEqualTo("external data");
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema", "catalog"})
  void forcedParentDropQueuesManagedVolumesOnly(String parent) {
    createSchema(tempDir.resolve("managed").toUri().toString());
    VolumeInfo managed = createVolume("managed", VolumeType.MANAGED, null);
    VolumeInfo external =
        createVolume(
            "external", VolumeType.EXTERNAL, tempDir.resolve("external").toUri().toString());

    if (parent.equals("schema")) {
      repositories.getSchemaRepository().deleteSchema("catalog.schema", true);
    } else {
      repositories.getCatalogRepository().deleteCatalog("catalog", true);
    }

    assertDroppedWithTask(managed);
    assertThat(findVolume(external)).isNull();
    assertThat(findTask(external)).isNull();
  }

  @Test
  void rejectedParentDropDoesNotQueueCleanup() {
    createSchema(tempDir.toUri().toString());
    VolumeInfo volume = createVolume("managed", VolumeType.MANAGED, null);

    assertThatThrownBy(
            () -> repositories.getSchemaRepository().deleteSchema("catalog.schema", false))
        .isInstanceOf(BaseException.class)
        .extracting(exception -> ((BaseException) exception).getErrorCode())
        .isEqualTo(ErrorCode.FAILED_PRECONDITION);
    assertThat(findVolume(volume)).isNotNull();
    assertThat(findTask(volume)).isNull();
  }

  @Test
  void taskInsertFailureRollsBackVolumeDeletion() throws Exception {
    createSchema(tempDir.toUri().toString());
    VolumeInfo volume = createVolume("managed", VolumeType.MANAGED, null);
    Path directory = Files.createDirectories(Path.of(URI.create(volume.getStorageLocation())));
    Path marker = Files.writeString(directory.resolve("data.txt"), "retained data");
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          repositories
              .getStorageCleanupTaskRepository()
              .create(
                  session,
                  ResourceType.VOLUME,
                  UUID.fromString(volume.getVolumeId()),
                  "existing task",
                  volume.getStorageLocation());
          return null;
        },
        "Failed to create conflicting task",
        /* readOnly= */ false);

    assertThatThrownBy(() -> repositories.getVolumeRepository().deleteVolume(volume.getFullName()))
        .isInstanceOf(RuntimeException.class);

    assertThat(findVolume(volume)).isNotNull();
    assertThat(findTask(volume).getName()).isEqualTo("existing task");
    assertThat(Files.readString(marker)).isEqualTo("retained data");
  }

  private void createSchema(String root) {
    repositories
        .getSchemaRepository()
        .createSchema(new CreateSchema().catalogName("catalog").name("schema").storageRoot(root));
  }

  private VolumeInfo createVolume(String name, VolumeType type, String location) {
    return repositories
        .getVolumeRepository()
        .createVolume(
            new CreateVolumeRequestContent()
                .catalogName("catalog")
                .schemaName("schema")
                .name(name)
                .volumeType(type)
                .storageLocation(location));
  }

  private void assertDroppedWithTask(VolumeInfo volume) {
    assertThat(findVolume(volume)).isNull();
    StorageCleanupTaskDAO task = findTask(volume);
    assertThat(task).isNotNull();
    assertThat(task.getId().toString()).isEqualTo(volume.getVolumeId());
    assertThat(task.getName()).isEqualTo(volume.getName());
    assertThat(task.getResourceType()).isEqualTo(ResourceType.VOLUME);
    assertThat(task.getStorageLocation()).isEqualTo(volume.getStorageLocation());
    assertThat(task.getDeletedAt()).isNotNull();
  }

  private VolumeInfoDAO findVolume(VolumeInfo volume) {
    try (var session = sessionFactory.openSession()) {
      return session.get(VolumeInfoDAO.class, UUID.fromString(volume.getVolumeId()));
    }
  }

  private StorageCleanupTaskDAO findTask(VolumeInfo volume) {
    try (var session = sessionFactory.openSession()) {
      return session.get(StorageCleanupTaskDAO.class, UUID.fromString(volume.getVolumeId()));
    }
  }
}
