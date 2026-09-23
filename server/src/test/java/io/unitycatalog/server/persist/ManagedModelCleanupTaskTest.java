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
import io.unitycatalog.server.model.CreateModelVersion;
import io.unitycatalog.server.model.CreateRegisteredModel;
import io.unitycatalog.server.model.CreateSchema;
import io.unitycatalog.server.model.ModelVersionInfo;
import io.unitycatalog.server.model.RegisteredModelInfo;
import io.unitycatalog.server.persist.dao.ModelVersionInfoDAO;
import io.unitycatalog.server.persist.dao.RegisteredModelInfoDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
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

class ManagedModelCleanupTaskTest {
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
  void versionThenModelDeletionQueuesTheirManagedPaths(String root) throws Exception {
    RegisteredModelInfo model =
        createModel(root.equals("local") ? tempDir.toUri().toString() : root);
    ModelVersionInfo first = createVersion();
    ModelVersionInfo second = createVersion();
    Path marker = null;
    if (root.equals("local")) {
      marker =
          Files.writeString(
              Path.of(URI.create(first.getStorageLocation())).resolve("model.bin"), "model data");
    }

    repositories.getModelRepository().deleteModelVersion(model.getFullName(), first.getVersion());

    assertThat(find(ModelVersionInfoDAO.class, first.getId())).isNull();
    assertThat(find(ModelVersionInfoDAO.class, second.getId())).isNotNull();
    assertThat(find(RegisteredModelInfoDAO.class, model.getId())).isNotNull();
    assertTask(
        first.getId(),
        first.getVersion().toString(),
        ResourceType.MODEL_VERSION,
        first.getStorageLocation());
    assertThat(find(StorageCleanupTaskDAO.class, model.getId())).isNull();

    repositories.getModelRepository().deleteRegisteredModel(model.getFullName(), true);

    assertThat(find(RegisteredModelInfoDAO.class, model.getId())).isNull();
    assertThat(find(ModelVersionInfoDAO.class, second.getId())).isNull();
    assertTask(
        model.getId(), model.getName(), ResourceType.REGISTERED_MODEL, model.getStorageLocation());
    // Keep the earlier version task: it has its own retention time and may already be leased.
    assertTask(
        first.getId(),
        first.getVersion().toString(),
        ResourceType.MODEL_VERSION,
        first.getStorageLocation());
    assertThat(find(StorageCleanupTaskDAO.class, second.getId())).isNull();
    assertThat(second.getStorageLocation()).startsWith(model.getStorageLocation() + "/versions/");
    assertThat(
            repositories
                .getStorageCleanupTaskRepository()
                .claim(Duration.ofHours(2), Duration.ofDays(7)))
        .isEmpty();
    if (marker != null) {
      assertThat(Files.readString(marker)).isEqualTo("model data");
    }
  }

  @Test
  void emptyModelDeletionQueuesItsDirectory() {
    RegisteredModelInfo model = createModel(tempDir.toUri().toString());

    repositories.getModelRepository().deleteRegisteredModel(model.getFullName(), false);

    assertThat(find(RegisteredModelInfoDAO.class, model.getId())).isNull();
    assertTask(
        model.getId(), model.getName(), ResourceType.REGISTERED_MODEL, model.getStorageLocation());
  }

  @Test
  void rejectedModelDeletionKeepsVersionsAndDoesNotQueueCleanup() {
    RegisteredModelInfo model = createModel(tempDir.toUri().toString());
    ModelVersionInfo version = createVersion();

    assertThatThrownBy(
            () ->
                repositories.getModelRepository().deleteRegisteredModel(model.getFullName(), false))
        .isInstanceOf(BaseException.class)
        .extracting(exception -> ((BaseException) exception).getErrorCode())
        .isEqualTo(ErrorCode.ABORTED);

    assertThat(find(RegisteredModelInfoDAO.class, model.getId())).isNotNull();
    assertThat(find(ModelVersionInfoDAO.class, version.getId())).isNotNull();
    assertThat(find(StorageCleanupTaskDAO.class, model.getId())).isNull();
    assertThat(find(StorageCleanupTaskDAO.class, version.getId())).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema", "catalog"})
  void forcedParentDeletionQueuesModelDirectory(String parent) {
    RegisteredModelInfo model = createModel(tempDir.toUri().toString());
    ModelVersionInfo first = createVersion();
    ModelVersionInfo second = createVersion();

    if (parent.equals("schema")) {
      repositories.getSchemaRepository().deleteSchema("catalog.schema", true);
    } else {
      repositories.getCatalogRepository().deleteCatalog("catalog", true);
    }

    assertThat(find(RegisteredModelInfoDAO.class, model.getId())).isNull();
    assertThat(find(ModelVersionInfoDAO.class, first.getId())).isNull();
    assertThat(find(ModelVersionInfoDAO.class, second.getId())).isNull();
    assertTask(
        model.getId(), model.getName(), ResourceType.REGISTERED_MODEL, model.getStorageLocation());
    assertThat(find(StorageCleanupTaskDAO.class, first.getId())).isNull();
    assertThat(find(StorageCleanupTaskDAO.class, second.getId())).isNull();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void taskInsertFailureRollsBackMetadataDeletion(boolean deleteWholeModel) throws Exception {
    RegisteredModelInfo model = createModel(tempDir.toUri().toString());
    ModelVersionInfo version = createVersion();
    Path marker =
        Files.writeString(
            Path.of(URI.create(version.getStorageLocation())).resolve("model.bin"), "model data");
    String id = deleteWholeModel ? model.getId() : version.getId();
    String location = deleteWholeModel ? model.getStorageLocation() : version.getStorageLocation();
    ResourceType type =
        deleteWholeModel ? ResourceType.REGISTERED_MODEL : ResourceType.MODEL_VERSION;
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          repositories
              .getStorageCleanupTaskRepository()
              .create(session, type, UUID.fromString(id), "existing task", location);
          return null;
        },
        "Failed to create conflicting task",
        /* readOnly= */ false);

    assertThatThrownBy(
            () -> {
              if (deleteWholeModel) {
                repositories.getModelRepository().deleteRegisteredModel(model.getFullName(), true);
              } else {
                repositories
                    .getModelRepository()
                    .deleteModelVersion(model.getFullName(), version.getVersion());
              }
            })
        .isInstanceOf(RuntimeException.class);

    assertThat(find(RegisteredModelInfoDAO.class, model.getId())).isNotNull();
    assertThat(find(ModelVersionInfoDAO.class, version.getId())).isNotNull();
    assertThat(find(StorageCleanupTaskDAO.class, id).getName()).isEqualTo("existing task");
    assertThat(Files.readString(marker)).isEqualTo("model data");
  }

  private RegisteredModelInfo createModel(String root) {
    repositories
        .getSchemaRepository()
        .createSchema(new CreateSchema().catalogName("catalog").name("schema").storageRoot(root));
    return repositories
        .getModelRepository()
        .createRegisteredModel(
            new CreateRegisteredModel().catalogName("catalog").schemaName("schema").name("model"));
  }

  private ModelVersionInfo createVersion() {
    return repositories
        .getModelRepository()
        .createModelVersion(
            new CreateModelVersion()
                .catalogName("catalog")
                .schemaName("schema")
                .modelName("model")
                .source(tempDir.resolve("source").toUri().toString()));
  }

  private void assertTask(String id, String name, ResourceType type, String location) {
    StorageCleanupTaskDAO task = find(StorageCleanupTaskDAO.class, id);
    assertThat(task).isNotNull();
    assertThat(task.getId().toString()).isEqualTo(id);
    assertThat(task.getName()).isEqualTo(name);
    assertThat(task.getResourceType()).isEqualTo(type);
    assertThat(task.getStorageLocation()).isEqualTo(location);
    assertThat(task.getDeletedAt()).isNotNull();
  }

  private <T> T find(Class<T> type, String id) {
    try (var session = sessionFactory.openSession()) {
      return session.get(type, UUID.fromString(id));
    }
  }
}
