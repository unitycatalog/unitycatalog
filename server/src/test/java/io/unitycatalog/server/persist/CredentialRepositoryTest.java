package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.util.SafeCloseable;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsIamRoleRequest;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CreateExternalLocation;
import io.unitycatalog.server.model.CredentialPurpose;
import io.unitycatalog.server.model.ExternalLocationInfo;
import io.unitycatalog.server.model.S3CompatibleRequest;
import io.unitycatalog.server.model.UpdateCredentialRequest;
import io.unitycatalog.server.model.UpdateExternalLocation;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.secret.CredentialSecretStore;
import io.unitycatalog.server.persist.secret.EncryptedCredentialSecretStore;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import jakarta.persistence.LockModeType;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.hibernate.Session;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Checks when credential changes store secret values in their store. */
class CredentialRepositoryTest {
  @TempDatabase private TestDatabase testDatabase;

  private final RecordingSecretStore secretStore = new RecordingSecretStore();
  private CredentialRepository credentialRepository;
  private SafeCloseable requestContext;

  /** Keeps values in memory. */
  private static class RecordingSecretStore implements CredentialSecretStore {
    private final List<String> stored = new ArrayList<>();

    @Override
    public String store(String secret, SecretContext context) {
      stored.add(secret);
      return "test:" + (stored.size() - 1);
    }

    @Override
    public String load(String storedValue, SecretContext context) {
      return stored.get(Integer.parseInt(storedValue.substring("test:".length())));
    }
  }

  @BeforeEach
  void setUp() {
    requestContext = ServiceRequestContext.of(HttpRequest.of(HttpMethod.POST, "/")).push();
    credentialRepository =
        new CredentialRepository(
            /* repositories= */ null,
            testDatabase.sessionFactory(),
            serverProperties(/* isS3CompatibleStorageEnabled= */ true),
            secretStore);
  }

  @AfterEach
  void tearDown() {
    requestContext.close();
  }

  private static ServerProperties serverProperties(boolean isS3CompatibleStorageEnabled) {
    Properties properties = new Properties();
    properties.setProperty(
        ServerProperties.Property.S3_COMPATIBLE_STORAGE_ENABLED.getKey(),
        String.valueOf(isS3CompatibleStorageEnabled));
    return new ServerProperties(properties);
  }

  @Test
  void testS3CompatibleNeedsTheExperimentalFlag() {
    CredentialRepository disabled =
        new CredentialRepository(
            /* repositories= */ null,
            testDatabase.sessionFactory(),
            serverProperties(/* isS3CompatibleStorageEnabled= */ false),
            secretStore);
    AwsIamRoleRequest role = new AwsIamRoleRequest().roleArn("arn:aws:iam::123456789012:role/r");
    disabled.addCredential(
        new CreateCredentialRequest()
            .name("role")
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(role));

    for (ThrowingCallable change :
        List.<ThrowingCallable>of(
            () ->
                disabled.addCredential(
                    new CreateCredentialRequest()
                        .name("credential")
                        .purpose(CredentialPurpose.STORAGE)
                        .s3Compatible(s3Compatible("value"))),
            () ->
                disabled.updateCredential(
                    "role", new UpdateCredentialRequest().s3Compatible(s3Compatible("value"))))) {
      assertThatThrownBy(change)
          .isInstanceOf(BaseException.class)
          .hasMessageContaining("server.s3-compatible-storage.enabled=true")
          .extracting(e -> ((BaseException) e).getErrorCode())
          .isEqualTo(ErrorCode.INVALID_ARGUMENT);
    }
    // Rejected before anything reaches the secret store.
    assertThat(secretStore.stored).isEmpty();
    // Other changes to credentials still work.
    disabled.updateCredential("role", new UpdateCredentialRequest().comment("comment"));
  }

  private static S3CompatibleRequest s3Compatible(String secret) {
    return new S3CompatibleRequest()
        .accessKeyId("key-id")
        .secretAccessKey(secret)
        .s3Endpoint("http://storage.example.com:9000");
  }

  private void createS3Compatible(String name, String secret) {
    credentialRepository.addCredential(
        new CreateCredentialRequest()
            .name(name)
            .purpose(CredentialPurpose.STORAGE)
            .s3Compatible(s3Compatible(secret)));
  }

  private String loadSecret(String name) {
    try (Session session = testDatabase.sessionFactory().openSession()) {
      return credentialRepository.getCredentialDAO(session, name).getS3SecretAccessKey(secretStore);
    }
  }

  @Test
  void testUpdateStoresANewKeyPairAndKeepsItOtherwise() {
    createS3Compatible("credential", "value-0");
    assertThat(loadSecret("credential")).isEqualTo("value-0");

    // A new key pair replaces the stored value.
    credentialRepository.updateCredential(
        "credential", new UpdateCredentialRequest().s3Compatible(s3Compatible("value-1")));
    assertThat(loadSecret("credential")).isEqualTo("value-1");

    // A change that keeps the key pair keeps its value.
    credentialRepository.updateCredential(
        "credential", new UpdateCredentialRequest().newName("renamed").comment("comment"));
    assertThat(loadSecret("renamed")).isEqualTo("value-1");
    assertThat(secretStore.stored).hasSize(2);
  }

  private CredentialRepository repositoryWithKey(Optional<String> key) {
    return new CredentialRepository(
        /* repositories= */ null,
        testDatabase.sessionFactory(),
        serverProperties(/* isS3CompatibleStorageEnabled= */ true),
        new EncryptedCredentialSecretStore(key));
  }

  @Test
  void testUnreadableSecretsAreFoundForAMissingOrChangedKey() {
    Optional<String> key = Optional.of(TestUtils.CREDENTIAL_ENCRYPTION_KEY);
    for (String name : List.of("b_credential", "a_credential")) {
      repositoryWithKey(key)
          .addCredential(
              new CreateCredentialRequest()
                  .name(name)
                  .purpose(CredentialPurpose.STORAGE)
                  .s3Compatible(s3Compatible("value")));
    }
    // An IAM role holds no secret, so it is never reported.
    repositoryWithKey(key)
        .addCredential(
            new CreateCredentialRequest()
                .name("role")
                .purpose(CredentialPurpose.STORAGE)
                .awsIamRole(new AwsIamRoleRequest().roleArn("arn:aws:iam::123456789012:role/r")));

    assertThat(repositoryWithKey(key).findUnreadableSecrets()).isEmpty();
    assertThat(repositoryWithKey(Optional.empty()).findUnreadableSecrets())
        .containsOnlyKeys("a_credential", "b_credential")
        .allSatisfy(
            (name, failure) ->
                assertThat(failure.getMessage()).contains("no server.credential-encryption-key"));
    assertThat(
            repositoryWithKey(Optional.of(Base64.getEncoder().encodeToString(new byte[32])))
                .findUnreadableSecrets())
        .containsOnlyKeys("a_credential", "b_credential")
        .allSatisfy(
            (name, failure) -> assertThat(failure.getMessage()).contains("encrypted with key"));
  }

  @Test
  void testTypeChangeWaitsForALocationChangeOnTheCredential() throws Exception {
    // A location change holds the credential's lock while it adds a gs:// location. A concurrent
    // change to s3_compatible waits for it, then sees the location and is refused.
    credentialRepository.addCredential(
        new CreateCredentialRequest()
            .name("role")
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn("arn:aws:iam::123456789012:role/r")));
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try (Session session = testDatabase.sessionFactory().openSession()) {
      session.beginTransaction();
      CredentialDAO role = credentialRepository.getCredentialDAOForUpdate(session, "role");
      Future<?> typeChange =
          pool.submit(
              () -> {
                try (SafeCloseable ignored =
                    ServiceRequestContext.of(HttpRequest.of(HttpMethod.POST, "/")).push()) {
                  credentialRepository.updateCredential(
                      "role", new UpdateCredentialRequest().s3Compatible(s3Compatible("value")));
                }
              });
      awaitLockWait();
      assertThat(typeChange).isNotDone();
      session.persist(
          ExternalLocationDAO.builder()
              .id(UUID.randomUUID())
              .name("location")
              .url("gs://bucket/path")
              .credentialId(role.getId())
              .build());
      session.getTransaction().commit();
      assertThatThrownBy(() -> typeChange.get(30, TimeUnit.SECONDS))
          .hasCauseInstanceOf(BaseException.class)
          .cause()
          .hasMessageContaining("Cannot make credential 'role' an s3_compatible credential");
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void testLocationUpdateWaitsForATypeChangeOnItsCredential() throws Exception {
    // A type change to s3_compatible holds the credential's lock. A concurrent URL change of a
    // location on that credential waits for it, then sees the new type and is refused.
    Repositories repositories =
        new Repositories(
            testDatabase.sessionFactory(),
            serverProperties(/* isS3CompatibleStorageEnabled= */ true));
    repositories
        .getCredentialRepository()
        .addCredential(
            new CreateCredentialRequest()
                .name("role")
                .purpose(CredentialPurpose.STORAGE)
                .awsIamRole(new AwsIamRoleRequest().roleArn("arn:aws:iam::123456789012:role/r")));
    repositories
        .getExternalLocationRepository()
        .addExternalLocation(
            new CreateExternalLocation()
                .name("location")
                .url("s3://bucket/path")
                .credentialName("role"));
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try (Session session = testDatabase.sessionFactory().openSession()) {
      session.beginTransaction();
      CredentialDAO role =
          repositories.getCredentialRepository().getCredentialDAOForUpdate(session, "role");
      Future<?> locationChange =
          pool.submit(
              () -> {
                try (SafeCloseable ignored =
                    ServiceRequestContext.of(HttpRequest.of(HttpMethod.POST, "/")).push()) {
                  repositories
                      .getExternalLocationRepository()
                      .updateExternalLocation(
                          "location", new UpdateExternalLocation().url("gs://bucket/path"));
                }
              });
      awaitLockWait();
      assertThat(locationChange).isNotDone();
      role.setS3Compatible(
          s3Compatible("value"),
          new EncryptedCredentialSecretStore(Optional.of(TestUtils.CREDENTIAL_ENCRYPTION_KEY)));
      session.merge(role);
      session.getTransaction().commit();
      assertThatThrownBy(() -> locationChange.get(30, TimeUnit.SECONDS))
          .hasCauseInstanceOf(BaseException.class)
          .cause()
          .hasMessageContaining("is an s3_compatible credential");
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void testFailedChangesKeepTheCommittedValue() {
    createS3Compatible("credential", "value-0");

    // The name is taken, so the new credential is not persisted.
    assertThatThrownBy(() -> createS3Compatible("credential", "value-1"))
        .isInstanceOf(BaseException.class);

    // An IAM role credential used by a location outside s3:// cannot become an s3_compatible.
    credentialRepository.addCredential(
        new CreateCredentialRequest()
            .name("role")
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn("arn:aws:iam::123456789012:role/r")));
    try (Session session = testDatabase.sessionFactory().openSession()) {
      CredentialDAO role = credentialRepository.getCredentialDAO(session, "role");
      session.beginTransaction();
      session.persist(
          ExternalLocationDAO.builder()
              .id(UUID.randomUUID())
              .name("location")
              .url("gs://bucket/path")
              .credentialId(role.getId())
              .build());
      session.getTransaction().commit();
    }
    assertThatThrownBy(
            () ->
                credentialRepository.updateCredential(
                    "role", new UpdateCredentialRequest().s3Compatible(s3Compatible("value-2"))))
        .isInstanceOf(BaseException.class);
    // The refused change left the role as it was.
    try (Session session = testDatabase.sessionFactory().openSession()) {
      CredentialDAO role = credentialRepository.getCredentialDAO(session, "role");
      assertThat(role.getCredentialType()).isEqualTo(CredentialDAO.CredentialType.AWS_IAM_ROLE);
      assertThat(role.getAwsIamRoleResponse().getRoleArn())
          .isEqualTo("arn:aws:iam::123456789012:role/r");
    }

    // The committed credential keeps its value.
    assertThat(loadSecret("credential")).isEqualTo("value-0");
  }

  @Test
  void testLocationUpdateDoesNotWriteBackAStaleUrl() throws Exception {
    // A URL change holds the location's lock. A concurrent comment change waits for it, then reads
    // the new URL, so it does not write the old one back.
    Repositories repositories =
        new Repositories(
            testDatabase.sessionFactory(),
            serverProperties(/* isS3CompatibleStorageEnabled= */ true));
    repositories
        .getCredentialRepository()
        .addCredential(
            new CreateCredentialRequest()
                .name("role")
                .purpose(CredentialPurpose.STORAGE)
                .awsIamRole(new AwsIamRoleRequest().roleArn("arn:aws:iam::123456789012:role/r")));
    repositories
        .getExternalLocationRepository()
        .addExternalLocation(
            new CreateExternalLocation()
                .name("location")
                .url("gs://bucket/path")
                .credentialName("role"));
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try (Session session = testDatabase.sessionFactory().openSession()) {
      session.beginTransaction();
      ExternalLocationDAO location =
          session
              .createQuery("FROM ExternalLocationDAO WHERE name = :name", ExternalLocationDAO.class)
              .setParameter("name", "location")
              .setLockMode(LockModeType.PESSIMISTIC_WRITE)
              .getSingleResult();
      Future<?> commentChange =
          pool.submit(
              () -> {
                try (SafeCloseable ignored =
                    ServiceRequestContext.of(HttpRequest.of(HttpMethod.POST, "/")).push()) {
                  repositories
                      .getExternalLocationRepository()
                      .updateExternalLocation(
                          "location", new UpdateExternalLocation().comment("comment"));
                }
              });
      awaitLockWait();
      assertThat(commentChange).isNotDone();
      location.setUrl("s3://bucket/path");
      session.getTransaction().commit();
      commentChange.get(30, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }
    assertThat(repositories.getExternalLocationRepository().getExternalLocation("location"))
        .returns("s3://bucket/path", ExternalLocationInfo::getUrl)
        .returns("comment", ExternalLocationInfo::getComment);
  }

  @Test
  void testDeleteWaitsForALocationChangeOnTheCredential() throws Exception {
    // A location change holds the credential's lock while it adds a location. A concurrent delete
    // waits for it, then sees the location and is refused.
    credentialRepository.addCredential(
        new CreateCredentialRequest()
            .name("role")
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn("arn:aws:iam::123456789012:role/r")));
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try (Session session = testDatabase.sessionFactory().openSession()) {
      session.beginTransaction();
      CredentialDAO role = credentialRepository.getCredentialDAOForUpdate(session, "role");
      Future<?> delete =
          pool.submit(() -> credentialRepository.deleteCredential("role", /* force= */ false));
      awaitLockWait();
      assertThat(delete).isNotDone();
      session.persist(
          ExternalLocationDAO.builder()
              .id(UUID.randomUUID())
              .name("location")
              .url("s3://bucket/path")
              .credentialId(role.getId())
              .build());
      session.getTransaction().commit();
      assertThatThrownBy(() -> delete.get(30, TimeUnit.SECONDS))
          .hasCauseInstanceOf(BaseException.class)
          .cause()
          .hasMessageContaining("Credential still used by external location 'location'");
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void testLocationLockDoesNotBlockACreateOnItsCredential() throws Exception {
    // A location update locks only that location's row, then its credential. A concurrent create
    // on the credential locks the credential, then inserts a location, which the update's lock must
    // not block, or the two would deadlock. On MySQL a locking read by the unindexed name locked
    // every row and gap up to the match, so the location locked here is the one with the highest
    // id, and an insert below it would wait.
    Repositories repositories =
        new Repositories(
            testDatabase.sessionFactory(),
            serverProperties(/* isS3CompatibleStorageEnabled= */ true));
    repositories
        .getCredentialRepository()
        .addCredential(
            new CreateCredentialRequest()
                .name("role")
                .purpose(CredentialPurpose.STORAGE)
                .awsIamRole(new AwsIamRoleRequest().roleArn("arn:aws:iam::123456789012:role/r")));
    for (int i = 0; i < 10; i++) {
      repositories
          .getExternalLocationRepository()
          .addExternalLocation(
              new CreateExternalLocation()
                  .name("location" + i)
                  .url("s3://bucket/path" + i)
                  .credentialName("role"));
    }
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try (Session session = testDatabase.sessionFactory().openSession()) {
      String lastName =
          session
              .createSelectionQuery(
                  "SELECT name FROM ExternalLocationDAO ORDER BY id DESC", String.class)
              .setMaxResults(1)
              .getSingleResult();
      session.beginTransaction();
      assertThat(
              repositories
                  .getExternalLocationRepository()
                  .getExternalLocationDAOForUpdate(session, lastName))
          .isNotNull();
      Future<?> locationCreate =
          pool.submit(
              () -> {
                try (SafeCloseable ignored =
                    ServiceRequestContext.of(HttpRequest.of(HttpMethod.POST, "/")).push()) {
                  repositories
                      .getExternalLocationRepository()
                      .addExternalLocation(
                          new CreateExternalLocation()
                              .name("new_location")
                              .url("s3://bucket/new")
                              .credentialName("role"));
                }
              });
      locationCreate.get(30, TimeUnit.SECONDS);
      assertThat(repositories.getCredentialRepository().getCredentialDAOForUpdate(session, "role"))
          .isNotNull();
      session.getTransaction().commit();
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void testLocationCreateWaitsForATypeChangeOnItsCredential() throws Exception {
    // A type change to s3_compatible holds the credential's lock. A concurrent create of a gs://
    // location on that credential waits for it, then sees the new type and is refused.
    Repositories repositories =
        new Repositories(
            testDatabase.sessionFactory(),
            serverProperties(/* isS3CompatibleStorageEnabled= */ true));
    repositories
        .getCredentialRepository()
        .addCredential(
            new CreateCredentialRequest()
                .name("role")
                .purpose(CredentialPurpose.STORAGE)
                .awsIamRole(new AwsIamRoleRequest().roleArn("arn:aws:iam::123456789012:role/r")));
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try (Session session = testDatabase.sessionFactory().openSession()) {
      session.beginTransaction();
      CredentialDAO role =
          repositories.getCredentialRepository().getCredentialDAOForUpdate(session, "role");
      Future<?> locationCreate =
          pool.submit(
              () -> {
                try (SafeCloseable ignored =
                    ServiceRequestContext.of(HttpRequest.of(HttpMethod.POST, "/")).push()) {
                  repositories
                      .getExternalLocationRepository()
                      .addExternalLocation(
                          new CreateExternalLocation()
                              .name("location")
                              .url("gs://bucket/path")
                              .credentialName("role"));
                }
              });
      awaitLockWait();
      assertThat(locationCreate).isNotDone();
      role.setS3Compatible(
          s3Compatible("value"),
          new EncryptedCredentialSecretStore(Optional.of(TestUtils.CREDENTIAL_ENCRYPTION_KEY)));
      session.merge(role);
      session.getTransaction().commit();
      assertThatThrownBy(() -> locationCreate.get(30, TimeUnit.SECONDS))
          .hasCauseInstanceOf(BaseException.class)
          .cause()
          .hasMessageContaining("is an s3_compatible credential");
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * Waits until another session on this test's database waits for a row lock, so a test lets go of
   * its lock only once the competing change reached its locking read.
   */
  private void awaitLockWait() throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    try (Connection connection = testDatabase.dataSource().getConnection()) {
      String query =
          switch (connection.getMetaData().getDatabaseProductName()) {
            case "PostgreSQL" ->
                "SELECT COUNT(*) FROM pg_stat_activity"
                    + " WHERE datname = current_database() AND wait_event_type = 'Lock'";
            case "MySQL" ->
                "SELECT COUNT(*) FROM information_schema.innodb_trx t"
                    + " JOIN information_schema.processlist p ON p.id = t.trx_mysql_thread_id"
                    + " WHERE t.trx_state = 'LOCK WAIT' AND p.db = DATABASE()";
            default ->
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.SESSIONS WHERE BLOCKER_ID IS NOT NULL";
          };
      while (true) {
        try (Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(query)) {
          result.next();
          if (result.getLong(1) > 0) {
            return;
          }
        }
        assertThat(System.nanoTime()).as("a session waiting for a row lock").isLessThan(deadline);
        Thread.sleep(20);
      }
    }
  }
}
