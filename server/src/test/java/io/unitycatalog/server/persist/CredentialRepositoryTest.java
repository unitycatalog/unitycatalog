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
import io.unitycatalog.server.model.CredentialInfo;
import io.unitycatalog.server.model.CredentialPurpose;
import io.unitycatalog.server.model.ExternalLocationInfo;
import io.unitycatalog.server.model.S3CompatibleRequest;
import io.unitycatalog.server.model.UpdateCredentialRequest;
import io.unitycatalog.server.model.UpdateExternalLocation;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.secret.CredentialSecretStore;
import io.unitycatalog.server.persist.secret.EncryptedCredentialSecretStore;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import jakarta.persistence.LockModeType;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.hibernate.Session;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Checks when credential changes store secret values in their store. */
class CredentialRepositoryTest {
  @TempDatabase private TestDatabase testDatabase;

  private final RecordingSecretStore secretStore = new RecordingSecretStore();
  private Repositories repositories;
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
    repositories =
        new Repositories(
            testDatabase.sessionFactory(),
            serverProperties(/* isS3CompatibleStorageEnabled= */ true));
    credentialRepository =
        new CredentialRepository(
            repositories,
            testDatabase.sessionFactory(),
            serverProperties(/* isS3CompatibleStorageEnabled= */ true),
            secretStore);
  }

  @AfterEach
  void tearDown() {
    repositories.getOwnedCloudCredentialVendor().ifPresent(CloudCredentialVendor::close);
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
            repositories,
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

  @Test
  void testS3CompatibleCredentialStaysManageableWithTheFlagOff() {
    // Created while the flag was on; the server then restarts with it off and no key, as the
    // server builds its store from the properties.
    credentialRepository.addCredential(
        new CreateCredentialRequest()
            .name("compat")
            .purpose(CredentialPurpose.STORAGE)
            .s3Compatible(s3Compatible("value")));
    ServerProperties flagOff = serverProperties(/* isS3CompatibleStorageEnabled= */ false);
    CredentialRepository disabled =
        new CredentialRepository(
            repositories,
            testDatabase.sessionFactory(),
            flagOff,
            EncryptedCredentialSecretStore.from(flagOff));

    assertThat(disabled.getCredential("compat").getS3Compatible().getAccessKeyId())
        .isEqualTo("key-id");
    assertThat(disabled.listCredentials(Optional.empty(), Optional.empty()).getCredentials())
        .extracting(CredentialInfo::getName)
        .contains("compat");
    CredentialInfo renamed =
        disabled.updateCredential(
            "compat", new UpdateCredentialRequest().newName("renamed").comment("comment"));
    assertThat(renamed.getComment()).isEqualTo("comment");
    assertThat(renamed.getS3Compatible().getAccessKeyId()).isEqualTo("key-id");
    // New s3_compatible data still needs the flag.
    assertThatThrownBy(
            () ->
                disabled.updateCredential(
                    "renamed", new UpdateCredentialRequest().s3Compatible(s3Compatible("other"))))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("server.s3-compatible-storage.enabled=true");
    disabled.deleteCredential("renamed", /* force= */ false);
    assertThatThrownBy(() -> disabled.getCredential("renamed"))
        .isInstanceOf(BaseException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.NOT_FOUND);
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
        repositories,
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
    assertThatThrownBy(
            () ->
                testDatabase.runBlockedByHeldWrite(
                    session -> {
                      CredentialDAO role =
                          credentialRepository.getCredentialDAOForUpdate(session, "role");
                      session.persist(
                          ExternalLocationDAO.builder()
                              .id(UUID.randomUUID())
                              .name("location")
                              .url("gs://bucket/path")
                              .credentialId(role.getId())
                              .build());
                    },
                    () -> {
                      try (SafeCloseable ignored =
                          ServiceRequestContext.of(HttpRequest.of(HttpMethod.POST, "/")).push()) {
                        return credentialRepository.updateCredential(
                            "role",
                            new UpdateCredentialRequest().s3Compatible(s3Compatible("value")));
                      }
                    }))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("Cannot make credential 'role' an s3_compatible credential")
        .hasMessageNotContaining("gs://bucket/path");
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
    assertThatThrownBy(
            () ->
                testDatabase.runBlockedByHeldWrite(
                    session -> makeS3Compatible(session, repositories, "role"),
                    () -> {
                      try (SafeCloseable ignored =
                          ServiceRequestContext.of(HttpRequest.of(HttpMethod.POST, "/")).push()) {
                        return repositories
                            .getExternalLocationRepository()
                            .updateExternalLocation(
                                "location", new UpdateExternalLocation().url("gs://bucket/path"));
                      }
                    }))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("is an s3_compatible credential");
  }

  /** Changes a credential to s3_compatible in {@code session}, holding its lock. */
  private static void makeS3Compatible(Session session, Repositories repositories, String name) {
    CredentialDAO credential =
        repositories.getCredentialRepository().getCredentialDAOForUpdate(session, name);
    credential.setS3Compatible(
        s3Compatible("value"),
        new EncryptedCredentialSecretStore(Optional.of(TestUtils.CREDENTIAL_ENCRYPTION_KEY)));
    session.merge(credential);
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
    testDatabase.runBlockedByHeldWrite(
        session ->
            session
                .createQuery(
                    "FROM ExternalLocationDAO WHERE name = :name", ExternalLocationDAO.class)
                .setParameter("name", "location")
                .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                .getSingleResult()
                .setUrl("s3://bucket/path"),
        () -> {
          try (SafeCloseable ignored =
              ServiceRequestContext.of(HttpRequest.of(HttpMethod.POST, "/")).push()) {
            return repositories
                .getExternalLocationRepository()
                .updateExternalLocation(
                    "location", new UpdateExternalLocation().comment("comment"));
          }
        });
    assertThat(repositories.getExternalLocationRepository().getExternalLocation("location"))
        .returns("s3://bucket/path", ExternalLocationInfo::getUrl)
        .returns("comment", ExternalLocationInfo::getComment);
  }

  @Test
  void testDeleteByAnOldNameAfterARenameFindsNothing() throws Exception {
    // A delete that read the credential's id waits for its lock while another change commits: a
    // rename, a delete, or a delete and a new credential of the same name. The row it read no
    // longer has the name, or is gone, so the delete finds nothing rather than delete another row.
    CreateCredentialRequest before =
        new CreateCredentialRequest()
            .name("before")
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn("arn:aws:iam::123456789012:role/r"));
    Map<String, Consumer<Session>> changes =
        Map.of(
            "rename",
            session ->
                credentialRepository.getCredentialDAOForUpdate(session, "before").setName("after"),
            "delete",
            session ->
                session.remove(credentialRepository.getCredentialDAOForUpdate(session, "before")),
            "delete and recreate",
            session -> {
              session.remove(credentialRepository.getCredentialDAOForUpdate(session, "before"));
              // Deleted first, as the new row takes the name.
              session.flush();
              session.persist(CredentialDAO.from(before, "test-user", secretStore));
            });
    for (Map.Entry<String, Consumer<Session>> change : changes.entrySet()) {
      String deletedId = credentialRepository.addCredential(before).getId();
      assertThatThrownBy(
              () ->
                  testDatabase.runBlockedByHeldWrite(
                      change.getValue(),
                      () -> credentialRepository.deleteCredential("before", /* force= */ true)))
          .as(change.getKey())
          .isInstanceOf(BaseException.class)
          .hasMessageContaining("Credential not found: before");
      // Whatever holds a name now is left as it is.
      try (Session session = testDatabase.sessionFactory().openSession()) {
        CredentialDAO renamed = credentialRepository.getCredentialDAO(session, "after");
        CredentialDAO recreated = credentialRepository.getCredentialDAO(session, "before");
        switch (change.getKey()) {
          case "rename" -> assertThat(renamed).isNotNull();
          case "delete" -> assertThat(recreated).isNull();
          default -> assertThat(recreated.getId().toString()).isNotEqualTo(deletedId);
        }
      }
      for (String name : List.of("after", "before")) {
        try (Session session = testDatabase.sessionFactory().openSession()) {
          if (credentialRepository.getCredentialDAO(session, name) != null) {
            credentialRepository.deleteCredential(name, /* force= */ true);
          }
        }
      }
    }
  }

  @Test
  void testLockedLookupMatchesNamesAsTheDatabaseDoes() throws Exception {
    // MySQL's default collation finds "MyCred" for "mycred"; the locked read must find the same
    // credential a plain lookup does, on every database, or updates would get NOT_FOUND. The test
    // databases compare names exactly, so on MySQL the column takes the default collation here.
    try (Connection connection = testDatabase.dataSource().getConnection();
        Statement statement = connection.createStatement()) {
      if (connection.getMetaData().getDatabaseProductName().equals("MySQL")) {
        statement.execute(
            "ALTER TABLE uc_credentials MODIFY name varchar(255) NOT NULL"
                + " COLLATE utf8mb4_0900_ai_ci");
      }
    }
    credentialRepository.addCredential(
        new CreateCredentialRequest()
            .name("MyCred")
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn("arn:aws:iam::123456789012:role/r")));
    try (Session session = testDatabase.sessionFactory().openSession()) {
      session.beginTransaction();
      CredentialDAO plain = credentialRepository.getCredentialDAO(session, "mycred");
      CredentialDAO locked = credentialRepository.getCredentialDAOForUpdate(session, "mycred");
      assertThat(locked == null).isEqualTo(plain == null);
      session.getTransaction().rollback();
    }
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
    assertThatThrownBy(
            () ->
                testDatabase.runBlockedByHeldWrite(
                    session -> {
                      CredentialDAO role =
                          credentialRepository.getCredentialDAOForUpdate(session, "role");
                      session.persist(
                          ExternalLocationDAO.builder()
                              .id(UUID.randomUUID())
                              .name("location")
                              .url("s3://bucket/path")
                              .credentialId(role.getId())
                              .build());
                    },
                    () -> credentialRepository.deleteCredential("role", /* force= */ false)))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("Credential still used by external location 'location'");
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
    assertThatThrownBy(
            () ->
                testDatabase.runBlockedByHeldWrite(
                    session -> makeS3Compatible(session, repositories, "role"),
                    () -> {
                      try (SafeCloseable ignored =
                          ServiceRequestContext.of(HttpRequest.of(HttpMethod.POST, "/")).push()) {
                        return repositories
                            .getExternalLocationRepository()
                            .addExternalLocation(
                                new CreateExternalLocation()
                                    .name("location")
                                    .url("gs://bucket/path")
                                    .credentialName("role"));
                      }
                    }))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("is an s3_compatible credential");
  }
}
