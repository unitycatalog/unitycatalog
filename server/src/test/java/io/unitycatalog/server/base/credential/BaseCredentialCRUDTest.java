package io.unitycatalog.server.base.credential;

import static io.unitycatalog.server.utils.TestUtils.COMMENT;
import static io.unitycatalog.server.utils.TestUtils.COMMENT2;
import static io.unitycatalog.server.utils.TestUtils.TEST_AWS_MASTER_ROLE_ARN;
import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.util.SafeCloseable;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.ExternalLocationInfo;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.cleanup.StorageCleanupTestSupport;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.CredentialRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.testdatabase.SupportedDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseCredentialCRUDTest extends BaseCRUDTest {
  private static final String CREDENTIAL_NAME = "uc_testcredential";
  private static final String NEW_CREDENTIAL_NAME = CREDENTIAL_NAME + "_new";
  private static final String DUMMY_ROLE_ARN = "arn:aws:iam::123456789012:role/role-name";
  private static final String NEW_ROLE_ARN = "arn:aws:iam::987654321098:role/new-role-name";
  private static final String EXTERNAL_LOCATION_NAME = "uc_testexternallocation";
  private static final String URL = "s3://unitycatalog-test";
  protected CredentialOperations credentialOperations;
  protected ExternalLocationOperations externalLocationOperations;

  protected abstract CredentialOperations createCredentialOperations(ServerConfig config);

  protected abstract ExternalLocationOperations createExternalLocationOperations(
      ServerConfig config);

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(
        ServerProperties.Property.AWS_MASTER_ROLE_ARN.getKey(), TEST_AWS_MASTER_ROLE_ARN);
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    credentialOperations = createCredentialOperations(serverConfig);
    externalLocationOperations = createExternalLocationOperations(serverConfig);
  }

  @Test
  public void testStorageCredentialCRUD() throws ApiException {
    // Create a storage credential
    CreateCredentialRequest createCredentialRequest =
        new CreateCredentialRequest()
            .name(CREDENTIAL_NAME)
            .comment(COMMENT)
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN));

    CredentialInfo credentialInfo = credentialOperations.createCredential(createCredentialRequest);
    assertThat(credentialInfo.getName()).isEqualTo(CREDENTIAL_NAME);
    assertThat(credentialInfo.getComment()).isEqualTo(COMMENT);
    assertThat(credentialInfo.getPurpose()).isEqualTo(CredentialPurpose.STORAGE);
    assertThat(credentialInfo.getAwsIamRole().getRoleArn()).isEqualTo(DUMMY_ROLE_ARN);
    assertThat(credentialInfo.getAwsIamRole().getExternalId()).isNotEmpty();
    assertDoesNotThrow(() -> UUID.fromString(credentialInfo.getAwsIamRole().getExternalId()));
    assertThat(credentialInfo.getAwsIamRole().getUnityCatalogIamArn())
        .isEqualTo(TEST_AWS_MASTER_ROLE_ARN);

    // List storage credentials
    assertThat(credentialOperations.listCredentials(Optional.empty(), CredentialPurpose.STORAGE))
        .contains(credentialInfo);

    // Get storage credential
    assertThat(credentialOperations.getCredential(CREDENTIAL_NAME)).isEqualTo(credentialInfo);

    // Update storage credential
    UpdateCredentialRequest updateCredentialRequest =
        new UpdateCredentialRequest()
            .newName(NEW_CREDENTIAL_NAME)
            .comment(COMMENT2)
            .awsIamRole(new AwsIamRoleRequest().roleArn(NEW_ROLE_ARN));

    CredentialInfo updatedCredentialInfo =
        credentialOperations.updateCredential(CREDENTIAL_NAME, updateCredentialRequest);
    assertThat(updatedCredentialInfo.getName()).isEqualTo(NEW_CREDENTIAL_NAME);
    assertThat(updatedCredentialInfo.getComment()).isEqualTo(COMMENT2);
    assertThat(updatedCredentialInfo.getAwsIamRole().getRoleArn()).isEqualTo(NEW_ROLE_ARN);
    assertThat(updatedCredentialInfo.getAwsIamRole().getExternalId()).isNotEmpty();
    assertDoesNotThrow(
        () -> UUID.fromString(updatedCredentialInfo.getAwsIamRole().getExternalId()));
    assertThat(updatedCredentialInfo.getAwsIamRole().getExternalId())
        .isNotEqualTo(credentialInfo.getAwsIamRole().getExternalId());
    assertThat(updatedCredentialInfo.getAwsIamRole().getUnityCatalogIamArn())
        .isEqualTo(TEST_AWS_MASTER_ROLE_ARN);

    // Delete storage credential
    credentialOperations.deleteCredential(NEW_CREDENTIAL_NAME, Optional.empty());
    assertThatThrownBy(() -> credentialOperations.getCredential(NEW_CREDENTIAL_NAME))
        .isInstanceOf(ApiException.class);
  }

  @Test
  public void testCredentialDeletion() throws ApiException {
    // Test 1: Credential with external location - delete without force should fail
    String credentialWithExternalLocationName = CREDENTIAL_NAME + "_with_el";
    CreateCredentialRequest createCredentialRequest =
        new CreateCredentialRequest()
            .name(credentialWithExternalLocationName)
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN));
    credentialOperations.createCredential(createCredentialRequest);

    // Create an external location that uses this credential
    CreateExternalLocation createExternalLocation =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME)
            .url(URL)
            .credentialName(credentialWithExternalLocationName);
    externalLocationOperations.createExternalLocation(createExternalLocation);

    // Try to delete credential without force - should fail
    assertApiException(
        () ->
            credentialOperations.deleteCredential(
                credentialWithExternalLocationName, Optional.empty()),
        ErrorCode.INVALID_ARGUMENT,
        "Credential still used by external location");

    // Delete with force should succeed
    credentialOperations.deleteCredential(credentialWithExternalLocationName, Optional.of(true));
    assertThat(externalLocationOperations.getExternalLocation(EXTERNAL_LOCATION_NAME))
        .returns(URL, ExternalLocationInfo::getUrl)
        .returns(null, ExternalLocationInfo::getCredentialName);
    // Clean up the external location
    externalLocationOperations.deleteExternalLocation(EXTERNAL_LOCATION_NAME, Optional.of(true));

    // Test 2: Credential without external location - delete without force should succeed
    String credentialWithoutExternalLocationName = CREDENTIAL_NAME + "_without_el";
    createCredentialRequest.setName(credentialWithoutExternalLocationName);
    credentialOperations.createCredential(createCredentialRequest);

    // Delete without force should succeed
    credentialOperations.deleteCredential(credentialWithoutExternalLocationName, Optional.empty());
  }

  /** A drop that waits for a concurrent rename must not delete the renamed credential. */
  @Test
  public void dropRacingARenameDoesNotDeleteTheRenamedCredential() throws Exception {
    UUID id = UUID.fromString(createCredential("drop_race").getId());

    TestUtils.assertApiException(
        () ->
            runBlockedByHeldWrite(
                held ->
                    TestDatabase.setColumn(
                        held, CredentialDAO.class, id, "name", "drop_race_renamed"),
                () -> {
                  credentialOperations.deleteCredential("drop_race", Optional.empty());
                  return null;
                }),
        ErrorCode.NOT_FOUND,
        "Credential not found");

    assertThat(credentialOperations.getCredential("drop_race_renamed").getId())
        .isEqualTo(id.toString());
  }

  /** A rename that waits for a concurrent edit of another column keeps that edit. */
  @Test
  public void renameKeepsAConcurrentCommentEdit() throws Exception {
    UUID id = UUID.fromString(createCredential("edit_race").getId());

    runBlockedByHeldWrite(
        held -> TestDatabase.setColumn(held, CredentialDAO.class, id, "comment", "held comment"),
        () ->
            credentialOperations.updateCredential(
                "edit_race", new UpdateCredentialRequest().newName("edited")));

    CredentialInfo renamed = credentialOperations.getCredential("edited");
    assertThat(renamed.getId()).isEqualTo(id.toString());
    assertThat(renamed.getComment()).isEqualTo("held comment");
  }

  private CredentialInfo createCredential(String name) throws ApiException {
    return credentialOperations.createCredential(
        new CreateCredentialRequest()
            .name(name)
            .comment(COMMENT)
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN)));
  }

  /** An update that waits for a concurrent rename of the credential misses it, changing nothing. */
  @Test
  public void updateRacingARenameIsNotFound() throws Exception {
    UUID id = UUID.fromString(createCredential("update_race").getId());

    TestUtils.assertApiException(
        () ->
            runBlockedByHeldWrite(
                held ->
                    TestDatabase.setColumn(
                        held, CredentialDAO.class, id, "name", "update_race_renamed"),
                () ->
                    credentialOperations.updateCredential(
                        "update_race", new UpdateCredentialRequest().comment("lost"))),
        ErrorCode.NOT_FOUND,
        "Storage credential not found");

    CredentialInfo renamed = credentialOperations.getCredential("update_race_renamed");
    assertThat(renamed.getId()).isEqualTo(id.toString());
    assertThat(renamed.getComment()).isEqualTo(COMMENT);
  }

  /**
   * A drop or update that cannot get the credential row lock before the lock timeout fails as
   * UNAVAILABLE (503, retryable), not INTERNAL.
   */
  @Test
  public void dropAndUpdateBlockedPastTheLockTimeoutAreUnavailable() throws Exception {
    UUID id = UUID.fromString(createCredential("lock_timeout").getId());
    CredentialRepository impatient =
        repositoriesWith(
                properties ->
                    properties.setProperty(
                        "hibernate.connection.url",
                        testDatabase.jdbcUrl()
                            + switch (SupportedDatabase.selected()) {
                              case H2 -> ";LOCK_TIMEOUT=50";
                              case POSTGRES_18 -> "?options=-c%20lock_timeout%3D50";
                              case MYSQL_8 -> "?sessionVariables=innodb_lock_wait_timeout=1";
                            }))
            .getCredentialRepository();
    List<String> codes = new ArrayList<>();
    try (Session held = testDatabase.sessionFactory().openSession()) {
      Transaction transaction = held.beginTransaction();
      TestDatabase.setColumn(held, CredentialDAO.class, id, "comment", "held comment");
      held.flush();
      try (SafeCloseable ignored = requestContext()) {
        codes.add("drop=" + errorCode(() -> impatient.deleteCredential("lock_timeout", false)));
        codes.add(
            "update="
                + errorCode(
                    () ->
                        impatient.updateCredential(
                            "lock_timeout",
                            new io.unitycatalog.server.model.UpdateCredentialRequest()
                                .comment("lost"))));
      }
      transaction.rollback();
    }
    assertThat(codes).containsExactly("drop=UNAVAILABLE", "update=UNAVAILABLE");
  }

  private Repositories repositoriesWith(Consumer<Properties> settings) {
    Properties properties = testDatabase.hibernateProperties();
    properties.setProperty("hibernate.hbm2ddl.auto", "none");
    settings.accept(properties);
    return StorageCleanupTestSupport.auditedRepositories(
        testDatabase.openHibernateConfigurator(properties).getSessionFactory(),
        new ServerProperties(serverProperties));
  }

  private static SafeCloseable requestContext() {
    return ServiceRequestContext.of(HttpRequest.of(HttpMethod.GET, "/")).push();
  }

  private static String errorCode(Runnable operation) {
    try {
      operation.run();
      return "none";
    } catch (BaseException e) {
      return e.getErrorCode().name();
    }
  }
}
