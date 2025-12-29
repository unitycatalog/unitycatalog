package io.unitycatalog.server.base.externallocation;

import static io.unitycatalog.server.utils.TestUtils.COMMENT;
import static io.unitycatalog.server.utils.TestUtils.COMMENT2;
import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.ExternalLocationInfo;
import io.unitycatalog.client.model.UpdateExternalLocation;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.credential.CredentialOperations;
import io.unitycatalog.server.exception.ErrorCode;
import java.util.Optional;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseExternalLocationCRUDTest extends BaseCRUDTest {
  private static final String EXTERNAL_LOCATION_NAME = "uc_testexternallocation";
  private static final String NEW_EXTERNAL_LOCATION_NAME = EXTERNAL_LOCATION_NAME + "_new";
  private static final String URL = "s3://unitycatalog-test";
  private static final String NEW_URL = "s3://unitycatalog-test-new";
  private static final String CREDENTIAL_NAME = "uc_testcredential";
  private static final String DUMMY_ROLE_ARN = "arn:aws:iam::123456789012:role/role-name";
  protected ExternalLocationOperations externalLocationOperations;

  protected abstract ExternalLocationOperations createExternalLocationOperations(
      ServerConfig config);

  protected CredentialOperations credentialOperations;

  protected abstract CredentialOperations createCredentialOperations(ServerConfig config);

  protected CredentialInfo credentialInfo = null;

  @SneakyThrows
  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    externalLocationOperations = createExternalLocationOperations(serverConfig);
    credentialOperations = createCredentialOperations(serverConfig);

    CreateCredentialRequest createCredentialRequest =
        new CreateCredentialRequest()
            .name(CREDENTIAL_NAME)
            .comment(COMMENT)
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN));
    credentialInfo = credentialOperations.createCredential(createCredentialRequest);
  }

  @SneakyThrows
  @AfterEach
  @Override
  public void tearDown() {
    credentialOperations.deleteCredential(credentialInfo.getName());
    credentialInfo = null;
    super.tearDown();
  }

  @Test
  public void testExternalLocationCRUD() throws ApiException {
    // Create an external location
    CreateExternalLocation createExternalLocation =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME)
            .comment(COMMENT)
            .url(URL)
            .credentialName("not_exist");
    // Fails as the credential does not exist
    assertApiException(
        () -> externalLocationOperations.createExternalLocation(createExternalLocation),
        ErrorCode.NOT_FOUND,
        "Credential not found: not_exist");

    assertThat(externalLocationOperations.listExternalLocations(Optional.empty()))
        .noneMatch(
            externalLocationInfo -> externalLocationInfo.getName().equals(EXTERNAL_LOCATION_NAME));

    createExternalLocation.setCredentialName(CREDENTIAL_NAME);
    ExternalLocationInfo externalLocationInfo =
        externalLocationOperations.createExternalLocation(createExternalLocation);
    assertThat(externalLocationInfo.getName()).isEqualTo(EXTERNAL_LOCATION_NAME);
    assertThat(externalLocationInfo.getComment()).isEqualTo(COMMENT);
    assertThat(externalLocationInfo.getUrl()).isEqualTo(URL);
    assertThat(externalLocationInfo.getCredentialId()).isEqualTo(credentialInfo.getId());

    // List external locations
    assertThat(externalLocationOperations.listExternalLocations(Optional.empty()))
        .contains(externalLocationInfo);

    // Get external location
    assertThat(externalLocationOperations.getExternalLocation(EXTERNAL_LOCATION_NAME))
        .isEqualTo(externalLocationInfo);

    // Update external location
    UpdateExternalLocation updateExternalLocation =
        new UpdateExternalLocation()
            .newName(NEW_EXTERNAL_LOCATION_NAME)
            .comment(COMMENT2)
            .url(NEW_URL);
    ExternalLocationInfo updatedExternalLocationInfo =
        externalLocationOperations.updateExternalLocation(
            EXTERNAL_LOCATION_NAME, updateExternalLocation);
    assertThat(updatedExternalLocationInfo.getName()).isEqualTo(NEW_EXTERNAL_LOCATION_NAME);
    assertThat(updatedExternalLocationInfo.getComment()).isEqualTo(COMMENT2);
    assertThat(updatedExternalLocationInfo.getUrl()).isEqualTo(NEW_URL);
    assertThat(updatedExternalLocationInfo.getCredentialId()).isEqualTo(credentialInfo.getId());

    // Delete external location
    externalLocationOperations.deleteExternalLocation(NEW_EXTERNAL_LOCATION_NAME);
    assertThatThrownBy(
            () -> externalLocationOperations.getExternalLocation(NEW_EXTERNAL_LOCATION_NAME))
        .isInstanceOf(ApiException.class);
  }

  @Test
  public void testExternalLocationUrlOverlapPrevention() throws ApiException {
    CreateExternalLocation firstLocation =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME)
            .comment(COMMENT)
            .url(URL)
            .credentialName(CREDENTIAL_NAME);
    externalLocationOperations.createExternalLocation(firstLocation);

    // Test 1: Duplicate URL should be rejected
    CreateExternalLocation duplicateLocation =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME + "_duplicate")
            .comment(COMMENT)
            .url(URL)
            .credentialName(CREDENTIAL_NAME);
    assertApiException(
        () -> externalLocationOperations.createExternalLocation(duplicateLocation),
        ErrorCode.INVALID_ARGUMENT,
        "An existing external location with URL");

    // Test 2: Duplicate name should be rejected
    CreateExternalLocation duplicateName =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME)
            .comment(COMMENT)
            .url(NEW_URL)
            .credentialName(CREDENTIAL_NAME);
    assertApiException(
        () -> externalLocationOperations.createExternalLocation(duplicateName),
        ErrorCode.ALREADY_EXISTS,
        "already exist");

    // Test 3: Child URL should be rejected when parent exists
    CreateExternalLocation childLocation =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME + "_child")
            .comment(COMMENT)
            .url(URL + "/subpath")
            .credentialName(CREDENTIAL_NAME);
    assertApiException(
        () -> externalLocationOperations.createExternalLocation(childLocation),
        ErrorCode.INVALID_ARGUMENT,
        "An existing external location with URL");

    // Clean up for next test
    externalLocationOperations.deleteExternalLocation(EXTERNAL_LOCATION_NAME);

    // Create an external location with subdir first
    CreateExternalLocation childFirst =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME)
            .comment(COMMENT)
            .url(URL + "/subpath/deeper")
            .credentialName(CREDENTIAL_NAME);
    externalLocationOperations.createExternalLocation(childFirst);

    // Test 4: Parent URL should be rejected when child exists
    CreateExternalLocation parentLocation =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME + "_parent")
            .comment(COMMENT)
            .url(URL)
            .credentialName(CREDENTIAL_NAME);
    assertApiException(
        () -> externalLocationOperations.createExternalLocation(parentLocation),
        ErrorCode.INVALID_ARGUMENT,
        "An existing external location with URL");

    // Test 5: Trailing slash should be normalized (treated as same URL)
    CreateExternalLocation withSlash =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME + "_slash")
            .comment(COMMENT)
            .url(URL + "/subpath/deeper/")
            .credentialName(CREDENTIAL_NAME);
    assertApiException(
        () -> externalLocationOperations.createExternalLocation(withSlash),
        ErrorCode.INVALID_ARGUMENT,
        "already exist");
    externalLocationOperations.deleteExternalLocation(EXTERNAL_LOCATION_NAME);

    // Test 6: Different buckets should be allowed (no overlap)
    CreateExternalLocation bucket1Location =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME + "_bucket1")
            .comment(COMMENT)
            .url("s3://bucket1/path")
            .credentialName(CREDENTIAL_NAME);
    externalLocationOperations.createExternalLocation(bucket1Location);

    CreateExternalLocation bucket2Location =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME + "_bucket2")
            .comment(COMMENT)
            .url("s3://bucket2/path")
            .credentialName(CREDENTIAL_NAME);
    ExternalLocationInfo bucket2Info =
        externalLocationOperations.createExternalLocation(bucket2Location);
    assertThat(bucket2Info.getName()).isEqualTo(EXTERNAL_LOCATION_NAME + "_bucket2");

    // Test 7: Update bucket2 location to use bucket1 with overlapping URL would fail.
    UpdateExternalLocation updateBucket2Location =
        new UpdateExternalLocation().url("s3://bucket1/");
    assertApiException(
        () ->
            externalLocationOperations.updateExternalLocation(
                EXTERNAL_LOCATION_NAME + "_bucket2", updateBucket2Location),
        ErrorCode.INVALID_ARGUMENT,
        "already exist");

    // Cleanup for both buckets
    externalLocationOperations.deleteExternalLocation(EXTERNAL_LOCATION_NAME + "_bucket1");
    externalLocationOperations.deleteExternalLocation(EXTERNAL_LOCATION_NAME + "_bucket2");

    // Test 8: sibling paths (same level, different names) are allowed
    CreateExternalLocation sibling1 =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME + "_sibling1")
            .comment(COMMENT)
            .url(URL + "/data1")
            .credentialName(CREDENTIAL_NAME);
    externalLocationOperations.createExternalLocation(sibling1);

    CreateExternalLocation sibling2 =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME + "_sibling2")
            .comment(COMMENT)
            .url(URL + "/data2")
            .credentialName(CREDENTIAL_NAME);
    ExternalLocationInfo sibling2Info = externalLocationOperations.createExternalLocation(sibling2);
    assertThat(sibling2Info.getName()).isEqualTo(EXTERNAL_LOCATION_NAME + "_sibling2");

    // Cleanup
    externalLocationOperations.deleteExternalLocation(EXTERNAL_LOCATION_NAME + "_sibling1");
    externalLocationOperations.deleteExternalLocation(EXTERNAL_LOCATION_NAME + "_sibling2");
  }
}
