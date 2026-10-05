package io.unitycatalog.server.sdk.storagecredential;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.AwsS3AccessKeyRequest;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.credential.BaseCredentialCRUDTest;
import io.unitycatalog.server.base.credential.CredentialOperations;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.externallocation.SdkExternalLocationOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class SdkCredentialCRUDTest extends BaseCredentialCRUDTest {
  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/role-name";
  private static final String ACCESS_KEY_ID = "AKIAEXAMPLEKEY";

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected CredentialOperations createCredentialOperations(ServerConfig config) {
    return new SdkCredentialOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected ExternalLocationOperations createExternalLocationOperations(ServerConfig config) {
    return new SdkExternalLocationOperations(TestUtils.createApiClient(config));
  }

  // Request validation below is server-side, so it is exercised through the SDK only rather than
  // through every transport.

  @Test
  public void testCreateCredentialRejectsBothCredentialTypes() {
    assertApiException(
        () ->
            credentialOperations.createCredential(
                new CreateCredentialRequest()
                    .name("uc_test_both_types")
                    .purpose(CredentialPurpose.STORAGE)
                    .awsIamRole(new AwsIamRoleRequest().roleArn(ROLE_ARN))
                    .awsS3AccessKey(new AwsS3AccessKeyRequest().accessKeyId(ACCESS_KEY_ID))),
        ErrorCode.INVALID_ARGUMENT,
        "Specify exactly one of aws_iam_role or aws_s3_access_key");
  }

  @Test
  public void testCreateCredentialRejectsMissingCredentialType() {
    assertApiException(
        () ->
            credentialOperations.createCredential(
                new CreateCredentialRequest()
                    .name("uc_test_no_type")
                    .purpose(CredentialPurpose.STORAGE)),
        ErrorCode.INVALID_ARGUMENT,
        "Specify exactly one of aws_iam_role or aws_s3_access_key");
  }

  @Test
  public void testCreateCredentialRejectsBlankAccessKeyId() {
    assertApiException(
        () ->
            credentialOperations.createCredential(
                new CreateCredentialRequest()
                    .name("uc_test_blank_key_id")
                    .purpose(CredentialPurpose.STORAGE)
                    .awsS3AccessKey(new AwsS3AccessKeyRequest().accessKeyId("   "))),
        ErrorCode.INVALID_ARGUMENT,
        "aws_s3_access_key.access_key_id is required and must be non-empty");
  }

  @Test
  public void testCreateCredentialRejectsBlankRoleArn() {
    assertApiException(
        () ->
            credentialOperations.createCredential(
                new CreateCredentialRequest()
                    .name("uc_test_blank_role_arn")
                    .purpose(CredentialPurpose.STORAGE)
                    .awsIamRole(new AwsIamRoleRequest().roleArn("   "))),
        ErrorCode.INVALID_ARGUMENT,
        "aws_iam_role.role_arn is required and must be non-empty");
  }

  @Test
  public void testUpdateCredentialRejectsBothCredentialTypes() throws ApiException {
    String name = "uc_test_update_both_types";
    credentialOperations.createCredential(
        new CreateCredentialRequest()
            .name(name)
            .purpose(CredentialPurpose.STORAGE)
            .awsS3AccessKey(new AwsS3AccessKeyRequest().accessKeyId(ACCESS_KEY_ID)));

    assertApiException(
        () ->
            credentialOperations.updateCredential(
                name,
                new UpdateCredentialRequest()
                    .awsIamRole(new AwsIamRoleRequest().roleArn(ROLE_ARN))
                    .awsS3AccessKey(new AwsS3AccessKeyRequest().accessKeyId(ACCESS_KEY_ID))),
        ErrorCode.INVALID_ARGUMENT,
        "Specify at most one of aws_iam_role or aws_s3_access_key");

    credentialOperations.deleteCredential(name, Optional.empty());
  }

  @Test
  public void testUpdateCredentialRejectsBlankAccessKeyId() throws ApiException {
    String name = "uc_test_update_blank_key_id";
    credentialOperations.createCredential(
        new CreateCredentialRequest()
            .name(name)
            .purpose(CredentialPurpose.STORAGE)
            .awsS3AccessKey(new AwsS3AccessKeyRequest().accessKeyId(ACCESS_KEY_ID)));

    assertApiException(
        () ->
            credentialOperations.updateCredential(
                name, new UpdateCredentialRequest().awsS3AccessKey(new AwsS3AccessKeyRequest())),
        ErrorCode.INVALID_ARGUMENT,
        "aws_s3_access_key.access_key_id is required and must be non-empty");

    // The rejected update left the stored access key id intact.
    assertThat(credentialOperations.getCredential(name).getAwsS3AccessKey().getAccessKeyId())
        .isEqualTo(ACCESS_KEY_ID);

    credentialOperations.deleteCredential(name, Optional.empty());
  }
}
