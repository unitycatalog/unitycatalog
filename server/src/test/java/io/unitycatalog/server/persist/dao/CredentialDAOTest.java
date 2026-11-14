package io.unitycatalog.server.persist.dao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialInfo;
import io.unitycatalog.server.model.CredentialPurpose;
import io.unitycatalog.server.model.S3AssumeRole;
import io.unitycatalog.server.model.S3CompatibleRequest;
import io.unitycatalog.server.model.S3CompatibleResponse;
import io.unitycatalog.server.persist.secret.EncryptedCredentialSecretStore;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Base64;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class CredentialDAOTest {
  private static final String S3_ENDPOINT = "https://storage.example.com:9000";

  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/role-name";
  private static final EncryptedCredentialSecretStore SECRET_STORE =
      new EncryptedCredentialSecretStore(Optional.of(TestUtils.CREDENTIAL_ENCRYPTION_KEY));

  private static CredentialDAO s3CompatibleDao(S3CompatibleRequest s3Compatible) {
    return CredentialDAO.from(
        new CreateCredentialRequest()
            .name("compat")
            .purpose(CredentialPurpose.STORAGE)
            .s3Compatible(s3Compatible),
        "caller",
        SECRET_STORE);
  }

  @Test
  public void testWriteOnlyFieldIsStoredButNotInResponse() throws Exception {
    CredentialDAO dao =
        s3CompatibleDao(
            new S3CompatibleRequest()
                .accessKeyId("key-id-1")
                .secretAccessKey("write-only-value")
                .s3Endpoint(S3_ENDPOINT));

    assertThat(dao.getS3SecretAccessKey(SECRET_STORE)).isEqualTo("write-only-value");
    // Stored encrypted: a server with another key cannot read it.
    EncryptedCredentialSecretStore otherKey =
        new EncryptedCredentialSecretStore(
            Optional.of(Base64.getEncoder().encodeToString(new byte[32])));
    assertThatThrownBy(() -> dao.getS3SecretAccessKey(otherKey)).isInstanceOf(BaseException.class);
    // Check the serialized response: the client model has no field for this value, so a
    // client-side read would drop it silently.
    CredentialInfo info = dao.toCredentialInfo(Optional.empty());
    String json = new ObjectMapper().writeValueAsString(info);
    assertThat(json).doesNotContain("write-only-value").doesNotContain("secret_access_key");
    assertThat(info.getS3Compatible().getAccessKeyId()).isEqualTo("key-id-1");
  }

  @Test
  public void testMissingStoredValueIsReportedByName() {
    CredentialDAO dao =
        CredentialDAO.builder()
            .id(UUID.randomUUID())
            .name("compat")
            .credentialType(CredentialDAO.CredentialType.S3_COMPATIBLE)
            .credential("{\"access_key_id\":\"key-id-1\",\"s3_endpoint\":\"" + S3_ENDPOINT + "\"}")
            .build();
    assertThatThrownBy(() -> dao.getS3SecretAccessKey(SECRET_STORE))
        .isInstanceOf(BaseException.class)
        .hasMessage("Storage credential 'compat' has no stored secret_access_key.");
  }

  @Test
  public void testStoredValueThatIsNotAnObjectIsRefused() {
    CredentialDAO dao =
        CredentialDAO.builder()
            .id(UUID.randomUUID())
            .name("compat")
            .credentialType(CredentialDAO.CredentialType.S3_COMPATIBLE)
            .credential("[\"key-id-1\"]")
            .build();
    assertThatThrownBy(dao::getS3CompatibleResponse)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse credential of S3_COMPATIBLE");
  }

  @Test
  public void testStsEndpointIsStoredOnlyWhenSet() {
    S3CompatibleRequest keyPair =
        new S3CompatibleRequest()
            .accessKeyId("key-id-1")
            .secretAccessKey("key-value-1")
            .s3Endpoint(S3_ENDPOINT);

    assertThat(s3CompatibleDao(keyPair).getS3CompatibleResponse())
        .isEqualTo(new S3CompatibleResponse().accessKeyId("key-id-1").s3Endpoint(S3_ENDPOINT));
    assertThat(
            s3CompatibleDao(keyPair.assumeRole(new S3AssumeRole().roleArn(ROLE_ARN)))
                .getS3CompatibleResponse())
        .isEqualTo(
            new S3CompatibleResponse()
                .accessKeyId("key-id-1")
                .s3Endpoint(S3_ENDPOINT)
                .assumeRole(new S3AssumeRole().roleArn(ROLE_ARN)));
    S3AssumeRole withSts =
        new S3AssumeRole().stsEndpoint("https://sts.example.com").roleArn(ROLE_ARN);
    assertThat(
            s3CompatibleDao(keyPair.assumeRole(withSts)).getS3CompatibleResponse().getAssumeRole())
        .isEqualTo(withSts);
  }

  @Test
  public void testCredentialStoredBeforeS3CompatibleStillReads() {
    // The blob shape persisted by servers predating s3_compatible.
    CredentialDAO dao =
        CredentialDAO.builder()
            .id(UUID.randomUUID())
            .name("aws")
            .purpose(CredentialPurpose.STORAGE)
            .credentialType(CredentialDAO.CredentialType.AWS_IAM_ROLE)
            .credential("{\"role_arn\":\"" + ROLE_ARN + "\",\"external_id\":\"ext-id\"}")
            .createdAt(new Date())
            .build();

    assertThat(dao.getAwsIamRoleResponse().getRoleArn()).isEqualTo(ROLE_ARN);
    assertThat(dao.getAwsIamRoleResponse().getExternalId()).isEqualTo("ext-id");
    assertThat(dao.toCredentialInfo(Optional.empty()).getS3Compatible()).isNull();
  }
}
