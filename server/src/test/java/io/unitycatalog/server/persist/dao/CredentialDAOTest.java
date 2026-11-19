package io.unitycatalog.server.persist.dao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialInfo;
import io.unitycatalog.server.model.CredentialPurpose;
import io.unitycatalog.server.model.S3AssumeRole;
import io.unitycatalog.server.model.S3CompatibleAccessKeyRequest;
import io.unitycatalog.server.model.S3CompatibleAccessKeyResponse;
import io.unitycatalog.server.persist.secret.EncryptedCredentialSecretStore;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Base64;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class CredentialDAOTest {
  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/role-name";
  private static final EncryptedCredentialSecretStore SECRET_STORE =
      new EncryptedCredentialSecretStore(Optional.of(TestUtils.CREDENTIAL_ENCRYPTION_KEY));

  private static CredentialDAO s3CompatibleDao(S3CompatibleAccessKeyRequest s3Compatible) {
    return CredentialDAO.from(
        new CreateCredentialRequest()
            .name("compat")
            .purpose(CredentialPurpose.STORAGE)
            .s3CompatibleAccessKey(s3Compatible),
        "caller",
        SECRET_STORE);
  }

  @Test
  public void testWriteOnlyFieldIsStoredButNotInResponse() throws Exception {
    S3CompatibleAccessKeyRequest request =
        new S3CompatibleAccessKeyRequest()
            .accessKeyId("key-id-1")
            .secretAccessKey("write-only-value");
    CredentialDAO dao = s3CompatibleDao(request);

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
    assertThat(info.getS3CompatibleAccessKey().getAccessKeyId()).isEqualTo("key-id-1");
    // The vend cache keys on the fingerprint of the stored value: the same value has the same
    // one, and the key pair stored again (with a fresh nonce) has another.
    String one = "{\"access_key_id\":\"key-id-1\"}";
    String two = "{\"access_key_id\":\"key-id-2\"}";
    assertThat(CredentialDAO.builder().credential(one).build().getCredentialFingerprint())
        .isEqualTo(CredentialDAO.builder().credential(one).build().getCredentialFingerprint())
        .isNotEqualTo(CredentialDAO.builder().credential(two).build().getCredentialFingerprint());
    assertThat(s3CompatibleDao(request).getCredentialFingerprint())
        .isNotEqualTo(dao.getCredentialFingerprint());
  }

  @Test
  public void testMissingStoredValueIsReportedByName() {
    CredentialDAO dao =
        CredentialDAO.builder()
            .id(UUID.randomUUID())
            .name("compat")
            .credentialType(CredentialDAO.CredentialType.S3_COMPATIBLE)
            .credential("{\"access_key_id\":\"key-id-1\"}")
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
    // The server's own data, so an internal error rather than a caller's invalid argument.
    assertThatThrownBy(dao::getS3CompatibleAccessKeyResponse)
        .isInstanceOf(BaseException.class)
        .hasMessage("Stored credential 'compat' of type S3_COMPATIBLE cannot be read.")
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INTERNAL);
  }

  @Test
  public void testStoredValueOfTheWrongShapeIsAnInternalError() {
    CredentialDAO dao =
        CredentialDAO.builder()
            .id(UUID.randomUUID())
            .name("compat")
            .credentialType(CredentialDAO.CredentialType.S3_COMPATIBLE)
            .credential("{\"access_key_id\":\"key-id-1\",\"assume_role\":\"not-an-object\"}")
            .build();
    assertThatThrownBy(dao::getS3CompatibleAccessKeyResponse)
        .isInstanceOf(BaseException.class)
        .hasMessage("Stored credential 'compat' of type S3_COMPATIBLE cannot be read.")
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INTERNAL);
  }

  @Test
  public void testStsEndpointIsStoredOnlyWhenSet() {
    S3CompatibleAccessKeyRequest keyPair =
        new S3CompatibleAccessKeyRequest().accessKeyId("key-id-1").secretAccessKey("key-value-1");

    assertThat(s3CompatibleDao(keyPair).getS3CompatibleAccessKeyResponse())
        .isEqualTo(new S3CompatibleAccessKeyResponse().accessKeyId("key-id-1"));
    assertThat(
            s3CompatibleDao(keyPair.assumeRole(new S3AssumeRole().roleArn(ROLE_ARN)))
                .getS3CompatibleAccessKeyResponse())
        .isEqualTo(
            new S3CompatibleAccessKeyResponse()
                .accessKeyId("key-id-1")
                .assumeRole(new S3AssumeRole().roleArn(ROLE_ARN)));
    S3AssumeRole withSts =
        new S3AssumeRole().stsEndpoint("https://sts.example.com").roleArn(ROLE_ARN);
    assertThat(
            s3CompatibleDao(keyPair.assumeRole(withSts))
                .getS3CompatibleAccessKeyResponse()
                .getAssumeRole())
        .isEqualTo(withSts);
  }

  @Test
  public void testCredentialStoredBeforeS3CompatibleStillReads() {
    // The blob shape persisted by servers predating s3_compatible_access_key.
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
    assertThat(dao.toCredentialInfo(Optional.empty()).getS3CompatibleAccessKey()).isNull();
  }
}
