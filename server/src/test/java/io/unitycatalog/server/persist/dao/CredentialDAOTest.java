package io.unitycatalog.server.persist.dao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialInfo;
import io.unitycatalog.server.model.CredentialPurpose;
import io.unitycatalog.server.model.GcpServiceAccountKeyRequest;
import io.unitycatalog.server.model.S3AssumeRole;
import io.unitycatalog.server.model.S3CompatibleRequest;
import io.unitycatalog.server.model.S3CompatibleResponse;
import io.unitycatalog.server.persist.secret.EncryptedCredentialSecretStore;
import io.unitycatalog.server.utils.TestUtils;
import java.security.KeyPairGenerator;
import java.util.Base64;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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
    S3CompatibleRequest request =
        new S3CompatibleRequest()
            .accessKeyId("key-id-1")
            .secretAccessKey("write-only-value")
            .s3Endpoint(S3_ENDPOINT);
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
    assertThat(info.getS3Compatible().getAccessKeyId()).isEqualTo("key-id-1");
    // The vend cache keys on the fingerprint of the stored value: the same value has the same
    // one, and the key pair stored again (with a fresh nonce) has another.
    String one = "{\"access_key_id\":\"key-id-1\",\"s3_endpoint\":\"" + S3_ENDPOINT + "\"}";
    String two = "{\"access_key_id\":\"key-id-2\",\"s3_endpoint\":\"" + S3_ENDPOINT + "\"}";
    assertThat(CredentialDAO.builder().credential(one).build().getCredentialFingerprint())
        .isEqualTo(CredentialDAO.builder().credential(one).build().getCredentialFingerprint())
        .isNotEqualTo(CredentialDAO.builder().credential(two).build().getCredentialFingerprint());
    assertThat(s3CompatibleDao(request).getCredentialFingerprint())
        .isNotEqualTo(dao.getCredentialFingerprint());
  }

  @Test
  public void testGcpPrivateKeyIsStoredEncryptedAndNotInResponse() throws Exception {
    String privateKey = TestUtils.generatePkcs8PrivateKeyPem();
    CredentialDAO dao =
        CredentialDAO.from(
            new CreateCredentialRequest()
                .name("gcs")
                .purpose(CredentialPurpose.STORAGE)
                .gcpServiceAccountKey(
                    new GcpServiceAccountKeyRequest()
                        .email("uc-test@my-project.iam.gserviceaccount.com")
                        .privateKeyId("key-id-1")
                        .privateKey(privateKey)),
            "caller",
            SECRET_STORE);

    assertThat(dao.getGcpPrivateKey(SECRET_STORE)).isEqualTo(privateKey);
    // What the database holds is not the key, and another key does not read it.
    assertThat(dao.getCredentialType())
        .isEqualTo(CredentialDAO.CredentialType.GCP_SERVICE_ACCOUNT_KEY);
    assertThat(storedJson(dao)).doesNotContain("BEGIN").contains("\"private_key\":\"v1:");
    EncryptedCredentialSecretStore otherKey =
        new EncryptedCredentialSecretStore(
            Optional.of(Base64.getEncoder().encodeToString(new byte[32])));
    assertThatThrownBy(() -> dao.getGcpPrivateKey(otherKey)).isInstanceOf(BaseException.class);
    // The response has the email and key id only.
    String json = new ObjectMapper().writeValueAsString(dao.toCredentialInfo(Optional.empty()));
    assertThat(json).doesNotContain("private_key\":").doesNotContain("BEGIN");
    assertThat(dao.toCredentialInfo(Optional.empty()).getGcpServiceAccountKey().getPrivateKeyId())
        .isEqualTo("key-id-1");
  }

  @Test
  public void testGcpKeyNeedsTheServerKey() {
    assertThatThrownBy(
            () ->
                CredentialDAO.from(
                    new CreateCredentialRequest()
                        .name("gcs")
                        .purpose(CredentialPurpose.STORAGE)
                        .gcpServiceAccountKey(
                            new GcpServiceAccountKeyRequest()
                                .email("uc-test@my-project.iam.gserviceaccount.com")
                                .privateKeyId("key-id-1")
                                .privateKey(TestUtils.generatePkcs8PrivateKeyPem())),
                    "caller",
                    new EncryptedCredentialSecretStore(Optional.empty())))
        .isInstanceOf(BaseException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.FAILED_PRECONDITION);
  }

  /** Keys that are not usable, built from parts so that no PEM header is a literal. */
  static Stream<String> badGcpKeys() {
    String begin = "-----BEGIN " + "PRIVATE" + " KEY-----\n";
    String end = "-----END " + "PRIVATE" + " KEY-----\n";
    return Stream.of(
        "", "   ", "not-a-key", begin + end, begin + "AAAA\n" + end, "{\"private_key\": \"x\"}");
  }

  @ParameterizedTest
  @MethodSource("badGcpKeys")
  public void testBadGcpKeysAreRefusedWithoutEchoingThem(String privateKey) {
    assertThatThrownBy(() -> gcsDao(privateKey))
        .isInstanceOf(BaseException.class)
        .satisfies(
            e -> {
              assertThat(((BaseException) e).getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
              // The rejected value is not part of the message.
              if (!privateKey.isBlank()) {
                assertThat(e.getMessage()).doesNotContain(privateKey.trim());
              }
            });
  }

  @Test
  public void testGcpKeysThatAreNotRsaPkcs8AreRefused() throws Exception {
    // Signing in with a service account key needs an RSA key in PKCS#8.
    KeyPairGenerator ec = KeyPairGenerator.getInstance("EC");
    ec.initialize(256);
    String ecPem = pem("PRIVATE" + " KEY", ec.generateKeyPair().getPrivate().getEncoded());
    assertThatThrownBy(() -> gcsDao(ecPem)).satisfies(DAO_REFUSAL);
    // A PEM labeled as PKCS#1 ("RSA PRIVATE KEY") is refused by its label: Google issues PKCS#8.
    KeyPairGenerator rsa = KeyPairGenerator.getInstance("RSA");
    rsa.initialize(2048);
    String pkcs1Pem = pem("RSA PRIVATE" + " KEY", rsa.generateKeyPair().getPrivate().getEncoded());
    assertThatThrownBy(() -> gcsDao(pkcs1Pem)).satisfies(DAO_REFUSAL);
  }

  private static final Consumer<Throwable> DAO_REFUSAL =
      e ->
          assertThat(e)
              .isInstanceOfSatisfying(
                  BaseException.class,
                  b -> assertThat(b.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT));

  private static String pem(String label, byte[] der) {
    return "-----BEGIN "
        + label
        + "-----\n"
        + Base64.getMimeEncoder(64, "\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
            .encodeToString(der)
        + "\n-----END "
        + label
        + "-----\n";
  }

  private static CredentialDAO gcsDao(String privateKey) {
    return CredentialDAO.from(
        new CreateCredentialRequest()
            .name("gcs")
            .purpose(CredentialPurpose.STORAGE)
            .gcpServiceAccountKey(
                new GcpServiceAccountKeyRequest()
                    .email("uc-test@my-project.iam.gserviceaccount.com")
                    .privateKeyId("key-id-1")
                    .privateKey(privateKey)),
        "caller",
        SECRET_STORE);
  }

  private static String storedJson(CredentialDAO dao) throws Exception {
    java.lang.reflect.Field field = CredentialDAO.class.getDeclaredField("credential");
    field.setAccessible(true);
    return (String) field.get(dao);
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
    // The server's own data, so an internal error rather than a caller's invalid argument.
    assertThatThrownBy(dao::getS3CompatibleResponse)
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
    assertThatThrownBy(dao::getS3CompatibleResponse)
        .isInstanceOf(BaseException.class)
        .hasMessage("Stored credential 'compat' of type S3_COMPATIBLE cannot be read.")
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INTERNAL);
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
