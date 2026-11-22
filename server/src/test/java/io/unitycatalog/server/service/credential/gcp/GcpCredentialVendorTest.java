package io.unitycatalog.server.service.credential.gcp;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.CredentialAccessBoundary.AccessBoundaryRule;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialPurpose;
import io.unitycatalog.server.model.GcpServiceAccountKeyRequest;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.secret.EncryptedCredentialSecretStore;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.util.Date;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class GcpCredentialVendorTest {

  private final GcpCredentialVendor credentialVendor =
      new GcpCredentialVendor(new ServerProperties(new Properties()));

  private AccessBoundaryRule ruleFor(String location) {
    CredentialContext context =
        CredentialContext.create(
            NormalizedURL.from(location),
            Set.of(CredentialContext.Privilege.SELECT),
            Optional.empty());
    DownscopedCredentials credentials =
        (DownscopedCredentials)
            credentialVendor.downscopeGcpCreds(
                GoogleCredentials.create(new AccessToken("test-token", null)), context);

    assertThat(credentials.getCredentialAccessBoundary().getAccessBoundaryRules()).hasSize(1);
    return credentials.getCredentialAccessBoundary().getAccessBoundaryRules().get(0);
  }

  private String availabilityConditionFor(String location) {
    return ruleFor(location).getAvailabilityCondition().getExpression();
  }

  /** The condition the vendor is expected to emit for a path already escaped for CEL. */
  private String expectedCondition(String bucket, String escapedPath) {
    return format(
        "resource.name == \"projects/_/buckets/%s/objects/%s\""
            + " || resource.name.startsWith(\"projects/_/buckets/%s/objects/%s/\")"
            + " || api.getAttribute('storage.googleapis.com/objectListPrefix', '') == \"%s\""
            + " || api.getAttribute('storage.googleapis.com/objectListPrefix', '')"
            + ".startsWith(\"%s/\")",
        bucket, escapedPath, bucket, escapedPath, escapedPath, escapedPath);
  }

  @Test
  public void testOrdinaryPath() {
    AccessBoundaryRule rule = ruleFor("gs://test-bucket/team/data");

    assertThat(rule.getAvailableResource())
        .isEqualTo("//storage.googleapis.com/projects/_/buckets/test-bucket");
    assertThat(rule.getAvailablePermissions()).containsExactly("inRole:roles/storage.objectViewer");
    assertThat(rule.getAvailabilityCondition().getExpression())
        .isEqualTo(
            "resource.name == \"projects/_/buckets/test-bucket/objects/team/data\" ||"
                + " resource.name.startsWith(\"projects/_/buckets/test-bucket/objects/team/data/\")"
                + " || api.getAttribute('storage.googleapis.com/objectListPrefix', '') =="
                + " \"team/data\" || api.getAttribute('storage.googleapis.com/objectListPrefix',"
                + " '').startsWith(\"team/data/\")");
  }

  @Test
  public void testBucketNameWithAnUnderscore() {
    AccessBoundaryRule rule = ruleFor("gs://my_bucket/data");

    assertThat(rule.getAvailableResource())
        .isEqualTo("//storage.googleapis.com/projects/_/buckets/my_bucket");
    assertThat(rule.getAvailabilityCondition().getExpression())
        .contains("projects/_/buckets/my_bucket/objects/data/")
        .doesNotContain("null");
  }

  @Test
  public void testBucketRootCoversWholeBucket() {
    assertThat(availabilityConditionFor("gs://test-bucket"))
        .isEqualTo(
            "resource.name.startsWith(\"projects/_/buckets/test-bucket/objects/\")"
                + " || api.getAttribute('storage.googleapis.com/objectListPrefix', '')"
                + ".startsWith(\"\")");
  }

  @Test
  public void testQuoteInPathCannotInjectCelOperators() {
    // gs://test-bucket/team/data') || ('1'=='1
    String location = "gs://test-bucket/team/data')%20%7C%7C%20('1'%3D%3D'1";

    assertThat(availabilityConditionFor(location))
        .isEqualTo(expectedCondition("test-bucket", "team/data') || ('1'=='1"));
  }

  @Test
  public void testDoubleQuoteInPathCannotInjectCelOperators() {
    // gs://test-bucket/team/data") || ("1"=="1
    String location = "gs://test-bucket/team/data%22)%20%7C%7C%20(%221%22%3D%3D%221";

    assertThat(availabilityConditionFor(location))
        .isEqualTo(expectedCondition("test-bucket", "team/data\\\") || (\\\"1\\\"==\\\"1"));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4})
  public void testBackslashesCannotConsumeTheDoubleQuoteEscape(int backslashCount) {
    String location =
        "gs://test-bucket/team/data"
            + "%5C".repeat(backslashCount)
            + "%22)%20%7C%7C%20(%221%22%3D%3D%221";
    String escapedPath =
        "team/data" + "\\".repeat(backslashCount * 2 + 1) + "\") || (\\\"1\\\"==\\\"1";

    assertThat(availabilityConditionFor(location))
        .isEqualTo(expectedCondition("test-bucket", escapedPath));
  }

  @Test
  public void testCelEscapeSequencesInPathStayLiteral() {
    String escapedPath = "team/\\\\n\\\\x27\\\\" + "u0027";

    assertThat(availabilityConditionFor("gs://test-bucket/team/%5Cn%5Cx27%5Cu0027"))
        .isEqualTo(expectedCondition("test-bucket", escapedPath));
  }

  @Test
  public void testCelControlCharactersAreEscaped() {
    String location = "gs://test-bucket/team/%07%08%0C%0A%0D%09%0Bdata";

    assertThat(availabilityConditionFor(location))
        .isEqualTo(expectedCondition("test-bucket", "team/\\a\\b\\f\\n\\r\\t\\vdata"));
  }

  @Test
  public void testUnicodePathIsPreserved() {
    assertThat(
            availabilityConditionFor("gs://test-bucket/caf%C3%A9/%E8%B3%87%E6%96%99/%F0%9F%92%BE"))
        .isEqualTo(expectedCondition("test-bucket", "café/資料/💾"));
  }

  @Test
  public void testConditionAtTheLengthLimit() {
    // With a 12-character bucket, a 440-character path makes a condition of exactly 2048.
    assertThat(availabilityConditionFor("gs://test-bucket0/" + "a".repeat(440))).hasSize(2048);

    assertThatThrownBy(() -> ruleFor("gs://test-bucket0/" + "a".repeat(441)))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("2052 characters, and the limit is 2048")
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INVALID_ARGUMENT);
  }

  @ParameterizedTest
  @CsvSource({"invalid_request, INVALID_ARGUMENT", "invalid_grant, FAILED_PRECONDITION"})
  public void testTokenServiceErrors(String oauthError, ErrorCode expected) {
    MockHttpTransport tokenService =
        new MockHttpTransport.Builder()
            .setLowLevelHttpResponse(
                new MockLowLevelHttpResponse()
                    .setStatusCode(400)
                    .setContentType("application/json")
                    .setContent(
                        format(
                            "{\"error\": \"%s\", \"error_description\": \"rejected\"}",
                            oauthError)))
            .build();
    DownscopedCredentials credentials =
        DownscopedCredentials.newBuilder()
            .setSourceCredential(
                GoogleCredentials.create(new AccessToken("test-token", new Date(Long.MAX_VALUE))))
            .setCredentialAccessBoundary(
                CredentialAccessBoundary.newBuilder()
                    .addRule(ruleFor("gs://test-bucket/t"))
                    .build())
            .setHttpTransportFactory(() -> tokenService)
            .build();

    IOException failure = catchThrowableOfType(IOException.class, credentials::refreshAccessToken);
    assertThat(GcpCredentialVendor.credentialError(failure).getErrorCode()).isEqualTo(expected);
  }

  /**
   * The failures Google's token endpoint gives for a stored key, produced by the real library
   * against a stand-in for the endpoint, so the mapping matches what the library throws.
   */
  static Stream<Arguments> storedKeyFailures() {
    String signature = "{\"error\": \"invalid_grant\", \"error_description\": \"Invalid JWT\"}";
    String client = "{\"error\": \"invalid_client\", \"error_description\": \"deleted\"}";
    String unauthorized =
        "{\"error\": \"unauthorized_client\", \"error_description\": \"not allowed\"}";
    String clockSkew =
        "{\"error\": \"invalid_grant\", \"error_description\": \"Invalid JWT: Token must be a"
            + " short-lived token (60 minutes) and in a reasonable timeframe.\"}";
    return Stream.of(
        Arguments.of(400, signature, ErrorCode.FAILED_PRECONDITION),
        Arguments.of(400, client, ErrorCode.FAILED_PRECONDITION),
        Arguments.of(400, unauthorized, ErrorCode.FAILED_PRECONDITION),
        Arguments.of(400, clockSkew, ErrorCode.INTERNAL),
        Arguments.of(503, "{\"error\": \"backend_error\"}", ErrorCode.UNAVAILABLE),
        Arguments.of(500, "upstream failure", ErrorCode.UNAVAILABLE));
  }

  @ParameterizedTest
  @MethodSource("storedKeyFailures")
  public void testStoredKeyFailures(int status, String body, ErrorCode expected) throws Exception {
    MockHttpTransport tokenService =
        new MockHttpTransport.Builder()
            .setLowLevelHttpResponse(
                new MockLowLevelHttpResponse()
                    .setStatusCode(status)
                    .setContentType("application/json")
                    .setContent(body))
            .build();
    IOException failure = refreshStoredKeyAgainst(tokenService);

    BaseException error = GcpCredentialVendor.storedKeyError(failure);
    assertThat(error.getErrorCode()).isEqualTo(expected);
    // Whoever asks for access may not read the credential, so the message names none of it.
    assertThat(error.getMessage()).doesNotContain(STORED_KEY_EMAIL).doesNotContain("key-id-1");
    if (expected == ErrorCode.FAILED_PRECONDITION) {
      assertThat(error.getMessage()).contains("Google rejected the service account key");
    } else if (expected == ErrorCode.INTERNAL) {
      // A clock problem is not about the key.
      assertThat(error.getMessage()).doesNotContain("service account key");
    }
  }

  /** The generator of the {@code gcs.*} settings, counting how often it is asked for a token. */
  public static class ConfiguredGenerator implements GcpCredentialGenerator {
    static final AtomicInteger CALLS = new AtomicInteger();

    @Override
    public AccessToken generate(CredentialContext context) {
      CALLS.incrementAndGet();
      return new AccessToken("from-gcs-settings", new Date(System.currentTimeMillis() + 600_000));
    }
  }

  private static final EncryptedCredentialSecretStore SECRET_STORE =
      new EncryptedCredentialSecretStore(Optional.of(TestUtils.CREDENTIAL_ENCRYPTION_KEY));

  private static CredentialContext contextWithStoredKey() {
    CredentialDAO stored =
        CredentialDAO.from(
            new CreateCredentialRequest()
                .name("gcs_credential")
                .purpose(CredentialPurpose.STORAGE)
                .gcpServiceAccountKey(
                    new GcpServiceAccountKeyRequest()
                        .email(STORED_KEY_EMAIL)
                        .privateKeyId("key-id-1")
                        .privateKey(TestUtils.generatePkcs8PrivateKeyPem())),
            "caller",
            SECRET_STORE);
    return CredentialContext.create(
        NormalizedURL.from("gs://test-bucket/t"),
        Set.of(CredentialContext.Privilege.SELECT),
        Optional.of(stored));
  }

  private static GcpCredentialVendor vendorWithGcsSettingsFor(
      java.util.function.Function<GcpServiceAccountKeyRequest, GcpCredentialGenerator> keys) {
    Properties properties = new Properties();
    properties.setProperty("gcs.bucketPath.0", "gs://test-bucket");
    properties.setProperty("gcs.credentialGenerator.0", ConfiguredGenerator.class.getName());
    return new GcpCredentialVendor(new ServerProperties(properties), SECRET_STORE, keys);
  }

  @Test
  public void testStoredKeyTakesPrecedenceOverGcsSettings() {
    ConfiguredGenerator.CALLS.set(0);
    GcpCredentialVendor vendor =
        vendorWithGcsSettingsFor(
            key -> context -> new AccessToken("from-stored-key:" + key.getEmail(), null));

    AccessToken stored = vendor.vendGcpCredential(contextWithStoredKey());

    assertThat(stored.getTokenValue()).isEqualTo("from-stored-key:" + STORED_KEY_EMAIL);
    assertThat(ConfiguredGenerator.CALLS).hasValue(0);

    // Without a stored credential, the same bucket is vended from the gcs.* settings.
    AccessToken configured =
        vendor.vendGcpCredential(
            CredentialContext.create(
                NormalizedURL.from("gs://test-bucket/t"),
                Set.of(CredentialContext.Privilege.SELECT),
                Optional.empty()));
    assertThat(configured.getTokenValue()).isEqualTo("from-gcs-settings");
    assertThat(ConfiguredGenerator.CALLS).hasValue(1);
  }

  @Test
  public void testStoredKeyFailureDoesNotFallBackToGcsSettings() {
    ConfiguredGenerator.CALLS.set(0);
    GcpCredentialVendor vendor =
        vendorWithGcsSettingsFor(
            key ->
                context -> {
                  throw new BaseException(ErrorCode.FAILED_PRECONDITION, "key rejected");
                });

    BaseException error =
        catchThrowableOfType(
            BaseException.class, () -> vendor.vendGcpCredential(contextWithStoredKey()));

    assertThat(error.getErrorCode()).isEqualTo(ErrorCode.FAILED_PRECONDITION);
    assertThat(ConfiguredGenerator.CALLS).hasValue(0);
  }

  @Test
  public void testInvalidRequestFromTheTokenServiceIsABadLocation() {
    BaseException error =
        GcpCredentialVendor.storedKeyError(
            new IOException("Error code invalid_request: the access boundary is too large"));

    assertThat(error.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
  }

  /** A stored key that cannot be decrypted names nothing of the credential to whoever asks. */
  @Test
  public void testUndecryptableStoredKeyIsNotNamedInTheError() {
    CredentialContext context = contextWithStoredKey();
    EncryptedCredentialSecretStore otherKey =
        new EncryptedCredentialSecretStore(
            Optional.of(java.util.Base64.getEncoder().encodeToString(new byte[32])));
    GcpCredentialVendor vendor =
        new GcpCredentialVendor(
            new ServerProperties(new Properties()),
            otherKey,
            key ->
                c -> {
                  throw new IllegalStateException("must not sign in with an unreadable key");
                });

    BaseException error =
        catchThrowableOfType(BaseException.class, () -> vendor.vendGcpCredential(context));

    assertThat(error.getMessage())
        .contains("Contact the server administrator")
        .doesNotContain("gcs_credential");
  }

  @Test
  public void testStoredKeyWhenGoogleCannotBeReached() throws Exception {
    MockHttpTransport unreachable =
        new MockHttpTransport() {
          @Override
          public com.google.api.client.http.LowLevelHttpRequest buildRequest(
              String method, String url) throws IOException {
            throw new java.net.UnknownHostException("oauth2.googleapis.com");
          }
        };
    assertThat(
            GcpCredentialVendor.storedKeyError(refreshStoredKeyAgainst(unreachable)).getErrorCode())
        .isEqualTo(ErrorCode.UNAVAILABLE);
  }

  private static final String STORED_KEY_EMAIL = "uc-test@my-project.iam.gserviceaccount.com";

  /** Refreshes a service account key against {@code tokenService}, returning how that fails. */
  private IOException refreshStoredKeyAgainst(MockHttpTransport tokenService) throws Exception {
    java.security.KeyPairGenerator generator = java.security.KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    com.google.auth.oauth2.ServiceAccountCredentials credentials =
        com.google.auth.oauth2.ServiceAccountCredentials.newBuilder()
            .setClientEmail(STORED_KEY_EMAIL)
            .setPrivateKey(generator.generateKeyPair().getPrivate())
            .setPrivateKeyId("key-id-1")
            .setScopes(GcpCredentialVendor.INITIAL_SCOPES)
            .setHttpTransportFactory(() -> tokenService)
            .setDefaultRetriesEnabled(false)
            .build();
    // Through the downscoping step, as the vendor does: it wraps what the key's refresh throws.
    DownscopedCredentials downscoped =
        DownscopedCredentials.newBuilder()
            .setSourceCredential(credentials)
            .setCredentialAccessBoundary(
                CredentialAccessBoundary.newBuilder()
                    .addRule(ruleFor("gs://test-bucket/t"))
                    .build())
            .build();
    return catchThrowableOfType(IOException.class, downscoped::refreshAccessToken);
  }
}
