package io.unitycatalog.server.service.credential.gcp;

import static io.unitycatalog.server.utils.Constants.MANAGED_STORAGE_CATALOG_PREFIX;
import static io.unitycatalog.server.utils.Constants.MANAGED_STORAGE_SCHEMA_PREFIX;
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
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageLocationValidator;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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

  private boolean canVend(NormalizedURL location) {
    try {
      ruleFor(location.toString());
      return true;
    } catch (BaseException e) {
      assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
      return false;
    }
  }

  private boolean rootAccepted(NormalizedURL root, String prefix) {
    StorageLocationValidator validator =
        new StorageLocationValidator(
            new CloudCredentialVendor(null, null, credentialVendor),
            (session, location) -> Optional.empty());
    try {
      if (prefix.equals(MANAGED_STORAGE_CATALOG_PREFIX)) {
        validator.validateCatalogStorageRoot(null, root);
      } else {
        validator.validateSchemaStorageRoot(null, root);
      }
      return true;
    } catch (BaseException e) {
      assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
      assertThat(e.getMessage()).contains("its GCS access boundary condition is");
      return false;
    }
  }

  @ParameterizedTest
  @CsvSource({
    MANAGED_STORAGE_CATALOG_PREFIX + ", test-bucket, r",
    MANAGED_STORAGE_SCHEMA_PREFIX + ", test-bucket, r",
    MANAGED_STORAGE_CATALOG_PREFIX + ", test-bucket0, r",
    MANAGED_STORAGE_SCHEMA_PREFIX + ", test-bucket0, r",
    MANAGED_STORAGE_CATALOG_PREFIX + ", test-bucket, %22",
    MANAGED_STORAGE_SCHEMA_PREFIX + ", test-bucket, %5C"
  })
  public void testGcsStorageRootIsAcceptedExactlyWhenManagedCredentialsCanBeVended(
      String prefix, String bucket, String pathChar) {
    UUID id = UUID.randomUUID();
    List<Integer> acceptedLengths =
        IntStream.rangeClosed(1, 500)
            .filter(
                length -> {
                  NormalizedURL root =
                      NormalizedURL.from("gs://" + bucket + "/" + pathChar.repeat(length));
                  NormalizedURL container =
                      prefix.equals(MANAGED_STORAGE_CATALOG_PREFIX)
                          ? ExternalLocationUtils.getManagedLocationForCatalog(root, id)
                          : ExternalLocationUtils.getManagedLocationForSchema(root, id);
                  NormalizedURL model =
                      ExternalLocationUtils.getManagedLocationForModel(container, id);
                  boolean vendable =
                      Stream.of(
                              ExternalLocationUtils.getManagedLocationForTable(container, id),
                              ExternalLocationUtils.getManagedLocationForVolume(container, id),
                              model,
                              ExternalLocationUtils.getManagedLocationForModelVersion(model, id))
                          .allMatch(this::canVend);
                  boolean accepted = rootAccepted(root, prefix);
                  assertThat(accepted)
                      .as("root path of %d x %s", length, pathChar)
                      .isEqualTo(vendable);
                  return accepted;
                })
            .boxed()
            .toList();

    assertThat(acceptedLengths).isNotEmpty();
    int longestAccepted = acceptedLengths.get(acceptedLengths.size() - 1);
    assertThat(longestAccepted).isLessThan(500);
    assertThat(acceptedLengths).hasSize(longestAccepted);
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
}
