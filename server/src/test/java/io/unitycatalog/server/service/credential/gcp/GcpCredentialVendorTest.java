package io.unitycatalog.server.service.credential.gcp;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.CredentialAccessBoundary.AccessBoundaryRule;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
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
        "resource.name.startsWith(\"projects/_/buckets/%s/objects/%s\")"
            + " || api.getAttribute('storage.googleapis.com/objectListPrefix', '')"
            + ".startsWith(\"%s\")",
        bucket, escapedPath, escapedPath);
  }

  @Test
  public void testOrdinaryPath() {
    AccessBoundaryRule rule = ruleFor("gs://test-bucket/team/data");

    assertThat(rule.getAvailableResource())
        .isEqualTo("//storage.googleapis.com/projects/_/buckets/test-bucket");
    assertThat(rule.getAvailablePermissions()).containsExactly("inRole:roles/storage.objectViewer");
    assertThat(rule.getAvailabilityCondition().getExpression())
        .isEqualTo(expectedCondition("test-bucket", "team/data"));
  }

  @Test
  public void testQuoteInPathCannotInjectCelOperators() {
    // gs://test-bucket/team/data') || ('1'=='1
    String location = "gs://test-bucket/team/data')%20%7C%7C%20('1'%3D%3D'1";

    assertThat(availabilityConditionFor(location))
        .isEqualTo(expectedCondition("test-bucket", "team/data')%20%7C%7C%20('1'%3D%3D'1"));
  }

  @Test
  public void testDoubleQuoteInPathCannotInjectCelOperators() {
    // gs://test-bucket/team/data") || ("1"=="1
    String location = "gs://test-bucket/team/data%22)%20%7C%7C%20(%221%22%3D%3D%221";

    assertThat(availabilityConditionFor(location))
        .isEqualTo(expectedCondition("test-bucket", "team/data%22)%20%7C%7C%20(%221%22%3D%3D%221"));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4})
  public void testBackslashesCannotConsumeTheDoubleQuoteEscape(int backslashCount) {
    String location =
        "gs://test-bucket/team/data"
            + "%5C".repeat(backslashCount)
            + "%22)%20%7C%7C%20(%221%22%3D%3D%221";
    String escapedPath =
        "team/data" + "%5C".repeat(backslashCount) + "%22)%20%7C%7C%20(%221%22%3D%3D%221";

    assertThat(availabilityConditionFor(location))
        .isEqualTo(expectedCondition("test-bucket", escapedPath));
  }

  @Test
  public void testCelEscapeSequencesInPathStayLiteral() {
    String escapedPath = "team/%5Cn%5Cx27%5Cu0027";

    assertThat(availabilityConditionFor("gs://test-bucket/team/%5Cn%5Cx27%5Cu0027"))
        .isEqualTo(expectedCondition("test-bucket", escapedPath));
  }

  /** A raw quote, backslash or control character in a stored path is escaped for CEL. */
  @ParameterizedTest
  @MethodSource("rawCelSpecialCharacters")
  public void testRawCelSpecialCharactersAreEscaped(String path, String escapedPath) {
    assertThat(availabilityConditionFor("gs://test-bucket/" + path))
        .isEqualTo(expectedCondition("test-bucket", escapedPath));
  }

  private static Stream<Arguments> rawCelSpecialCharacters() {
    return Stream.of(
        Arguments.of("team/data\") || (\"1\"==\"1", "team/data\\\") || (\\\"1\\\"==\\\"1"),
        Arguments.of("team/data\\\") || (\"1", "team/data\\\\\\\") || (\\\"1"),
        Arguments.of("team/\u0007\b\f\n\r\t\u000Bdata", "team/\\a\\b\\f\\n\\r\\t\\vdata"));
  }

  @Test
  public void testEncodedControlCharactersStayLiteral() {
    String location = "gs://test-bucket/team/%07%08%0C%0A%0D%09%0Bdata";

    assertThat(availabilityConditionFor(location))
        .isEqualTo(expectedCondition("test-bucket", "team/%07%08%0C%0A%0D%09%0Bdata"));
  }

  @Test
  public void testEncodedUnicodePathStaysLiteral() {
    assertThat(
            availabilityConditionFor("gs://test-bucket/caf%C3%A9/%E8%B3%87%E6%96%99/%F0%9F%92%BE"))
        .isEqualTo(expectedCondition("test-bucket", "caf%C3%A9/%E8%B3%87%E6%96%99/%F0%9F%92%BE"));
  }

  @Test
  public void testUnicodePathIsPreserved() {
    assertThat(availabilityConditionFor("gs://test-bucket/café/資料/💾"))
        .isEqualTo(expectedCondition("test-bucket", "café/資料/💾"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"team%2Freports", "team%2F", "team%252Freports", "team+reports"})
  public void testBoundaryPreservesLiteralCloudPath(String path) {
    String location = "gs://test-bucket/" + path;
    String clientPath = new Path(location).toUri().getPath().substring(1);

    assertThat(availabilityConditionFor(location))
        .isEqualTo(expectedCondition("test-bucket", clientPath));
  }
}
