package io.unitycatalog.server.service.credential.gcp;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class GcpCredentialVendorTest {

  private final GcpCredentialVendor credentialVendor =
      new GcpCredentialVendor(new ServerProperties(new Properties()));

  private String availabilityConditionFor(String location) {
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
    return credentials
        .getCredentialAccessBoundary()
        .getAccessBoundaryRules()
        .get(0)
        .getAvailabilityCondition()
        .getExpression();
  }

  /** The condition the vendor is expected to emit for a path already escaped for CEL. */
  private String expectedCondition(String bucket, String escapedPath) {
    return format(
        "resource.name.startsWith('projects/_/buckets/%s/objects/%s')"
            + " || api.getAttribute('storage.googleapis.com/objectListPrefix', '')"
            + ".startsWith('%s')",
        bucket, escapedPath, escapedPath);
  }

  @Test
  public void testOrdinaryPath() {
    assertThat(availabilityConditionFor("gs://test-bucket/team/data"))
        .isEqualTo(expectedCondition("test-bucket", "team/data"));
  }

  @Test
  public void testQuoteInPathCannotInjectCelOperators() {
    // gs://test-bucket/team/data') || ('1'=='1
    String location = "gs://test-bucket/team/data')%20%7C%7C%20('1'%3D%3D'1";

    assertThat(availabilityConditionFor(location))
        .isEqualTo(expectedCondition("test-bucket", "team/data\\') || (\\'1\\'==\\'1"));
  }

  @Test
  public void testBackslashInPathCannotConsumeTheQuoteEscape() {
    // gs://test-bucket/team/data\') || ('1'=='1
    String location = "gs://test-bucket/team/data%5C')%20%7C%7C%20('1'%3D%3D'1";

    assertThat(availabilityConditionFor(location))
        .isEqualTo(expectedCondition("test-bucket", "team/data\\\\\\') || (\\'1\\'==\\'1"));
  }

  @Test
  public void testLineBreaksInPathAreEscaped() {
    assertThat(availabilityConditionFor("gs://test-bucket/team/two%0D%0Alines"))
        .isEqualTo(expectedCondition("test-bucket", "team/two\\r\\nlines"));
  }
}
