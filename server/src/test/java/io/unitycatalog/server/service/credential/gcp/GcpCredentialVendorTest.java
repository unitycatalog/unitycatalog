package io.unitycatalog.server.service.credential.gcp;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.CredentialAccessBoundary;
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
  private final GoogleCredentials sourceCredentials =
      GoogleCredentials.create(new AccessToken("test-token", null));

  private String availabilityConditionFor(String location) {
    CredentialContext context =
        CredentialContext.create(
            NormalizedURL.from(location),
            Set.of(CredentialContext.Privilege.SELECT),
            Optional.empty());
    DownscopedCredentials downscopedCredentials =
        (DownscopedCredentials) credentialVendor.downscopeGcpCreds(sourceCredentials, context);
    CredentialAccessBoundary credentialAccessBoundary =
        downscopedCredentials.getCredentialAccessBoundary();

    assertThat(credentialAccessBoundary.getAccessBoundaryRules()).hasSize(1);
    return credentialAccessBoundary
        .getAccessBoundaryRules()
        .get(0)
        .getAvailabilityCondition()
        .getExpression();
  }

  private String expectedCondition(String bucket, String escapedPath) {
    return format(
        "resource.name.startsWith('projects/_/buckets/%s/objects/%s')"
            + " || api.getAttribute('storage.googleapis.com/objectListPrefix', '')"
            + ".startsWith('%s')",
        bucket, escapedPath, escapedPath);
  }

  @Test
  public void testOrdinaryPathConditionIsUnchanged() {
    assertThat(availabilityConditionFor("gs://victim-bucket/team/x"))
        .isEqualTo(expectedCondition("victim-bucket", "team/x"));
  }

  @Test
  public void testCelExpressionInjectionCharactersAreEscaped() {
    String maliciousLocation = "gs://victim-bucket/team/x')%20%7C%7C%20('1'%3D%3D'1";
    String escapedPath = "team/x\\') || (\\'1\\'==\\'1";

    assertThat(availabilityConditionFor(maliciousLocation))
        .isEqualTo(expectedCondition("victim-bucket", escapedPath));
  }

  @Test
  public void testBackslashCannotConsumeQuoteEscape() {
    String maliciousLocation =
        "gs://victim-bucket/public/inj%5C%27)%7C%7Ctrue%7C%7C" + "resource.name.startsWith(%27";
    String escapedPath = "public/inj" + "\\".repeat(3) + "')||true||resource.name.startsWith(\\'";

    assertThat(availabilityConditionFor(maliciousLocation))
        .isEqualTo(expectedCondition("victim-bucket", escapedPath));
  }

  @Test
  public void testLineBreaksAreEscaped() {
    assertThat(availabilityConditionFor("gs://victim-bucket/team/line%0D%0Abreak"))
        .isEqualTo(expectedCondition("victim-bucket", "team/line\\r\\nbreak"));
  }
}
