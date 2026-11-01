package io.unitycatalog.server.service.credential.gcp;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.ServerProperties;
import java.time.Instant;
import java.util.Date;
import java.util.Properties;

/** Returns the real access boundary as a test token, without exchanging cloud credentials. */
public class BoundaryTestingCredentialGenerator implements GcpCredentialGenerator {
  @Override
  public AccessToken generate(CredentialContext context) {
    GcpCredentialVendor vendor = new GcpCredentialVendor(new ServerProperties(new Properties()));
    DownscopedCredentials credentials =
        (DownscopedCredentials)
            vendor.downscopeGcpCreds(
                GoogleCredentials.create(new AccessToken("test-source-token", null)), context);
    String condition =
        credentials
            .getCredentialAccessBoundary()
            .getAccessBoundaryRules()
            .get(0)
            .getAvailabilityCondition()
            .getExpression();
    return new AccessToken(condition, Date.from(Instant.now().plusSeconds(3600)));
  }
}
