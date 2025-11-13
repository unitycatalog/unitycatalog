package io.unitycatalog.server.service.credential.gcp;

import static java.lang.String.format;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.base.CharMatcher;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.ServerProperties;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.sql.Date;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.SneakyThrows;
import org.apache.iceberg.Files;

/**
 * Vends GCS OAuth tokens for Unity Catalog by downscoping credentials against the configured bucket
 * allow list.
 *
 * <p>For local integration tests we support two synthetic credential sources:
 *
 * <ul>
 *   <li>{@code testing://<bucket>} returns a non-expiring token to mirror the fixed-credential
 *       experience.
 *   <li>{@code testing-renew://<clock-name>/<bucket>} issues short-lived, time-bucketed tokens
 *       whose expiration aligns with the shared manual clock. This mirrors the AWS renewal tests,
 *       but uses clock-driven tokens instead of a credentials generator.
 * </ul>
 */
public class GcpCredentialVendor {

  public static final List<String> INITIAL_SCOPES =
      List.of("https://www.googleapis.com/auth/cloud-platform");
  private static final String TEST_STATIC_PREFIX = "testing://";
  private static final String TEST_RENEW_PREFIX = "testing-renew://";
  private static final long TEST_RENEW_INTERVAL_MILLIS = 30_000L;

  private final Map<String, String> gcsConfigurations;

  public GcpCredentialVendor(ServerProperties serverProperties) {
    this.gcsConfigurations = serverProperties.getGcsConfigurations();
  }

  @SneakyThrows
  public AccessToken vendGcpToken(CredentialContext credentialContext) {
    String serviceAccountKeyJsonFilePath =
        gcsConfigurations.get(credentialContext.getStorageBase());

    GoogleCredentials creds;
    if (serviceAccountKeyJsonFilePath != null && !serviceAccountKeyJsonFilePath.isEmpty()) {
      if (serviceAccountKeyJsonFilePath.startsWith(TEST_RENEW_PREFIX)) {
        return createTestingRenewalToken(serviceAccountKeyJsonFilePath, credentialContext);
      }
      if (serviceAccountKeyJsonFilePath.startsWith(TEST_STATIC_PREFIX)) {
        // allow pass-through of a dummy value for integration testing
        return AccessToken.newBuilder()
            .setTokenValue(serviceAccountKeyJsonFilePath)
            .setExpirationTime(Date.from(Instant.ofEpochMilli(253370790000000L)))
            .build();
      }
      creds =
          ServiceAccountCredentials.fromStream(
              Files.localInput(serviceAccountKeyJsonFilePath).newStream());
    } else {
      try {
        creds = GoogleCredentials.getApplicationDefault();
      } catch (IOException e) {
        throw new BaseException(ErrorCode.FAILED_PRECONDITION, "GCS credentials not found.", e);
      }
    }

    return downscopeGcpCreds(creds.createScoped(INITIAL_SCOPES), credentialContext)
        .refreshAccessToken();
  }

  private AccessToken createTestingRenewalToken(
      String configValue, CredentialContext credentialContext) {
    String payload = configValue.substring(TEST_RENEW_PREFIX.length());
    String clockName = null;
    String bucketHint = payload;
    int slashIdx = payload.indexOf('/');
    if (slashIdx >= 0) {
      clockName = payload.substring(0, slashIdx);
      bucketHint = payload.substring(slashIdx + 1);
    }

    long currentMillis = currentTestMillis(clockName);
    long windowStart = alignToInterval(currentMillis);
    Instant expiration = Instant.ofEpochMilli(windowStart + TEST_RENEW_INTERVAL_MILLIS);
    String bucketComponent =
        bucketHint == null || bucketHint.isEmpty()
            ? credentialContext.getStorageBase()
            : bucketHint;
    String tokenValue = format("%s%s#%d", TEST_RENEW_PREFIX, bucketComponent, windowStart);
    return AccessToken.newBuilder()
        .setTokenValue(tokenValue)
        .setExpirationTime(Date.from(expiration))
        .build();
  }

  private long currentTestMillis(String clockName) {
    if (clockName != null && !clockName.isEmpty()) {
      try {
        // Use reflection to avoid a compile-time dependency on the Spark connector clock helper
        // while still letting integration tests drive a shared manual clock.
        Class<?> clockClass = Class.forName("io.unitycatalog.spark.utils.Clock");
        Method getManualClock = clockClass.getMethod("getManualClock", String.class);
        Object manualClock = getManualClock.invoke(null, clockName);
        if (manualClock != null) {
          Method nowMethod = manualClock.getClass().getMethod("now");
          Object instant = nowMethod.invoke(manualClock);
          if (instant instanceof Instant) {
            return ((Instant) instant).toEpochMilli();
          }
        }
      } catch (ReflectiveOperationException ignored) {
        // Fall through to the system clock when the Spark Clock is not available.
      }
    }
    return System.currentTimeMillis();
  }

  private long alignToInterval(long millis) {
    return Math.floorDiv(millis, TEST_RENEW_INTERVAL_MILLIS) * TEST_RENEW_INTERVAL_MILLIS;
  }

  OAuth2Credentials downscopeGcpCreds(GoogleCredentials credentials, CredentialContext context) {
    CredentialAccessBoundary.Builder boundaryBuilder = CredentialAccessBoundary.newBuilder();
    List<String> roles = resolvePrivilegesToRoles(context.getPrivileges());

    context
        .getLocations()
        .forEach(
            location -> {
              URI locationUri = URI.create(location);
              String path = CharMatcher.is('/').trimLeadingFrom(locationUri.getPath());

              String resource =
                  format("//storage.googleapis.com/projects/_/buckets/%s", locationUri.getHost());

              // for reading/writing objects
              String resourceNameStartsWithExpr =
                  format(
                      "resource.name.startsWith('projects/_/buckets/%s/objects/%s')",
                      locationUri.getHost(), path);

              // for listing objects
              String objectListPrefixStartsWithExpr =
                  format(
                      "api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('%s')",
                      path);

              String combinedExpr =
                  resourceNameStartsWithExpr + " || " + objectListPrefixStartsWithExpr;

              boundaryBuilder.addRule(
                  CredentialAccessBoundary.AccessBoundaryRule.newBuilder()
                      .setAvailablePermissions(roles)
                      .setAvailabilityCondition(
                          CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition
                              .newBuilder()
                              .setExpression(combinedExpr)
                              .build())
                      .setAvailableResource(resource)
                      .build());
            });

    return DownscopedCredentials.newBuilder()
        .setSourceCredential(credentials)
        .setCredentialAccessBoundary(boundaryBuilder.build())
        .build();
  }

  List<String> resolvePrivilegesToRoles(Set<CredentialContext.Privilege> privileges) {
    if (privileges.contains(CredentialContext.Privilege.UPDATE)) {
      return List.of("inRole:roles/storage.objectAdmin");
    } else if (privileges.contains(CredentialContext.Privilege.SELECT)) {
      return List.of("inRole:roles/storage.objectViewer");
    }
    return List.of();
  }
}
